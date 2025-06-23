package processors

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/stellar/go/asset"
	"github.com/stellar/go/ingest"
	ttp "github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrOperationNotFound = errors.New("operation not found")

// TokenTransferProcessor processes Stellar transactions and extracts token transfer events.
// It converts Stellar operations (payments, account merges, etc.) into standardized state changes.
type TokenTransferProcessor struct {
	eventsProcessor *ttp.EventsProcessor
}

// NewTokenTransferProcessor creates a new token transfer processor for the specified Stellar network.
func NewTokenTransferProcessor(networkPassphrase string) *TokenTransferProcessor {
	return &TokenTransferProcessor{
		eventsProcessor: ttp.NewEventsProcessor(networkPassphrase),
	}
}

// ProcessTransaction extracts token transfer events from a Stellar transaction and converts them into state changes.
// It handles fee events (transaction costs/refunds) and operation events (payments, mints, burns, etc.).
// Returns a slice of state changes representing debits, credits, mints, and burns.
func (p *TokenTransferProcessor) ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error) {
	ledgerCloseTime := tx.Ledger.LedgerCloseTime()
	ledgerNumber := tx.Ledger.LedgerSequence()
	txHash := tx.Hash.HexString()
	txIdx := tx.Index

	// Extract token transfer events from the transaction using Stellar SDK
	txEvents, err := p.eventsProcessor.EventsFromTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer events from transaction: %w", err)
	}

	// Process both fee events (transaction costs) and operation events (transfers, mints, etc.)
	allEvents := append(txEvents.FeeEvents, txEvents.OperationEvents...)

	stateChanges := make([]types.StateChange, 0, len(allEvents))
	for _, e := range allEvents {
		meta := e.GetMeta()
		contractAddress := meta.GetContractAddress()
		opIdx := meta.GetOperationIndex()
		event := e.GetEvent()

		// For non-fee events, we need operation details to determine the correct state change type
		var opID, opSourceAccount string
		var opType *xdr.OperationType
		if _, isFeeEvent := event.(*ttp.TokenTransferEvent_Fee); !isFeeEvent {
			opID, opType, opSourceAccount, err = p.parseOperationDetails(tx, ledgerNumber, txIdx, opIdx)
			if err != nil {
				if errors.Is(err, ErrOperationNotFound) {
					// Skip events for operations that couldn't be found
					continue
				}
				return nil, fmt.Errorf("parsing operation details: %w", err)
			}
		}

		changes, err := p.processEvent(event, contractAddress, ledgerNumber, ledgerCloseTime, txHash, opID, opType, opSourceAccount)
		if err != nil {
			return nil, err
		}
		stateChanges = append(stateChanges, changes...)
	}

	return stateChanges, nil
}

// parseOperationDetails extracts operation metadata needed for processing token transfer events.
// Returns operation ID, type, and source account which determine how events should be categorized.
func (p *TokenTransferProcessor) parseOperationDetails(tx ingest.LedgerTransaction, ledgerIdx uint32, txIdx uint32, opIdx uint32) (string, *xdr.OperationType, string, error) {
	op, found := tx.GetOperation(opIdx - 1)
	if !found {
		return "", nil, "", ErrOperationNotFound
	}

	operationType := &op.Body.Type
	opSourceAccount := OperationSourceAccount(tx, op)
	opID := utils.OperationID(int32(ledgerIdx), int32(txIdx), int32(opIdx))

	return opID, operationType, opSourceAccount, nil
}

// processEvent routes different types of token transfer events to their specific handlers.
// Each event type (transfer, mint, burn, clawback, fee) requires different business logic.
func (p *TokenTransferProcessor) processEvent(event any, contractAddress string, ledgerNumber uint32, ledgerCloseTime int64, txHash, opID string, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	builder := NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, opID)

	switch event := event.(type) {
	case *ttp.TokenTransferEvent_Transfer:
		return p.handleTransfer(event.Transfer, contractAddress, builder, operationType)
	case *ttp.TokenTransferEvent_Mint:
		return p.handleMint(event.Mint, contractAddress, builder)
	case *ttp.TokenTransferEvent_Burn:
		return p.handleBurn(event.Burn, contractAddress, builder, operationType, opSourceAccount)
	case *ttp.TokenTransferEvent_Clawback:
		return p.handleClawback(event.Clawback, contractAddress, builder, operationType, opSourceAccount)
	case *ttp.TokenTransferEvent_Fee:
		return p.handleFee(event.Fee, contractAddress, builder)
	default:
		return nil, fmt.Errorf("unknown event type: %T", event)
	}
}

// createDebitCreditPair creates a pair of debit and credit state changes for normal transfers.
// Used for regular payments between two accounts (e.g., Alice sends 100 USDC to Bob).
func (p *TokenTransferProcessor) createDebitCreditPair(from, to, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	debitChange := builder.WithCategory(types.StateChangeCategoryDebit).
		WithAccount(from).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		Build()

	creditChange := builder.WithCategory(types.StateChangeCategoryCredit).
		WithAccount(to).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		Build()

	return []types.StateChange{debitChange, creditChange}
}

// createLiquidityPoolChange creates a state change for liquidity pool interactions.
// Includes the liquidity pool ID to track which pool the tokens moved to/from.
func (p *TokenTransferProcessor) createLiquidityPoolChange(category types.StateChangeCategory, accountID, poolID, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		WithLiquidityPool(poolID).
		Build()
}

// createClaimableBalanceChange creates a state change for claimable balance interactions.
// Includes the claimable balance ID to track which balance was created, claimed, or clawed back.
func (p *TokenTransferProcessor) createClaimableBalanceChange(category types.StateChangeCategory, accountID, claimableBalanceID, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		WithClaimableBalance(claimableBalanceID).
		Build()
}

// handleTransfer processes transfer events based on the operation type.
// Different operations require different state change patterns:
// - Claimable balances: single debit/credit with claimable balance ID
// - Liquidity pools: single debit/credit with liquidity pool ID
// - Regular transfers: debit/credit pair between accounts
func (p *TokenTransferProcessor) handleTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType) ([]types.StateChange, error) {
	amount := transfer.GetAmount()
	asset := transfer.GetAsset()

	switch *operationType {
	case xdr.OperationTypeCreateClaimableBalance:
		// When creating a claimable balance, record debit from creator with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeClaimClaimableBalance:
		// When claiming a claimable balance, record credit to claimer with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolDeposit:
		// When depositing to LP, record debit from depositor with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		// When withdrawing from LP, record credit to withdrawer with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeSetTrustLineFlags, xdr.OperationTypeAllowTrust:
		// Skip events generated by these operations since they involve only an LP and Claimable Balance ID
		return nil, nil

	default:
		// Handle regular transfers, path payments, and other operations
		return p.handleDefaultTransfer(transfer, contractAddress, builder)
	}
}

// handleDefaultTransfer handles normal transfers and liquidity pool interactions.
// For LP interactions, creates single state change with LP ID. For regular transfers, creates debit/credit pair.
func (p *TokenTransferProcessor) handleDefaultTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	from := transfer.GetFrom()
	to := transfer.GetTo()
	amount := transfer.GetAmount()
	asset := transfer.GetAsset()

	fromIsLP := IsLiquidityPool(from)
	toIsLP := IsLiquidityPool(to)

	// Handle liquidity pool interactions (from path payments, manual LP operations)
	if fromIsLP || toIsLP {
		if fromIsLP {
			// LP is sending tokens to account (e.g., path payment buying from LP)
			change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, to, from, amount, asset, contractAddress, builder)
			return []types.StateChange{change}, nil
		}
		// LP is receiving tokens from account (e.g., path payment selling to LP)
		change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, from, to, amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil
	}

	// Normal transfer between two accounts (payments, account merge)
	return p.createDebitCreditPair(from, to, amount, asset, contractAddress, builder), nil
}

// handleMint processes mint events that occur when asset issuers create new tokens.
// This happens when an issuer sends their own asset to another account (e.g., USDC payment from USDC issuer).
func (p *TokenTransferProcessor) handleMint(mint *ttp.Mint, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	amount := mint.GetAmount()
	asset := mint.GetAsset()
	var changes []types.StateChange

	// For issued assets, record mint for the issuer account
	if !asset.GetNative() {
		mintChange := builder.WithCategory(types.StateChangeCategoryMint).
			WithAccount(asset.GetIssuedAsset().GetIssuer()).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			Build()
		changes = append(changes, mintChange)
	}

	// Create credit state change for the receiving account. Skip mints to liquidity pools since we dont track LP accounts
	if !IsLiquidityPool(mint.GetTo()) {
		change := builder.WithCategory(types.StateChangeCategoryCredit).
			WithAccount(mint.GetTo()).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			Build()
		changes = append(changes, change)
	}

	return changes, nil
}

// handleBurn processes burn events that occur when tokens are destroyed.
// Burns happen when: 1) tokens are sent to their issuer, 2) issuer claims claimable balance, 3) issuer withdraws from LP.
func (p *TokenTransferProcessor) handleBurn(burn *ttp.Burn, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	amount := burn.GetAmount()
	asset := burn.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClaimClaimableBalance:
		// When issuer claims their own asset from claimable balance, it's burned with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, burn.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		// When issuer withdraws their own asset from LP, it's burned with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryBurn, opSourceAccount, burn.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	default:
		// Regular burn (payment to issuer) creates burn + debit pair
		return p.handleDefaultBurnOrClawback(burn.GetFrom(), amount, asset, contractAddress, builder)
	}
}

// handleClawback processes clawback events where an asset issuer forcibly reclaims tokens.
// Similar to burns but initiated by issuer rather than voluntary transfer to issuer.
func (p *TokenTransferProcessor) handleClawback(clawback *ttp.Clawback, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	amount := clawback.GetAmount()
	asset := clawback.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClawbackClaimableBalance:
		// Clawback of claimable balance creates burn with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, clawback.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	default:
		// Regular clawback creates burn (issuer) + debit (victim) pair
		return p.handleDefaultBurnOrClawback(clawback.GetFrom(), amount, asset, contractAddress, builder)
	}
}

// handleDefaultBurnOrClawback creates state changes for regular burns and clawbacks.
// For non-native assets: creates burn (issuer receives) + debit (sender loses) pair.
// For native XLM: only creates debit since there's no issuer to burn from.
func (p *TokenTransferProcessor) handleDefaultBurnOrClawback(from string, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	var changes []types.StateChange

	// For issued assets, record burn at the issuer account
	if !asset.GetNative() {
		burnChange := builder.WithCategory(types.StateChangeCategoryBurn).
			WithAccount(asset.GetIssuedAsset().GetIssuer()).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			Build()
		changes = append(changes, burnChange)
	}

	// Always record debit from the account losing the tokens. Skip burns from LP accounts since we dont track LP accounts
	if !IsLiquidityPool(from) {
		debitChange := builder.WithCategory(types.StateChangeCategoryDebit).
			WithAccount(from).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			Build()
		changes = append(changes, debitChange)
	}

	return changes, nil
}

// handleFee processes transaction fee events.
// Positive amounts are debits (fee charged), negative amounts are credits (fee refunds).
func (p *TokenTransferProcessor) handleFee(fee *ttp.Fee, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	amount := fee.GetAmount()
	var category types.StateChangeCategory
	var finalAmount string

	// Negative fee amounts represent refunds (common in Soroban transactions)
	if strings.HasPrefix(amount, "-") {
		category = types.StateChangeCategoryCredit
		// Store the refund as positive credit amount
		finalAmount = strings.TrimPrefix(amount, "-")
	} else {
		// Positive fee amounts are normal transaction costs
		category = types.StateChangeCategoryDebit
		finalAmount = amount
	}

	change := builder.WithCategory(category).
		WithAccount(fee.GetFrom()).
		WithAmount(finalAmount).
		WithAsset(fee.GetAsset(), contractAddress).
		Build()

	return []types.StateChange{change}, nil
}
