package processors

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/stellar/go/asset"
	"github.com/stellar/go/ingest"
	ttp "github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var ErrOperationNotFound = errors.New("operation not found")

// TokenTransferProcessor processes Stellar transactions and extracts token transfer events.
// It converts Stellar operations (payments, account merges, etc.) into our internal (synthetic) state changes representation.
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
	txHash := tx.Result.TransactionHash.HexString()

	// Extract token transfer events from the transaction using Stellar SDK
	txEvents, err := p.eventsProcessor.EventsFromTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer events for transaction hash: %s, err: %w", txHash, err)
	}

	stateChanges := make([]types.StateChange, 0, len(txEvents.OperationEvents)+1)
	builder := NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash)

	// Process fee events
	feeChange, err := p.processFeeEvents(builder.Clone(), txEvents.FeeEvents)
	if err != nil {
		return nil, fmt.Errorf("processing fee events for transaction hash: %s, err: %w", txHash, err)
	}
	stateChanges = append(stateChanges, feeChange)

	for _, e := range txEvents.OperationEvents {
		meta := e.GetMeta()
		contractAddress := meta.GetContractAddress()
		opIdx := meta.GetOperationIndex()
		event := e.GetEvent()

		// For non-fee events, we need operation details to determine the correct state change type
		opID, opType, opSourceAccount, err := p.parseOperationDetails(tx, ledgerNumber, tx.Index, opIdx)
		if err != nil {
			if errors.Is(err, ErrOperationNotFound) {
				// While we should never see this since ttp sends valid events, this is meant as a defensive check to ensure
				// the indexer doesn't crash. We still log it to help debug issues.
				log.Ctx(ctx).Debugf("skipping event for operation that couldn't be found: txHash: %s, opID: %d", txHash, opID)
				continue
			}
			return nil, fmt.Errorf("parsing operation details for transaction hash: %s, operation ID: %d, err: %w", txHash, opID, err)
		}

		changes, err := p.processNonFeeEvent(event, contractAddress, builder.Clone(), opID, opType, opSourceAccount)
		if err != nil {
			return nil, err
		}
		stateChanges = append(stateChanges, changes...)
	}

	return stateChanges, nil
}

// parseOperationDetails extracts operation metadata needed for processing token transfer events.
// Returns operation ID, type, and source account which determine how events should be categorized.
func (p *TokenTransferProcessor) parseOperationDetails(tx ingest.LedgerTransaction, ledgerIdx uint32, txIdx uint32, opIdx uint32) (int64, *xdr.OperationType, string, error) {
	op, found := tx.GetOperation(opIdx - 1)
	if !found {
		return 0, nil, "", ErrOperationNotFound
	}

	operationType := &op.Body.Type
	opSourceAccount := OperationSourceAccount(tx, op)
	opID := toid.New(int32(ledgerIdx), int32(txIdx), int32(opIdx+1)).ToInt64()

	return opID, operationType, opSourceAccount, nil
}

// processNonFeeEvent routes different types of non-fee token transfer events to their specific handlers.
// Each event type (transfer, mint, burn, clawback) requires different business logic.
func (p *TokenTransferProcessor) processNonFeeEvent(event any, contractAddress string, builder *StateChangeBuilder, opID int64, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	builder = builder.WithOperationID(opID)

	switch event := event.(type) {
	case *ttp.TokenTransferEvent_Transfer:
		return p.handleTransfer(event.Transfer, contractAddress, builder, operationType)
	case *ttp.TokenTransferEvent_Mint:
		return p.handleMint(event.Mint, contractAddress, builder)
	case *ttp.TokenTransferEvent_Burn:
		return p.handleBurn(event.Burn, contractAddress, builder, operationType, opSourceAccount)
	case *ttp.TokenTransferEvent_Clawback:
		return p.handleClawback(event.Clawback, contractAddress, builder, operationType, opSourceAccount)
	default:
		return nil, fmt.Errorf("unknown event type: %T", event)
	}
}

// processFeeEvents processes both fee and refund events and calculates a state change with the net fee per transaction.
func (p *TokenTransferProcessor) processFeeEvents(builder *StateChangeBuilder, feeEvents []*ttp.TokenTransferEvent) (types.StateChange, error) {
	if len(feeEvents) == 0 {
		return types.StateChange{}, nil
	}

	// Use the first event for metadata (account, token)
	firstEvent := feeEvents[0]
	assetContractID := firstEvent.GetMeta().GetContractAddress()
	builder = builder.
		WithAccount(firstEvent.GetFee().GetFrom()).
		WithToken(assetContractID)

	// Calculate net fee by summing all fee events
	var netFee int64
	for _, feeEvent := range feeEvents {
		amount, err := strconv.ParseInt(feeEvent.GetFee().GetAmount(), 10, 64)
		if err != nil {
			return types.StateChange{}, fmt.Errorf("parsing fee amount for fee event: %w", err)
		}
		netFee += amount
	}

	// There is no fee to process
	if netFee == 0 {
		return types.StateChange{}, nil
	}

	builder = builder.WithAmount(strconv.FormatInt(netFee, 10))
	if netFee > 0 {
		return builder.WithCategory(types.StateChangeCategoryDebit).Build(), nil
	}
	return builder.WithCategory(types.StateChangeCategoryCredit).Build(), nil
}

// createStateChange creates a basic state change with the common fields.
func (p *TokenTransferProcessor) createStateChange(category types.StateChangeCategory, account, amount, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(account).
		WithAmount(amount).
		WithToken(contractAddress).
		Build()
}

// createDebitCreditPair creates a pair of debit and credit state changes for normal transfers.
// Used for regular payments between two accounts (e.g., Alice sends 100 USDC to Bob).
func (p *TokenTransferProcessor) createDebitCreditPair(from, to, amount string, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	change := builder.
		WithToken(contractAddress).
		WithAmount(amount)

	debitChange := change.Clone().WithCategory(types.StateChangeCategoryDebit).WithAccount(from).Build()
	creditChange := change.Clone().WithCategory(types.StateChangeCategoryCredit).WithAccount(to).Build()

	return []types.StateChange{debitChange, creditChange}
}

// createLiquidityPoolChange creates a state change for liquidity pool interactions.
// Includes the liquidity pool ID to track which pool the tokens moved to/from.
func (p *TokenTransferProcessor) createLiquidityPoolChange(category types.StateChangeCategory, accountID, poolID, amount string, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithToken(contractAddress).
		WithLiquidityPool(poolID).
		Build()
}

// createClaimableBalanceChange creates a state change for claimable balance interactions.
// Includes the claimable balance ID to track which balance was created, claimed, or clawed back.
func (p *TokenTransferProcessor) createClaimableBalanceChange(category types.StateChangeCategory, accountID, claimableBalanceID, amount string, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithToken(contractAddress).
		WithClaimableBalance(claimableBalanceID).
		Build()
}

// handleTransfer processes transfer events based on the operation type.
// Different operations require different state change patterns:
// - Claimable balances: single debit/credit with claimable balance ID
// - Liquidity pools: single debit/credit with liquidity pool ID
// - Regular transfers: debit/credit pair between accounts
func (p *TokenTransferProcessor) handleTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType) ([]types.StateChange, error) {
	switch *operationType {
	case xdr.OperationTypeCreateClaimableBalance:
		// When creating a claimable balance, record debit from creator with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeClaimClaimableBalance:
		// When claiming a claimable balance, record credit to claimer with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolDeposit:
		// When depositing to LP, record debit from depositor with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		// When withdrawing from LP, record credit to withdrawer with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeSetTrustLineFlags, xdr.OperationTypeAllowTrust:
		// Skip events generated by these operations since they involve only an LP and Claimable Balance ID
		return nil, nil

	default:
		if IsLiquidityPool(transfer.GetFrom()) || IsLiquidityPool(transfer.GetTo()) {
			return p.handleTransfersWithLiquidityPool(transfer, contractAddress, builder)
		}

		// Normal transfer between two accounts (payments, account merge)
		return p.createDebitCreditPair(transfer.GetFrom(), transfer.GetTo(), transfer.GetAmount(), contractAddress, builder), nil
	}
}

// handleTransfersWithLiquidityPool handles transfers between liquidity pools and accounts.
// This is a special case where a liquidity pool is the source or destination account which could occur when path payments go through liquidity pools.
func (p *TokenTransferProcessor) handleTransfersWithLiquidityPool(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	from := transfer.GetFrom()
	to := transfer.GetTo()
	amount := transfer.GetAmount()

	// LP is sending tokens to account (e.g., path payment buying from LP)
	if IsLiquidityPool(from) {
		change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, to, from, amount, contractAddress, builder)
		return []types.StateChange{change}, nil
	}

	// LP is receiving tokens from account (e.g., path payment selling to LP)
	change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, from, to, amount, contractAddress, builder)
	return []types.StateChange{change}, nil
}

// handleMint processes mint events that occur when asset issuers create new tokens.
// This happens when an issuer sends their own asset to another account (e.g., USDC payment from USDC issuer).
func (p *TokenTransferProcessor) handleMint(mint *ttp.Mint, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	asset := mint.GetAsset()
	var changes []types.StateChange

	// For issued assets, record mint for the issuer account
	if !asset.GetNative() {
		mintChange := p.createStateChange(types.StateChangeCategoryMint, asset.GetIssuedAsset().GetIssuer(), mint.GetAmount(), contractAddress, builder)
		changes = append(changes, mintChange)
	}

	// Create credit state change for the receiving account. Skip mints to liquidity pools since we dont track LP accounts
	if !IsLiquidityPool(mint.GetTo()) {
		creditChange := p.createStateChange(types.StateChangeCategoryCredit, mint.GetTo(), mint.GetAmount(), contractAddress, builder)
		changes = append(changes, creditChange)
	}

	return changes, nil
}

// handleBurn processes burn events that occur when tokens are destroyed.
// Burns happen when: 1) tokens are sent to their issuer, 2) issuer claims claimable balance, 3) issuer withdraws from LP.
func (p *TokenTransferProcessor) handleBurn(burn *ttp.Burn, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	asset := burn.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClaimClaimableBalance:
		// When issuer claims their own asset from claimable balance, it's burned with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, burn.GetFrom(), burn.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		// When issuer withdraws their own asset from LP, it's burned with LP ID
		change := p.createLiquidityPoolChange(types.StateChangeCategoryBurn, opSourceAccount, burn.GetFrom(), burn.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	default:
		// Regular burn (payment to issuer) creates burn + debit pair
		return p.handleDefaultBurnOrClawback(burn.GetFrom(), burn.GetAmount(), asset, contractAddress, builder)
	}
}

// handleClawback processes clawback events where an asset issuer forcibly reclaims tokens.
// Similar to burns but initiated by issuer rather than voluntary transfer to issuer.
func (p *TokenTransferProcessor) handleClawback(clawback *ttp.Clawback, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	asset := clawback.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClawbackClaimableBalance:
		// Clawback of claimable balance creates burn with CB ID
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, clawback.GetFrom(), clawback.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}, nil

	default:
		// Regular clawback creates burn (issuer) + debit (victim) pair
		return p.handleDefaultBurnOrClawback(clawback.GetFrom(), clawback.GetAmount(), asset, contractAddress, builder)
	}
}

// handleDefaultBurnOrClawback creates state changes for regular burns and clawbacks.
// For non-native assets: creates burn (issuer receives) + debit (sender loses) pair.
// For native XLM: only creates debit since there's no issuer to burn from.
func (p *TokenTransferProcessor) handleDefaultBurnOrClawback(from string, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	var changes []types.StateChange

	// For issued assets, record burn at the issuer account
	if !asset.GetNative() {
		burnChange := p.createStateChange(types.StateChangeCategoryBurn, asset.GetIssuedAsset().GetIssuer(), amount, contractAddress, builder)
		changes = append(changes, burnChange)
	}

	// Always record debit from the account losing the tokens. Skip burns from LP accounts since we dont track LP accounts
	if !IsLiquidityPool(from) {
		debitChange := p.createStateChange(types.StateChangeCategoryDebit, from, amount, contractAddress, builder)
		changes = append(changes, debitChange)
	}

	return changes, nil
}
