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
	txID := tx.ID()

	// Extract token transfer events from the transaction using Stellar SDK
	txEvents, err := p.eventsProcessor.EventsFromTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer events for transaction hash: %s, err: %w", txHash, err)
	}

	stateChanges := make([]types.StateChange, 0, len(txEvents.OperationEvents)+1)
	builder := NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, txID)

	// Process fee events
	feeChange, err := p.processFeeEvents(builder.Clone(), txEvents.FeeEvents)
	if err != nil {
		return nil, fmt.Errorf("processing fee events for transaction hash: %s, err: %w", txHash, err)
	}
	stateChanges = append(stateChanges, feeChange)

	for _, e := range txEvents.OperationEvents {
		meta := e.GetMeta()
		contractAddress := meta.GetContractAddress()
		// The input operationIndex is 0-indexed.
		// As per SEP-35, the OperationIndex in the output proto is 1-indexed.
		// So we need to subtract 1 from the input operationIndex to get the correct operation index.
		opIdx := meta.GetOperationIndex() - 1
		event := e.GetEvent()

		// For non-fee events, we need operation details to determine the correct state change type
		opID, opType, opSourceAccount := p.parseOperationDetails(tx, ledgerNumber, tx.Index, opIdx)
		changes := p.processNonFeeEvent(event, contractAddress, builder.Clone(), opID, opType, opSourceAccount)
		stateChanges = append(stateChanges, changes...)
	}

	return stateChanges, nil
}

// parseOperationDetails extracts operation metadata needed for processing token transfer events.
// Returns operation ID, type, and source account which determine how events should be categorized.
func (p *TokenTransferProcessor) parseOperationDetails(tx ingest.LedgerTransaction, ledgerIdx uint32, txIdx uint32, opIdx uint32) (int64, *xdr.OperationType, string) {
	op, _ := tx.GetOperation(opIdx)
	operationType := &op.Body.Type
	opSourceAccount := operationSourceAccount(tx, op)
	opID := toid.New(int32(ledgerIdx), int32(txIdx), int32(opIdx+1)).ToInt64()
	return opID, operationType, opSourceAccount
}

// processNonFeeEvent routes different types of non-fee token transfer events to their specific handlers.
// Each event type (transfer, mint, burn, clawback) requires different business logic.
func (p *TokenTransferProcessor) processNonFeeEvent(event any, contractAddress string, builder *StateChangeBuilder, opID int64, operationType *xdr.OperationType, opSourceAccount string) []types.StateChange {
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
		log.Debugf("unknown event type: %T", event)
		return nil
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
		return builder.
			WithCategory(types.StateChangeCategoryBalance).
			WithReason(types.StateChangeReasonDebit).Build(), nil
	}
	return builder.
		WithCategory(types.StateChangeCategoryBalance).
		WithReason(types.StateChangeReasonCredit).Build(), nil
}

// createStateChange creates a basic state change with the common fields.
func (p *TokenTransferProcessor) createStateChange(category types.StateChangeCategory, reason types.StateChangeReason, account, amount, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	b := builder.WithCategory(category).
		WithReason(reason).
		WithAccount(account).
		WithAmount(amount)

	if contractAddress != "" {
		b = b.WithToken(contractAddress)
	}

	return b.Build()
}

// createDebitCreditPair creates a pair of debit and credit state changes for normal transfers.
// Used for regular payments between two accounts (e.g., Alice sends 100 USDC to Bob).
func (p *TokenTransferProcessor) createDebitCreditPair(from, to, amount string, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	change := builder.
		WithToken(contractAddress).
		WithAmount(amount)

	debitChange := change.Clone().WithCategory(types.StateChangeCategoryBalance).WithReason(types.StateChangeReasonDebit).WithAccount(from).Build()
	creditChange := change.Clone().WithCategory(types.StateChangeCategoryBalance).WithReason(types.StateChangeReasonCredit).WithAccount(to).Build()

	return []types.StateChange{debitChange, creditChange}
}

// handleTransfer processes transfer events based on the operation type.
// Different operations require different state change patterns:
// - Claimable balances: single debit/credit since we dont record claimable balance IDs as accounts
// - Liquidity pools: single debit/credit since we dont record liquidity pool IDs as accounts
// - Regular transfers: debit/credit pair between accounts
func (p *TokenTransferProcessor) handleTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType) []types.StateChange {
	switch *operationType {
	case xdr.OperationTypeCreateClaimableBalance, xdr.OperationTypeLiquidityPoolDeposit:
		// When creating a claimable balance, record debit from creator with CB ID
		change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonDebit, transfer.GetFrom(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}

	case xdr.OperationTypeClaimClaimableBalance, xdr.OperationTypeLiquidityPoolWithdraw:
		// When claiming a claimable balance, record credit to claimer with CB ID
		change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonCredit, transfer.GetTo(), transfer.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}

	case xdr.OperationTypeSetTrustLineFlags, xdr.OperationTypeAllowTrust:
		// Skip events generated by these operations since they involve only an LP and Claimable Balance ID
		return nil

	default:
		if isLiquidityPool(transfer.GetFrom()) || isLiquidityPool(transfer.GetTo()) {
			return p.handleTransfersWithLiquidityPool(transfer, contractAddress, builder)
		}

		// For account creation and merge, we add a 3rd state change (ACCOUNT/CREATE, ACCOUNT/MERGE) apart from the debit and credit ones.
		stateChanges := []types.StateChange{}
		//exhaustive:ignore
		switch *operationType {
		case xdr.OperationTypeCreateAccount:
			funder := transfer.GetFrom()
			stateChanges = append(stateChanges, p.createStateChange(types.StateChangeCategoryAccount, types.StateChangeReasonCreate, transfer.GetTo(), "", "", builder.Clone().WithFunder(funder)))
		case xdr.OperationTypeAccountMerge:
			stateChanges = append(stateChanges, p.createStateChange(types.StateChangeCategoryAccount, types.StateChangeReasonMerge, transfer.GetTo(), "", "", builder.Clone()))
		}

		// Normal transfer between two accounts
		stateChanges = append(stateChanges, p.createDebitCreditPair(transfer.GetFrom(), transfer.GetTo(), transfer.GetAmount(), contractAddress, builder)...)
		return stateChanges
	}
}

// handleTransfersWithLiquidityPool handles transfers between liquidity pools and accounts.
// This is a special case where a liquidity pool is the source or destination account which could occur when path payments go through liquidity pools.
func (p *TokenTransferProcessor) handleTransfersWithLiquidityPool(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	from := transfer.GetFrom()
	to := transfer.GetTo()
	amount := transfer.GetAmount()

	// LP is sending tokens to account (e.g., path payment buying from LP)
	if isLiquidityPool(from) {
		change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonCredit, to, amount, contractAddress, builder)
		return []types.StateChange{change}
	}

	// LP is receiving tokens from account (e.g., path payment selling to LP)
	change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonDebit, from, amount, contractAddress, builder)
	return []types.StateChange{change}
}

// handleMint processes mint events that occur when asset issuers create new tokens.
// This happens when an issuer sends their own asset to another account (e.g., USDC payment from USDC issuer).
func (p *TokenTransferProcessor) handleMint(mint *ttp.Mint, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	asset := mint.GetAsset()
	var changes []types.StateChange

	// For issued assets, record mint for the issuer account
	if !asset.GetNative() {
		mintChange := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonMint, asset.GetIssuedAsset().GetIssuer(), mint.GetAmount(), contractAddress, builder)
		changes = append(changes, mintChange)
	}

	// Create credit state change for the receiving account. Skip mints to liquidity pools since we dont track LP accounts
	if !isLiquidityPool(mint.GetTo()) {
		creditChange := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonCredit, mint.GetTo(), mint.GetAmount(), contractAddress, builder)
		changes = append(changes, creditChange)
	}

	return changes
}

// handleBurn processes burn events that occur when tokens are destroyed.
// Burns happen when: 1) tokens are sent to their issuer, 2) issuer claims claimable balance, 3) issuer withdraws from LP.
func (p *TokenTransferProcessor) handleBurn(burn *ttp.Burn, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) []types.StateChange {
	asset := burn.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClaimClaimableBalance, xdr.OperationTypeLiquidityPoolWithdraw:
		change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonBurn, opSourceAccount, burn.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}

	default:
		// Regular burn (payment to issuer) creates burn + debit pair
		return p.handleDefaultBurnOrClawback(burn.GetFrom(), burn.GetAmount(), asset, contractAddress, builder)
	}
}

// handleClawback processes clawback events where an asset issuer forcibly reclaims tokens.
// Similar to burns but initiated by issuer rather than voluntary transfer to issuer.
func (p *TokenTransferProcessor) handleClawback(clawback *ttp.Clawback, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) []types.StateChange {
	asset := clawback.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClawbackClaimableBalance:
		change := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonBurn, opSourceAccount, clawback.GetAmount(), contractAddress, builder)
		return []types.StateChange{change}

	default:
		// Regular clawback creates burn (issuer) + debit (victim) pair
		return p.handleDefaultBurnOrClawback(clawback.GetFrom(), clawback.GetAmount(), asset, contractAddress, builder)
	}
}

// handleDefaultBurnOrClawback creates state changes for regular burns and clawbacks.
// For non-native assets: creates burn (issuer receives) + debit (sender loses) pair.
// For native XLM: only creates debit since there's no issuer to burn from.
func (p *TokenTransferProcessor) handleDefaultBurnOrClawback(from string, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) []types.StateChange {
	var changes []types.StateChange

	// For issued assets, record burn at the issuer account
	if !asset.GetNative() {
		burnChange := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonBurn, asset.GetIssuedAsset().GetIssuer(), amount, contractAddress, builder)
		changes = append(changes, burnChange)
	}

	// Always record debit from the account losing the tokens. Skip burns from LP accounts since we dont track LP accounts
	if !isLiquidityPool(from) {
		debitChange := p.createStateChange(types.StateChangeCategoryBalance, types.StateChangeReasonDebit, from, amount, contractAddress, builder)
		changes = append(changes, debitChange)
	}

	return changes
}
