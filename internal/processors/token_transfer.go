package processors

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/stellar/go/asset"
	"github.com/stellar/go/ingest"
	ttp "github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrOperationNotFound = errors.New("operation not found")

type TokenTransferProcessor struct {
	eventsProcessor *ttp.EventsProcessor
}

// StateChangeBuilder provides a fluent interface for creating state changes
type StateChangeBuilder struct {
	base types.StateChange
}

// newStateChangeBuilder creates a new builder with base state change fields
func (p *TokenTransferProcessor) newStateChangeBuilder(ledgerNumber uint32, ledgerCloseTime int64, txHash, opID string) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			LedgerNumber:    int64(ledgerNumber),
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
			IngestedAt:      time.Now(),
			TxHash:          txHash,
			OperationID:     opID,
		},
	}
}

// WithCategory sets the state change category and account
func (b *StateChangeBuilder) WithCategory(category types.StateChangeCategory) *StateChangeBuilder {
	b.base.StateChangeCategory = category
	return b
}

// WithAccount sets the account ID
func (b *StateChangeBuilder) WithAccount(accountID string) *StateChangeBuilder {
	b.base.AccountID = accountID
	return b
}

// WithAmount sets the amount
func (b *StateChangeBuilder) WithAmount(amount string) *StateChangeBuilder {
	b.base.Amount = sql.NullString{String: amount}
	return b
}

// WithAsset sets the asset or contract
func (b *StateChangeBuilder) WithAsset(asset *asset.Asset, contractAddress string) *StateChangeBuilder {
	if asset != nil {
		if asset.GetNative() {
			b.base.Token = sql.NullString{String: "native"}
		} else if issuedAsset := asset.GetIssuedAsset(); issuedAsset != nil {
			b.base.Token = sql.NullString{String: fmt.Sprintf("%s:%s", issuedAsset.GetAssetCode(), issuedAsset.GetIssuer())}
		}
	} else {
		b.base.ContractID = sql.NullString{String: contractAddress}
	}
	return b
}

// WithClaimableBalance sets the claimable balance ID
func (b *StateChangeBuilder) WithClaimableBalance(balanceID string) *StateChangeBuilder {
	b.base.ClaimableBalanceID = sql.NullString{String: balanceID}
	return b
}

// WithLiquidityPool sets the liquidity pool ID
func (b *StateChangeBuilder) WithLiquidityPool(poolID string) *StateChangeBuilder {
	b.base.LiquidityPoolID = sql.NullString{String: poolID}
	return b
}

// Build returns the constructed state change
func (b *StateChangeBuilder) Build() types.StateChange {
	return b.base
}

func NewTokenTransferProcessor(networkPassphrase string) *TokenTransferProcessor {
	return &TokenTransferProcessor{
		eventsProcessor: ttp.NewEventsProcessor(networkPassphrase),
	}
}

func (p *TokenTransferProcessor) Process(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error) {
	ledgerCloseTime := tx.Ledger.LedgerCloseTime()
	ledgerNumber := tx.Ledger.LedgerSequence()
	txHash := tx.Hash.HexString()
	txIdx := tx.Index

	// Get events from this specific transaction
	txEvents, err := p.eventsProcessor.EventsFromTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer events from transaction: %w", err)
	}

	// Combine fee events and operation events
	allEvents := append(txEvents.FeeEvents, txEvents.OperationEvents...)

	stateChanges := make([]types.StateChange, 0, len(allEvents))
	for _, e := range allEvents {
		meta := e.GetMeta()
		contractAddress := meta.GetContractAddress()
		opIdx := meta.GetOperationIndex()
		event := e.GetEvent()

		var opID, opSourceAccount string
		var opType *xdr.OperationType
		if _, isFeeEvent := event.(*ttp.TokenTransferEvent_Fee); !isFeeEvent {
			opID, opType, opSourceAccount, err = p.parseOperationDetails(tx, ledgerNumber, txIdx, opIdx)
			if err != nil {
				if errors.Is(err, ErrOperationNotFound) {
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

func (p *TokenTransferProcessor) parseOperationDetails(tx ingest.LedgerTransaction, ledgerIdx uint32, txIdx uint32, opIdx uint32) (string, *xdr.OperationType, string, error) {
	op, found := tx.GetOperation(opIdx - 1)
	if !found {
		return "", nil, "", ErrOperationNotFound
	}

	operationType := &op.Body.Type
	opSourceAccount := operationSourceAccount(tx, op)
	opID := utils.OperationID(int32(ledgerIdx), int32(txIdx), int32(opIdx))

	return opID, operationType, opSourceAccount, nil
}

func operationSourceAccount(tx ingest.LedgerTransaction, op xdr.Operation) string {
	acc := op.SourceAccount
	if acc != nil {
		return acc.ToAccountId().Address()
	}
	res := tx.Envelope.SourceAccount()
	return res.ToAccountId().Address()
}

func (p *TokenTransferProcessor) processEvent(event any, contractAddress string, ledgerNumber uint32, ledgerCloseTime int64, txHash, opID string, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	builder := p.newStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, opID)

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

// createDebitCreditPair creates a pair of debit and credit state changes for normal transfers
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

// createLiquidityPoolChange creates a state change for liquidity pool interactions
func (p *TokenTransferProcessor) createLiquidityPoolChange(category types.StateChangeCategory, accountID, poolID, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		WithLiquidityPool(poolID).
		Build()
}

func (p *TokenTransferProcessor) createClaimableBalanceChange(category types.StateChangeCategory, accountID, claimableBalanceID, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) types.StateChange {
	return builder.WithCategory(category).
		WithAccount(accountID).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		WithClaimableBalance(claimableBalanceID).
		Build()
}

func (p *TokenTransferProcessor) handleTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType) ([]types.StateChange, error) {
	amount := transfer.GetAmount()
	asset := transfer.GetAsset()

	switch *operationType {
	case xdr.OperationTypeCreateClaimableBalance:
		change := p.createClaimableBalanceChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeClaimClaimableBalance:
		change := p.createClaimableBalanceChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolDeposit:
		change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, transfer.GetFrom(), transfer.GetTo(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, transfer.GetTo(), transfer.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeSetTrustLineFlags, xdr.OperationTypeAllowTrust:
		// Skip events generated by these operations since they involve only an LP and Claimable Balance ID
		return nil, nil

	default:
		return p.handleDefaultTransfer(transfer, contractAddress, builder)
	}
}

// handleDefaultTransfer handles normal transfers and liquidity pool interactions
func (p *TokenTransferProcessor) handleDefaultTransfer(transfer *ttp.Transfer, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	from := transfer.GetFrom()
	to := transfer.GetTo()
	amount := transfer.GetAmount()
	asset := transfer.GetAsset()

	fromIsLP := isLiquidityPool(from)
	toIsLP := isLiquidityPool(to)

	// Handle liquidity pool interactions
	if fromIsLP || toIsLP {
		if fromIsLP {
			// LP is sending, so this is a credit to the non-LP account
			change := p.createLiquidityPoolChange(types.StateChangeCategoryCredit, to, from, amount, asset, contractAddress, builder)
			return []types.StateChange{change}, nil
		}
		// LP is receiving, so this is a debit from the non-LP account
		change := p.createLiquidityPoolChange(types.StateChangeCategoryDebit, from, to, amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil
	}

	// Normal transfer between two non-LP accounts
	return p.createDebitCreditPair(from, to, amount, asset, contractAddress, builder), nil
}

func (p *TokenTransferProcessor) handleMint(mint *ttp.Mint, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	if isLiquidityPool(mint.GetTo()) {
		return nil, nil
	}

	change := builder.WithCategory(types.StateChangeCategoryMint).
		WithAccount(mint.GetTo()).
		WithAmount(mint.GetAmount()).
		WithAsset(mint.GetAsset(), contractAddress).
		Build()

	return []types.StateChange{change}, nil
}

func (p *TokenTransferProcessor) handleBurn(burn *ttp.Burn, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	amount := burn.GetAmount()
	asset := burn.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClaimClaimableBalance:
		// If the claimable balance is claimed by the issuer, it will be a burn state change for the issuer (operation source account)
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, burn.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	case xdr.OperationTypeLiquidityPoolWithdraw:
		// If an asset issuer is withdrawing from the LP, it will be a burn state change for the issuer (operation source account)
		change := builder.WithCategory(types.StateChangeCategoryBurn).
			WithAccount(opSourceAccount).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			WithLiquidityPool(burn.GetFrom()).
			Build()
		return []types.StateChange{change}, nil

	default:
		// The asset issuer has a burn change for their account while the other account has the asset debited
		return p.handleDefaultBurnOrClawback(burn.GetFrom(),amount, asset, contractAddress, builder)
	}
}

func (p *TokenTransferProcessor) handleClawback(clawback *ttp.Clawback, contractAddress string, builder *StateChangeBuilder, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	amount := clawback.GetAmount()
	asset := clawback.GetAsset()

	switch *operationType {
	case xdr.OperationTypeClawbackClaimableBalance:
		change := p.createClaimableBalanceChange(types.StateChangeCategoryBurn, opSourceAccount, clawback.GetFrom(), amount, asset, contractAddress, builder)
		return []types.StateChange{change}, nil

	default:
		// The asset issuer has a burn change for their account while the other account has the asset debited
		return p.handleDefaultBurnOrClawback(clawback.GetFrom(), amount, asset, contractAddress, builder)
	}
}

func (p *TokenTransferProcessor) handleDefaultBurnOrClawback(from string, amount string, asset *asset.Asset, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	var changes []types.StateChange

	// Add burn change for non-native assets
	if !asset.GetNative() {
		burnChange := builder.WithCategory(types.StateChangeCategoryBurn).
			WithAccount(asset.GetIssuedAsset().GetIssuer()).
			WithAmount(amount).
			WithAsset(asset, contractAddress).
			Build()
		changes = append(changes, burnChange)
	}

	// Add debit change
	debitChange := builder.WithCategory(types.StateChangeCategoryDebit).
		WithAccount(from).
		WithAmount(amount).
		WithAsset(asset, contractAddress).
		Build()
	changes = append(changes, debitChange)

	return changes, nil
}

func (p *TokenTransferProcessor) handleFee(fee *ttp.Fee, contractAddress string, builder *StateChangeBuilder) ([]types.StateChange, error) {
	amount := fee.GetAmount()
	var category types.StateChangeCategory
	var finalAmount string

	// If a fee event is negative, it represents a refund. By default, a "fee" represents a debit but a negative fee represents a credit.
	if strings.HasPrefix(amount, "-") {
		category = types.StateChangeCategoryCredit
		// Store the fee as a credit refund without the negative sign
		finalAmount = strings.TrimPrefix(amount, "-")
	} else {
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

// isLiquidityPool checks if the given account ID is a liquidity pool
func isLiquidityPool(accountID string) bool {
	// Try to decode the account ID as a strkey
	versionByte, _, err := strkey.DecodeAny(accountID)
	if err != nil {
		return false
	}
	// Check if it's a liquidity pool strkey
	return versionByte == strkey.VersionByteLiquidityPool
}
