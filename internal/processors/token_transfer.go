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
		var err error
		if _, isFeeEvent := event.(*ttp.TokenTransferEvent_Fee); !isFeeEvent {
			opID, opType, opSourceAccount, err = p.parseOperationDetails(tx, ledgerNumber, txIdx, opIdx)
			if err != nil {
				if errors.Is(err, ErrOperationNotFound) {
					continue
				}
				return nil, fmt.Errorf("parsing operation details: %w", err)
			}
		}

		baseChange := p.createBaseStateChange(ledgerNumber, ledgerCloseTime, txHash, opID)
		changes, err := p.processEvent(event, contractAddress, baseChange, opType, opSourceAccount)
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
	var opSourceAccount string
	if op.SourceAccount != nil {
		opSourceAccount = op.SourceAccount.ToAccountId().Address()
	} else {
		opSourceAccount = tx.Envelope.SourceAccount().ToAccountId().Address()
	}
	opID := utils.OperationID(int32(ledgerIdx), int32(txIdx), int32(opIdx))

	return opID, operationType, opSourceAccount, nil
}

func (p *TokenTransferProcessor) processEvent(event any, contractAddress string, baseChange types.StateChange, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	switch event := event.(type) {
	case *ttp.TokenTransferEvent_Transfer:
		return p.handleTransfer(event.Transfer, contractAddress, baseChange, operationType)
	case *ttp.TokenTransferEvent_Mint:
		return p.handleMint(event.Mint, contractAddress, baseChange)
	case *ttp.TokenTransferEvent_Burn:
		return p.handleBurn(event.Burn, contractAddress, baseChange, operationType, opSourceAccount)
	case *ttp.TokenTransferEvent_Clawback:
		return p.handleClawback(event.Clawback, contractAddress, baseChange, operationType, opSourceAccount)
	case *ttp.TokenTransferEvent_Fee:
		return p.handleFee(event.Fee, contractAddress, baseChange)
	default:
		return nil, fmt.Errorf("unknown event type: %T", event)
	}
}

func (p *TokenTransferProcessor) createBaseStateChange(ledgerNumber uint32, ledgerCloseTime int64, txHash string, opID string) types.StateChange {
	change := types.StateChange{
		LedgerNumber:    int64(ledgerNumber),
		LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
		IngestedAt:      time.Now(),
		TxHash:          txHash,
		OperationID:     opID,
	}

	return change
}

func (p *TokenTransferProcessor) setAssetOrContract(change *types.StateChange, asset *asset.Asset, contractAddress string) {
	if asset != nil {
		if asset.GetNative() {
			change.Token = sql.NullString{String: "native"}
		} else if issuedAsset := asset.GetIssuedAsset(); issuedAsset != nil {
			change.Token = sql.NullString{String: fmt.Sprintf("%s:%s", issuedAsset.GetAssetCode(), issuedAsset.GetIssuer())}
		}
	} else {
		change.ContractID = sql.NullString{String: contractAddress}
	}
}

func (p *TokenTransferProcessor) handleTransfer(transfer *ttp.Transfer, contractAddress string, baseChange types.StateChange, operationType *xdr.OperationType) ([]types.StateChange, error) {
	switch *operationType {
	case xdr.OperationTypeCreateClaimableBalance:
		baseChange.StateChangeCategory = types.StateChangeCategoryDebit
		baseChange.AccountID = transfer.GetFrom()
		baseChange.ClaimableBalanceID = sql.NullString{String: transfer.GetTo()}
	case xdr.OperationTypeClaimClaimableBalance:
		baseChange.StateChangeCategory = types.StateChangeCategoryCredit
		baseChange.AccountID = transfer.GetTo()
		baseChange.ClaimableBalanceID = sql.NullString{String: transfer.GetFrom()}
	case xdr.OperationTypeLiquidityPoolDeposit:
		baseChange.StateChangeCategory = types.StateChangeCategoryDebit
		baseChange.AccountID = transfer.GetFrom()
		baseChange.LiquidityPoolID = sql.NullString{String: transfer.GetTo()}
	case xdr.OperationTypeLiquidityPoolWithdraw:
		baseChange.StateChangeCategory = types.StateChangeCategoryCredit
		baseChange.AccountID = transfer.GetTo()
		baseChange.LiquidityPoolID = sql.NullString{String: transfer.GetFrom()}
	case xdr.OperationTypeSetTrustLineFlags, xdr.OperationTypeAllowTrust:
		// We skip events generated by these operations since they involve only an LP and Claimable Balance ID.
		return nil, nil
	default:
		// Check if either the from or to account is a liquidity pool
		fromIsLP := isLiquidityPool(transfer.GetFrom())
		toIsLP := isLiquidityPool(transfer.GetTo())

		// If one side is an LP, create a single state change for the non-LP account
		if fromIsLP || toIsLP {
			var change types.StateChange
			if fromIsLP {
				// LP is sending, so this is a credit to the non-LP account
				change = baseChange
				change.StateChangeCategory = types.StateChangeCategoryCredit
				change.AccountID = transfer.GetTo()
				change.LiquidityPoolID = sql.NullString{String: transfer.GetFrom(), Valid: true}
			} else {
				// LP is receiving, so this is a debit from the non-LP account
				change = baseChange
				change.StateChangeCategory = types.StateChangeCategoryDebit
				change.AccountID = transfer.GetFrom()
				change.LiquidityPoolID = sql.NullString{String: transfer.GetTo(), Valid: true}
			}
			change.Amount = sql.NullString{String: transfer.GetAmount()}
			p.setAssetOrContract(&change, transfer.GetAsset(), contractAddress)
			return []types.StateChange{change}, nil
		}

		// Normal transfer between two non-LP accounts
		debitChange := baseChange
		debitChange.StateChangeCategory = types.StateChangeCategoryDebit
		debitChange.AccountID = transfer.GetFrom()
		debitChange.Amount = sql.NullString{String: transfer.GetAmount()}

		creditChange := baseChange
		creditChange.StateChangeCategory = types.StateChangeCategoryCredit
		creditChange.AccountID = transfer.GetTo()
		creditChange.Amount = sql.NullString{String: transfer.GetAmount()}

		p.setAssetOrContract(&debitChange, transfer.GetAsset(), contractAddress)
		p.setAssetOrContract(&creditChange, transfer.GetAsset(), contractAddress)
		return []types.StateChange{debitChange, creditChange}, nil
	}

	baseChange.Amount = sql.NullString{String: transfer.GetAmount()}
	p.setAssetOrContract(&baseChange, transfer.GetAsset(), contractAddress)
	return []types.StateChange{baseChange}, nil
}

func (p *TokenTransferProcessor) handleMint(mint *ttp.Mint, contractAddress string, baseChange types.StateChange) ([]types.StateChange, error) {
	// We skip events involving only LP accounts
	toIsLP := isLiquidityPool(mint.GetTo())
	if toIsLP {
		return nil, nil
	}

	baseChange.StateChangeCategory = types.StateChangeCategoryMint
	baseChange.AccountID = mint.GetTo()
	baseChange.Amount = sql.NullString{String: mint.GetAmount()}
	p.setAssetOrContract(&baseChange, mint.GetAsset(), contractAddress)
	return []types.StateChange{baseChange}, nil
}

func (p *TokenTransferProcessor) handleBurn(burn *ttp.Burn, contractAddress string, baseChange types.StateChange, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	// We skip events involving only LP accounts
	fromIsLP := isLiquidityPool(burn.GetFrom())
	if fromIsLP {
		return nil, nil
	}

	switch *operationType {
	// If the claimable balance is claimed by the issuer, it will be a burn state change for the issuer (operation source account).
	case xdr.OperationTypeClaimClaimableBalance:
		baseChange.AccountID = opSourceAccount
		baseChange.ClaimableBalanceID = sql.NullString{String: burn.GetFrom()}
	default:
		baseChange.AccountID = burn.GetFrom()
	}

	baseChange.StateChangeCategory = types.StateChangeCategoryBurn
	baseChange.Amount = sql.NullString{String: burn.GetAmount()}
	p.setAssetOrContract(&baseChange, burn.GetAsset(), contractAddress)
	return []types.StateChange{baseChange}, nil
}

func (p *TokenTransferProcessor) handleClawback(clawback *ttp.Clawback, contractAddress string, baseChange types.StateChange, operationType *xdr.OperationType, opSourceAccount string) ([]types.StateChange, error) {
	switch *operationType {
	case xdr.OperationTypeClawback:
		baseChange.StateChangeCategory = types.StateChangeCategoryDebit
		baseChange.AccountID = clawback.GetFrom()
	case xdr.OperationTypeClawbackClaimableBalance:
		baseChange.StateChangeCategory = types.StateChangeCategoryBurn
		baseChange.AccountID = opSourceAccount
		baseChange.ClaimableBalanceID = sql.NullString{String: clawback.GetFrom()}
	default:
		baseChange.AccountID = clawback.GetFrom()
		baseChange.StateChangeCategory = types.StateChangeCategoryBurn
	}

	baseChange.Amount = sql.NullString{String: clawback.GetAmount()}
	p.setAssetOrContract(&baseChange, clawback.GetAsset(), contractAddress)
	return []types.StateChange{baseChange}, nil
}

func (p *TokenTransferProcessor) handleFee(fee *ttp.Fee, contractAddress string, baseChange types.StateChange) ([]types.StateChange, error) {
	amount := fee.GetAmount()
	switch {
		// If a fee event is negative, it represents a refund. By default, a "fee" represents a debit but a negative fee represents a credit.
	case strings.HasPrefix(amount, "-"):
		baseChange.StateChangeCategory = types.StateChangeCategoryCredit
		// We are storing the fee as a credit refund so we dont need to store it as negative.
		baseChange.Amount = sql.NullString{String: strings.TrimPrefix(amount, "-")}
	default:
		baseChange.StateChangeCategory = types.StateChangeCategoryDebit
		baseChange.Amount = sql.NullString{String: amount}
	}
	baseChange.AccountID = fee.GetFrom()
	p.setAssetOrContract(&baseChange, fee.GetAsset(), contractAddress)
	return []types.StateChange{baseChange}, nil
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
