package contracts

import (
	"context"
	"fmt"

	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	setAuthorizedFunctionName              = "set_authorized"
	AuthorizedFlagName                     = "authorized_flag"
	AuthorizedToMaintainLiabilitesFlagName = "authorized_to_maintain_liabilites_flag"
	txMetaVersionV3                        = 3
	txMetaVersionV4                        = 4
)

type SACEventsProcessor struct {
	networkPassphrase string
}

func NewSACEventsProcessor(networkPassphrase string) *SACEventsProcessor {
	return &SACEventsProcessor{
		networkPassphrase: networkPassphrase,
	}
}

func (p *SACEventsProcessor) Name() string {
	return "sac"
}

// ProcessOperation processes contract events and converts them into state changes.
func (p *SACEventsProcessor) ProcessOperation(_ context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	if opWrapper.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return nil, processors.ErrInvalidOpType
	}

	ledgerCloseTime := opWrapper.Transaction.Ledger.LedgerCloseTime()
	ledgerNumber := opWrapper.Transaction.Ledger.LedgerSequence()
	txHash := opWrapper.Transaction.Result.TransactionHash.HexString()
	txID := opWrapper.Transaction.ID()

	tx := opWrapper.Transaction
	contractEvents, err := tx.GetContractEventsForOperation(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting contract events for operation %d: %w", opWrapper.ID(), err)
	}

	stateChanges := make([]types.StateChange, 0)
	builder := processors.NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, txID).WithOperationID(opWrapper.ID())
	for _, event := range contractEvents {
		// Validate basic contract contractEvent structure
		if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 {
			continue
		}

		// Validate if number of topics matches the expected number of topics for an SAC set_authorized event
		topics := event.Body.V0.Topics
		if !p.validateExpectedTopicsForSAC(len(topics), tx.UnsafeMeta.V) {
			continue
		}

		fn, ok := topics[0].GetSym()
		if !ok {
			continue
		}

		switch string(fn) {
		case setAuthorizedFunctionName:
			accountToAuthorize, asset, err := p.extractAccountAndAsset(topics, tx.UnsafeMeta.V)
			if err != nil {
				continue
			}

			// Since SAC contract IDs are deterministic, we can use the contract ID from the event to validate the SEP11 asset string
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			if !isSAC(contractID, asset, p.networkPassphrase) {
				continue
			}

			value := event.Body.V0.Data
			isAuthorized, ok := value.GetB()
			if !ok {
				continue
			}

			/*
				According to the SAC contract spec: https://github.com/stellar/stellar-protocol/blob/master/core/cap-0046-06.md#set_authorized
				- If the set_authorized function is called with authorize == true on an Account, it will set the accounts trustline's flag to AUTHORIZED_FLAG and
				unset AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG if the trustline currently does not have AUTHORIZED_FLAG set.

				- If the set_authorized function is called with authorize == false on an Account, it will set the accounts trustline's flag to AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG
				and unset AUTHORIZED_FLAG if the trustline currently has AUTHORIZED_FLAG set. We set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG instead of clearing the flags to avoid pulling
				all offers and pool shares that involve this trustline.

				We will generate 2 balance authorization state changes: one to set the AUTHORIZED_FLAG and one to remove the AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG and vice versa for the case where isAuthorized is false.
			*/
			scBuilder := builder.WithCategory(types.StateChangeCategoryBalanceAuthorization).WithAccount(accountToAuthorize).WithToken(contractID)
			var changes []types.StateChange
			if isAuthorized {
				changes = []types.StateChange{
					scBuilder.Clone().
						WithReason(types.StateChangeReasonSet).
						WithFlags([]string{AuthorizedFlagName}).
						Build(),
					scBuilder.Clone().
						WithReason(types.StateChangeReasonRemove).
						WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
						Build(),
				}
			} else {
				changes = []types.StateChange{
					scBuilder.Clone().
						WithReason(types.StateChangeReasonRemove).
						WithFlags([]string{AuthorizedFlagName}).
						Build(),
					scBuilder.Clone().
						WithReason(types.StateChangeReasonSet).
						WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
						Build(),
				}
			}

			stateChanges = append(stateChanges, changes...)
		default:
			continue
		}
	}
	return stateChanges, nil
}

// validateExpectedTopicsForSAC validates the expected number of topics for a set_authorized event
func (p *SACEventsProcessor) validateExpectedTopicsForSAC(numTopics int, txMetaVersion int32) bool {
	/*
		For meta V3, a set_authorized event will have 4 topics: ["set_authorized", admin: Address, id: Address, sep0011_asset: String]
		For meta V4, a set_authorized event will have 3 topics: ["set_authorized", id: Address, sep0011_asset: String]
	*/
	switch txMetaVersion {
	case txMetaVersionV3:
		return numTopics == 4
	case txMetaVersionV4:
		return numTopics == 3
	default:
		return false
	}
}

// extractAccountAndAsset extracts the account and asset from the topics
func (p *SACEventsProcessor) extractAccountAndAsset(topics []xdr.ScVal, txMetaVersion int32) (string, xdr.Asset, error) {
	var accountIdx, assetIdx int
	switch txMetaVersion {
	case txMetaVersionV3:
		accountIdx = 2
		assetIdx = 3
	case txMetaVersionV4:
		accountIdx = 1
		assetIdx = 2
	}
	account, err := extractAddress(topics[accountIdx])
	if err != nil {
		return "", xdr.Asset{}, fmt.Errorf("invalid account: %w", err)
	}
	asset, err := extractAsset(topics[assetIdx])
	if err != nil {
		return "", xdr.Asset{}, fmt.Errorf("invalid asset: %w", err)
	}
	return account, asset, nil
}
