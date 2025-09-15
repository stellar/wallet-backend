package contracts

import (
	"context"
	"fmt"

	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/ingest"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	setAuthorizedFunctionName              = "set_authorized"
	AuthorizedFlagName                     = "authorized"
	AuthorizedToMaintainLiabilitesFlagName = "authorized_to_maintain_liabilites"
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

	// Get operation changes to access previous trustline flag state
	changes, err := tx.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes for operation %d: %w", opWrapper.ID(), err)
	}

	stateChanges := make([]types.StateChange, 0)
	builder := processors.NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, txID).WithOperationID(opWrapper.ID())
	for _, event := range contractEvents {
		// Validate basic contract contractEvent structure
		if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 {
			log.Debugf("processor: %s: skipping event with invalid contract structure: txHash=%s opID=%d eventType=%d contractId=%v bodyV=%d",
				p.Name(), txHash, opWrapper.ID(), event.Type, event.ContractId != nil, event.Body.V)
			continue
		}

		// Validate if number of topics matches the expected number of topics for an SAC set_authorized event
		topics := event.Body.V0.Topics
		if !p.validateExpectedTopicsForSAC(len(topics), tx.UnsafeMeta.V) {
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			log.Debugf("processor: %s: skipping event with invalid topic count for SAC: txHash=%s opID=%d contractId=%s topicCount=%d metaVersion=%d",
				p.Name(), txHash, opWrapper.ID(), contractID, len(topics), tx.UnsafeMeta.V)
			continue
		}

		fn, ok := topics[0].GetSym()
		if !ok {
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			log.Debugf("processor: %s: skipping event with non-symbol function name: txHash=%s opID=%d contractId=%s",
				p.Name(), txHash, opWrapper.ID(), contractID)
			continue
		}

		switch string(fn) {
		case setAuthorizedFunctionName:
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			isSAC, err := p.isSACContract(topics, contractID, tx.UnsafeMeta.V)
			if err != nil {
				log.Debugf("processor: %s: skipping event due to SAC contract validation failure: txHash=%s opID=%d contractId=%s error=%v",
					p.Name(), txHash, opWrapper.ID(), contractID, err)
				continue
			}
			if !isSAC {
				log.Debugf("processor: %s: skipping event due to non-SAC contract: txHash=%s opID=%d contractId=%s",
					p.Name(), txHash, opWrapper.ID(), contractID)
				continue
			}

			accountToAuthorize, err := p.extractAccount(topics, tx.UnsafeMeta.V)
			if err != nil {
				log.Debugf("processor: %s: skipping event due to account extraction failure: txHash=%s opID=%d contractId=%s error=%v",
					p.Name(), txHash, opWrapper.ID(), contractID, err)
				continue
			}

			value := event.Body.V0.Data
			isAuthorized, ok := value.GetB()
			if !ok {
				log.Debugf("processor: %s: skipping event with non-boolean authorization value: txHash=%s opID=%d contractId=%s",
					p.Name(), txHash, opWrapper.ID(), contractID)
				continue
			}

			// Extract previous trustline flag state to determine actual changes
			wasAuthorized, wasMaintainLiabilities, err := p.extractTrustlineFlagChanges(changes, accountToAuthorize)
			if err != nil {
				log.Debugf("processor: %s: skipping event due to trustline flag extraction failure: txHash=%s opID=%d contractId=%s error=%v",
					p.Name(), txHash, opWrapper.ID(), contractID, err)
				continue
			}

			scBuilder := builder.WithCategory(types.StateChangeCategoryBalanceAuthorization).WithAccount(accountToAuthorize).WithToken(contractID)
			var flagChanges []types.StateChange

			/*
				Generate state changes based on actual flag transitions:
				- If authorize == true: should set AUTHORIZED_FLAG and potentially clear AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG
				- If authorize == false: should clear AUTHORIZED_FLAG and potentially set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG

				Only generate state changes for flags that actually changed from their previous state.
			*/
			if isAuthorized {
				// Authorizing: should set AUTHORIZED_FLAG if it wasn't already set
				if !wasAuthorized {
					flagChanges = append(flagChanges, scBuilder.Clone().
						WithReason(types.StateChangeReasonSet).
						WithFlags([]string{AuthorizedFlagName}).
						Build())
				}
				// Should clear AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG if it was previously set
				if wasMaintainLiabilities {
					flagChanges = append(flagChanges, scBuilder.Clone().
						WithReason(types.StateChangeReasonRemove).
						WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
						Build())
				}
			} else {
				// Deauthorizing: should clear AUTHORIZED_FLAG if it was previously set
				if wasAuthorized {
					flagChanges = append(flagChanges, scBuilder.Clone().
						WithReason(types.StateChangeReasonRemove).
						WithFlags([]string{AuthorizedFlagName}).
						Build())
				}
				// Should set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG if it wasn't already set
				if !wasMaintainLiabilities {
					flagChanges = append(flagChanges, scBuilder.Clone().
						WithReason(types.StateChangeReasonSet).
						WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
						Build())
				}
			}

			stateChanges = append(stateChanges, flagChanges...)
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
func (p *SACEventsProcessor) extractAccount(topics []xdr.ScVal, txMetaVersion int32) (string, error) {
	var accountIdx int
	switch txMetaVersion {
	case txMetaVersionV3:
		accountIdx = 2
	case txMetaVersionV4:
		accountIdx = 1
	}
	account, err := extractAddressFromScVal(topics[accountIdx])
	if err != nil {
		return "", fmt.Errorf("invalid account: %w", err)
	}
	return account, nil
}

// isSACContract checks if a contract is an SAC contract
func (p *SACEventsProcessor) isSACContract(topics []xdr.ScVal, contractID string, txMetaVersion int32) (bool, error) {
	var assetIdx int
	switch txMetaVersion {
	case txMetaVersionV3:
		assetIdx = 3
	case txMetaVersionV4:
		assetIdx = 2
	}
	asset, err := extractAssetFromScVal(topics[assetIdx])
	if err != nil {
		return false, fmt.Errorf("invalid asset: %w", err)
	}
	assetContractID, err := asset.ContractID(p.networkPassphrase)
	if err != nil {
		return false, fmt.Errorf("invalid asset contract ID: %w", err)
	}
	return strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]) == contractID, nil
}

// extractTrustlineFlagChanges extracts the previous trustline flags for the given account
// from the operation changes to determine what flags were set before the SAC operation
func (p *SACEventsProcessor) extractTrustlineFlagChanges(changes []ingest.Change, accountToAuthorize string) (wasAuthorized bool, wasMaintainLiabilities bool, err error) {
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		if change.Pre == nil {
			continue
		}

		prevTrustline := change.Pre.Data.MustTrustLine()
		trustlineAccount := prevTrustline.AccountId.Address()

		if trustlineAccount == accountToAuthorize {
			prevFlags := uint32(prevTrustline.Flags)
			wasAuthorized = (prevFlags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
			wasMaintainLiabilities = (prevFlags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
			return wasAuthorized, wasMaintainLiabilities, nil
		}
	}

	return false, false, fmt.Errorf("trustline change not found for account %s", accountToAuthorize)
}
