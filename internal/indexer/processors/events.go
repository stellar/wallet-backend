package processors

import (
	"context"
	"errors"
	"fmt"

	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	setAuthorizedFunctionName = "set_authorized"
	authorizedKeyName       = "authorized"
	AuthorizedFlagName = "authorized_flag"
	AuthorizedToMaintainLiabilitesFlagName = "authorized_to_maintain_liabilites_flag"
	txMetaVersionV3     = 3
	txMetaVersionV4     = 4
)

type EventsProcessor struct {
	networkPassphrase string
}

func NewEventsProcessor(networkPassphrase string) *EventsProcessor {
	return &EventsProcessor{
		networkPassphrase: networkPassphrase,
	}
}

// ProcessOperation processes contract events and converts them into state changes.
func (p *EventsProcessor) ProcessOperation(_ context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	if opWrapper.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return nil, ErrInvalidOpType
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
	builder := NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, txID).WithOperationID(opWrapper.ID())
	for _, event := range contractEvents {
		// Validate basic contract contractEvent structure
		if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 {
			continue
		}

		// A set_authorized event needs to have 4 topics: ["set_authorized", admin: Address, id: Address, sep0011_asset: String]
		topics := event.Body.V0.Topics
		value := event.Body.V0.Data
		if len(topics) < 4 {
			continue
		}

		fn, ok := topics[0].GetSym()
		if !ok {
			continue
		}

		switch string(fn) {
		case setAuthorizedFunctionName:
			contractAddress := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			accountToAuthorize, err := extractAddress(topics[2])
			if err != nil {
				return nil, fmt.Errorf("invalid fromAddress error: %w", err)
			}

			isAuthorized, err := extractAuthorizedData(value, tx.UnsafeMeta.V)
			if err != nil {
				return nil, fmt.Errorf("invalid event data: %w", err)
			}

			scBuilder := builder.WithCategory(types.StateChangeCategoryBalanceAuthorization).WithAccount(accountToAuthorize).WithToken(contractAddress)
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

func parseV4MapDataForTokenEvents(mapData xdr.ScMap) (bool, error) {
	for _, entry := range mapData {
		key, ok := entry.Key.GetSym()
		if !ok {
			return false, fmt.Errorf("invalid key type in data map: %s", entry.Key.Type)
		}
		if string(key) == authorizedKeyName {
			isAuthorized, ok := entry.Val.GetB()
			if !ok {
				return false, fmt.Errorf("amt field is not i128")
			}
			return isAuthorized, nil
		}
	}
	return false, fmt.Errorf("amount field not found in data map")
}

func extractAuthorizedData(value xdr.ScVal, txMetaVersion int32) (bool, error) {
	var ok, isAuthorized bool
	switch txMetaVersion {
	case txMetaVersionV3:
		isAuthorized, ok = value.GetB()
		if !ok {
			return false, fmt.Errorf("invalid event amount")
		}
	case txMetaVersionV4:
		// V4 data format can be:
		// 1. Direct i128 (when there's no memo)
		// 2. ScMap with exactly 2 fields: "amount" (i128) + "to_muxed_id" (u64/bytes/string)
		// If it's a map, at the very least "amount" should be present.
		if mapData, ok := value.GetMap(); ok {
			if mapData == nil {
				return false, fmt.Errorf("map is empty")
			}
			var err error
			isAuthorized, err = parseV4MapDataForTokenEvents(*mapData)
			if err != nil {
				return false, fmt.Errorf("failed to parse V4 map data: %w", err)
			}
		} else {
			// Fall back to direct i128 parsing (V4 without to_muxed_id)
			var ok bool
			isAuthorized, ok = value.GetB()
			if !ok {
				return false, fmt.Errorf("invalid event amount")
			}
		}
	}
	return isAuthorized, nil
}

// extractAddress helps extract a string representation of an address from a ScVal
func extractAddress(val xdr.ScVal) (string, error) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", errors.New("invalid address")
	}
	addrStr, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("failed to convert address to string: %w", err)
	}
	return addrStr, nil
}