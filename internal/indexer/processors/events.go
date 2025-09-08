package processors

import (
	"context"
	"errors"
	"fmt"

	amount "github.com/stellar/go/amount"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	approveFunctionName = "approve"
	amountKeyName       = "amount"
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
			return nil, fmt.Errorf("invalid contractEvent")
		}

		topics := event.Body.V0.Topics
		value := event.Body.V0.Data
		if len(topics) < 2 {
			return nil, fmt.Errorf("insufficient topics in contractEvent")
		}

		fn, ok := topics[0].GetSym()
		if !ok {
			return nil, fmt.Errorf("invalid function name")
		}

		contractAddress := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])

		amt, err := extractAmount(value, tx.UnsafeMeta.V)
		if err != nil {
			return nil, fmt.Errorf("invalid event amount: %w", err)
		}
		amountStr := amount.String128Raw(amt)

		switch string(fn) {
		case approveFunctionName:
			if len(topics) < 3 {
				return nil, fmt.Errorf("insufficient topics for an `approve` event")
			}

			from, err := extractAddress(topics[1])
			if err != nil {
				return nil, fmt.Errorf("invalid fromAddress error: %w", err)
			}

			stateChanges = append(stateChanges, builder.WithCategory(types.StateChangeCategoryAllowance).
				WithReason(types.StateChangeReasonSet).
				WithAccount(from).
				WithAmount(amountStr).
				WithToken(contractAddress).
				Build())

		default:
			continue
		}
	}
	return stateChanges, nil
}

func parseV4MapDataForTokenEvents(mapData xdr.ScMap) (xdr.Int128Parts, error) {
	for _, entry := range mapData {
		key, ok := entry.Key.GetSym()
		if !ok {
			return xdr.Int128Parts{}, fmt.Errorf("invalid key type in data map: %s", entry.Key.Type)
		}
		if string(key) == amountKeyName {
			amt, ok := entry.Val.GetI128()
			if !ok {
				return xdr.Int128Parts{}, fmt.Errorf("amt field is not i128")
			}
			return amt, nil
		}
	}
	return xdr.Int128Parts{}, fmt.Errorf("amount field not found in data map")
}

func extractAmount(value xdr.ScVal, txMetaVersion int32) (xdr.Int128Parts, error) {
	var amt xdr.Int128Parts
	var ok bool
	switch txMetaVersion {
	case txMetaVersionV3:
		amt, ok = value.GetI128()
		if !ok {
			return xdr.Int128Parts{}, fmt.Errorf("invalid event amount")
		}
	case txMetaVersionV4:
		// V4 data format can be:
		// 1. Direct i128 (when there's no memo)
		// 2. ScMap with exactly 2 fields: "amount" (i128) + "to_muxed_id" (u64/bytes/string)
		// If it's a map, at the very least "amount" should be present.
		if mapData, ok := value.GetMap(); ok {
			if mapData == nil {
				return xdr.Int128Parts{}, fmt.Errorf("map is empty")
			}
			var err error
			amt, err = parseV4MapDataForTokenEvents(*mapData)
			if err != nil {
				return xdr.Int128Parts{}, fmt.Errorf("failed to parse V4 map data: %w", err)
			}
		} else {
			// Fall back to direct i128 parsing (V4 without to_muxed_id)
			var ok bool
			amt, ok = value.GetI128()
			if !ok {
				return xdr.Int128Parts{}, fmt.Errorf("invalid event amount")
			}
		}
	}
	return amt, nil
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
