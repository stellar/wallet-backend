package contracts

import (
	"bytes"
	"fmt"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	approveFunctionName = "approve"
	// A SAC approve event carries [approve, from, spender, sep0011_asset].
	sacApproveTopicCount = 4
)

// SACApproveStateChanges builds one allowance state change per approve event a
// Stellar Asset Contract emitted, in the shape the SEP-41 processor uses for
// custom tokens. Events from other contracts and malformed events are skipped.
// Ingestion does not call this.
func SACApproveStateChanges(networkPassphrase string, events []xdr.ContractEvent, builder processors.StateChangeBuilder) []types.StateChange {
	var stateChanges []types.StateChange
	for _, event := range events {
		approve, err := parseSACApproveEvent(networkPassphrase, event)
		if err != nil {
			continue
		}
		stateChanges = append(stateChanges, builder.
			WithCategory(types.StateChangeCategoryAllowance).
			WithReason(types.StateChangeReasonUpdate).
			WithToken(approve.contractID).
			WithAccount(approve.from).
			WithSpender(approve.spender).
			WithAmount(approve.amount).
			WithKeyValue(map[string]any{"live_until_ledger": approve.liveUntilLedger}).
			Build())
	}
	return stateChanges
}

type sacApproveEvent struct {
	contractID      string
	from            string
	spender         string
	amount          string
	liveUntilLedger uint32
}

// parseSACApproveEvent decodes an approve event and checks that the emitting
// contract is the Stellar Asset Contract of the asset named in the event, so
// another contract cannot pass its own approve event off as a SAC's.
func parseSACApproveEvent(networkPassphrase string, event xdr.ContractEvent) (*sacApproveEvent, error) {
	if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 || event.Body.V0 == nil {
		return nil, fmt.Errorf("unsupported event shape")
	}
	topics := event.Body.V0.Topics
	if len(topics) != sacApproveTopicCount {
		return nil, fmt.Errorf("expected %d topics, got %d", sacApproveTopicCount, len(topics))
	}
	if fn, ok := topics[0].GetSym(); !ok || string(fn) != approveFunctionName {
		return nil, fmt.Errorf("not an approve event")
	}

	asset, err := extractAssetFromScVal(topics[3])
	if err != nil {
		return nil, fmt.Errorf("decoding asset: %w", err)
	}
	assetContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("deriving asset contract ID: %w", err)
	}
	if !bytes.Equal(assetContractID[:], event.ContractId[:]) {
		return nil, fmt.Errorf("emitting contract is not the asset's contract")
	}

	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding from: %w", err)
	}
	spender, err := extractAddressFromScVal(topics[2])
	if err != nil {
		return nil, fmt.Errorf("decoding spender: %w", err)
	}

	data, ok := event.Body.V0.Data.GetVec()
	if !ok || data == nil || len(*data) != 2 {
		return nil, fmt.Errorf("data must be a 2-element vector")
	}
	amountParts, ok := (*data)[0].GetI128()
	if !ok {
		return nil, fmt.Errorf("amount must be i128")
	}
	liveUntilLedger, ok := (*data)[1].GetU32()
	if !ok {
		return nil, fmt.Errorf("live_until_ledger must be u32")
	}

	return &sacApproveEvent{
		contractID:      strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:]),
		from:            from,
		spender:         spender,
		amount:          amount.String128Raw(amountParts),
		liveUntilLedger: uint32(liveUntilLedger),
	}, nil
}
