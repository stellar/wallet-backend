package sep41

import (
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// SEP-41 event topic symbols (topics[0]).
const (
	EventTransfer = "transfer"
	EventMint     = "mint"
	EventBurn     = "burn"
	EventClawback = "clawback"
	EventApprove  = "approve"
)

// TransferEvent represents a decoded SEP-41 transfer event.
// CAP-67 token-memo variants carry a destination memo via ToMuxedID; for plain transfers it is nil.
type TransferEvent struct {
	From      string
	To        string
	Amount    *big.Int
	ToMuxedID *uint64
}

// MintEvent represents a decoded SEP-41 mint event.
type MintEvent struct {
	To        string
	Amount    *big.Int
	ToMuxedID *uint64
}

// BurnEvent represents a decoded SEP-41 burn event.
type BurnEvent struct {
	From   string
	Amount *big.Int
}

// ClawbackEvent represents a decoded SEP-41 clawback event.
type ClawbackEvent struct {
	From   string
	Amount *big.Int
}

// ApproveEvent represents a decoded SEP-41 approve event.
// LiveUntilLedger is the expiration_ledger advertised by the contract.
type ApproveEvent struct {
	From            string
	Spender         string
	Amount          *big.Int
	LiveUntilLedger uint32
}

// ContractIDString returns the strkey-encoded C... address for an event's ContractId, if set.
func ContractIDString(event xdr.ContractEvent) (string, error) {
	if event.ContractId == nil {
		return "", fmt.Errorf("contract event missing contract id")
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract id to strkey: %w", err)
	}
	return addr, nil
}

// ParseTransferEvent decodes a SEP-41 transfer ContractEvent.
// Topics: [sym("transfer"), from: Address, to: Address] — data = i128 amount (classic)
// or map { amount: i128, to_muxed_id: u64 } (CAP-67).
func ParseTransferEvent(event xdr.ContractEvent) (*TransferEvent, error) {
	topics, err := eventTopics(event, 3, EventTransfer)
	if err != nil {
		return nil, err
	}
	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding transfer.from: %w", err)
	}
	to, err := extractAddressFromScVal(topics[2])
	if err != nil {
		return nil, fmt.Errorf("decoding transfer.to: %w", err)
	}

	amt, muxedID, err := extractAmountAndMuxedID(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding transfer amount: %w", err)
	}
	return &TransferEvent{From: from, To: to, Amount: amt, ToMuxedID: muxedID}, nil
}

// ParseMintEvent decodes a SEP-41 mint event: [sym("mint"), to: Address] with i128 or map data.
func ParseMintEvent(event xdr.ContractEvent) (*MintEvent, error) {
	topics, err := eventTopics(event, 2, EventMint)
	if err != nil {
		return nil, err
	}
	to, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding mint.to: %w", err)
	}
	amt, muxedID, err := extractAmountAndMuxedID(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding mint amount: %w", err)
	}
	return &MintEvent{To: to, Amount: amt, ToMuxedID: muxedID}, nil
}

// ParseBurnEvent decodes a SEP-41 burn event: [sym("burn"), from: Address], data = i128.
func ParseBurnEvent(event xdr.ContractEvent) (*BurnEvent, error) {
	topics, err := eventTopics(event, 2, EventBurn)
	if err != nil {
		return nil, err
	}
	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding burn.from: %w", err)
	}
	amt, err := extractI128(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding burn amount: %w", err)
	}
	return &BurnEvent{From: from, Amount: amt}, nil
}

// ParseClawbackEvent decodes a SEP-41 clawback event: [sym("clawback"), from: Address], data = i128.
func ParseClawbackEvent(event xdr.ContractEvent) (*ClawbackEvent, error) {
	topics, err := eventTopics(event, 2, EventClawback)
	if err != nil {
		return nil, err
	}
	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding clawback.from: %w", err)
	}
	amt, err := extractI128(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding clawback amount: %w", err)
	}
	return &ClawbackEvent{From: from, Amount: amt}, nil
}

// ParseApproveEvent decodes a SEP-41 approve event:
// topics: [sym("approve"), from: Address, spender: Address]
// data:   [ i128 amount, u32 live_until_ledger ]  (ScVec of 2 elements)
func ParseApproveEvent(event xdr.ContractEvent) (*ApproveEvent, error) {
	topics, err := eventTopics(event, 3, EventApprove)
	if err != nil {
		return nil, err
	}
	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding approve.from: %w", err)
	}
	spender, err := extractAddressFromScVal(topics[2])
	if err != nil {
		return nil, fmt.Errorf("decoding approve.spender: %w", err)
	}

	data := event.Body.V0.Data
	if data.Type != xdr.ScValTypeScvVec {
		return nil, fmt.Errorf("approve data must be ScVec, got %v", data.Type)
	}
	vec, ok := data.GetVec()
	if !ok || vec == nil || len(*vec) != 2 {
		return nil, fmt.Errorf("approve data must be a 2-element ScVec")
	}

	amt, err := extractI128((*vec)[0])
	if err != nil {
		return nil, fmt.Errorf("decoding approve amount: %w", err)
	}
	liveUntil, ok := (*vec)[1].GetU32()
	if !ok {
		return nil, fmt.Errorf("approve live_until_ledger must be u32, got %v", (*vec)[1].Type)
	}

	return &ApproveEvent{
		From:            from,
		Spender:         spender,
		Amount:          amt,
		LiveUntilLedger: uint32(liveUntil),
	}, nil
}

// eventTopics validates a contract event's shape and returns its topics slice when it starts with symName.
func eventTopics(event xdr.ContractEvent, wantTopics int, symName string) ([]xdr.ScVal, error) {
	if event.Type != xdr.ContractEventTypeContract {
		return nil, fmt.Errorf("event type must be Contract, got %v", event.Type)
	}
	if event.Body.V != 0 {
		return nil, fmt.Errorf("unsupported event body version %d", event.Body.V)
	}
	topics := event.Body.V0.Topics
	if len(topics) != wantTopics {
		return nil, fmt.Errorf("expected %d topics for %s, got %d", wantTopics, symName, len(topics))
	}
	sym, ok := topics[0].GetSym()
	if !ok || string(sym) != symName {
		return nil, fmt.Errorf("topic[0] must be symbol %q", symName)
	}
	return topics, nil
}

// extractI128 converts an i128 ScVal into a *big.Int representing the raw 128-bit value.
// Unlike amount.String128, this preserves the raw integer (no 10^7 divisor), which is
// what SEP-41 events carry.
func extractI128(val xdr.ScVal) (*big.Int, error) {
	if val.Type != xdr.ScValTypeScvI128 {
		return nil, fmt.Errorf("expected i128, got %v", val.Type)
	}
	parts := val.MustI128()
	// value = Hi * 2^64 + Lo, where Hi is signed and Lo is unsigned.
	bi := big.NewInt(int64(parts.Hi))
	bi.Lsh(bi, 64)
	bi.Add(bi, new(big.Int).SetUint64(uint64(parts.Lo)))
	return bi, nil
}

// extractAmountAndMuxedID decodes either a raw i128 (classic transfer/mint) or the
// CAP-67 map form { amount: i128, to_muxed_id: <u64|string|bytes> } into an amount
// plus an optional u64 memo id. Map keys are Symbol per CAP-67.
func extractAmountAndMuxedID(val xdr.ScVal) (*big.Int, *uint64, error) {
	switch val.Type {
	case xdr.ScValTypeScvI128:
		amt, err := extractI128(val)
		return amt, nil, err
	case xdr.ScValTypeScvMap:
		m, ok := val.GetMap()
		if !ok || m == nil {
			return nil, nil, fmt.Errorf("amount map was nil")
		}
		var (
			amt     *big.Int
			muxedID *uint64
		)
		for _, entry := range *m {
			keySym, ok := entry.Key.GetSym()
			if !ok {
				continue
			}
			switch string(keySym) {
			case "amount":
				a, err := extractI128(entry.Val)
				if err != nil {
					return nil, nil, fmt.Errorf("map amount: %w", err)
				}
				amt = a
			case "to_muxed_id":
				if id, ok := entry.Val.GetU64(); ok {
					v := uint64(id)
					muxedID = &v
				}
			}
		}
		if amt == nil {
			return nil, nil, fmt.Errorf("map missing amount key")
		}
		return amt, muxedID, nil
	default:
		return nil, nil, fmt.Errorf("amount must be i128 or Map, got %v", val.Type)
	}
}

// extractAddressFromScVal decodes an address ScVal to its strkey-encoded form.
func extractAddressFromScVal(val xdr.ScVal) (string, error) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", fmt.Errorf("invalid address")
	}
	s, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("converting address to string: %w", err)
	}
	return s, nil
}
