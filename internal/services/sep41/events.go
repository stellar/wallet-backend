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
	topics, err := eventTopics(event, EventTransfer, 3)
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

// ParseMintEvent decodes a SEP-41 mint ContractEvent. Two topic shapes are accepted:
//   - normalized (soroban-sdk 25.x+): [sym("mint"), to: Address]
//   - legacy (soroban-sdk <=24.x, Stellar Asset Contract, Aqua AMM, DeFindex): [sym("mint"), admin: Address, to: Address]
//
// `to` is always the last address topic. The admin topic in the legacy shape is
// not used downstream, but its type is still validated so we don't accept
// arbitrary 3-topic events that just happen to start with `mint` and end with
// an Address.
func ParseMintEvent(event xdr.ContractEvent) (*MintEvent, error) {
	topics, err := eventTopics(event, EventMint, 2, 3)
	if err != nil {
		return nil, err
	}
	if len(topics) == 3 {
		// Validate the legacy admin slot is an Address; discard the value.
		if _, err := extractAddressFromScVal(topics[1]); err != nil {
			return nil, fmt.Errorf("decoding mint.admin: %w", err)
		}
	}
	to, err := extractAddressFromScVal(topics[len(topics)-1])
	if err != nil {
		return nil, fmt.Errorf("decoding mint.to: %w", err)
	}
	amt, muxedID, err := extractAmountAndMuxedID(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding mint amount: %w", err)
	}
	return &MintEvent{To: to, Amount: amt, ToMuxedID: muxedID}, nil
}

// ParseBurnEvent decodes a SEP-41 burn event: [sym("burn"), from: Address],
// data = i128 or { amount: i128 }.
func ParseBurnEvent(event xdr.ContractEvent) (*BurnEvent, error) {
	topics, err := eventTopics(event, EventBurn, 2)
	if err != nil {
		return nil, err
	}
	from, err := extractAddressFromScVal(topics[1])
	if err != nil {
		return nil, fmt.Errorf("decoding burn.from: %w", err)
	}
	amt, err := extractAmount(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding burn amount: %w", err)
	}
	return &BurnEvent{From: from, Amount: amt}, nil
}

// ParseClawbackEvent decodes a SEP-41 clawback ContractEvent. Two topic shapes are accepted:
//   - 2-topic [sym("clawback"), from: Address]
//   - legacy 3-topic [sym("clawback"), admin: Address, from: Address] — emitted by the
//     Stellar Asset Contract reference and by tokens built against soroban-sdk <=24.x.
//
// `from` is always the last address topic. As with ParseMintEvent, the admin
// topic in the legacy shape is unused but still type-checked so a 3-topic
// `clawback` event with a non-Address middle topic is rejected.
func ParseClawbackEvent(event xdr.ContractEvent) (*ClawbackEvent, error) {
	topics, err := eventTopics(event, EventClawback, 2, 3)
	if err != nil {
		return nil, err
	}
	if len(topics) == 3 {
		if _, err := extractAddressFromScVal(topics[1]); err != nil {
			return nil, fmt.Errorf("decoding clawback.admin: %w", err)
		}
	}
	from, err := extractAddressFromScVal(topics[len(topics)-1])
	if err != nil {
		return nil, fmt.Errorf("decoding clawback.from: %w", err)
	}
	amt, err := extractAmount(event.Body.V0.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding clawback amount: %w", err)
	}
	return &ClawbackEvent{From: from, Amount: amt}, nil
}

// ParseApproveEvent decodes a SEP-41 approve event:
// topics: [sym("approve"), from: Address, spender: Address]
// data:   [i128 amount, u32 live_until_ledger] or
// { amount: i128, live_until_ledger: u32 }.
func ParseApproveEvent(event xdr.ContractEvent) (*ApproveEvent, error) {
	topics, err := eventTopics(event, EventApprove, 3)
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

	var amountVal, liveUntilVal xdr.ScVal
	data := event.Body.V0.Data
	switch data.Type {
	case xdr.ScValTypeScvVec:
		vec, ok := data.GetVec()
		if !ok || vec == nil || len(*vec) != 2 {
			return nil, fmt.Errorf("approve data must be a 2-element ScVec")
		}
		amountVal = (*vec)[0]
		liveUntilVal = (*vec)[1]
	case xdr.ScValTypeScvMap:
		fields, err := extractRequiredMapFields(data, "amount", "live_until_ledger")
		if err != nil {
			return nil, fmt.Errorf("decoding approve data: %w", err)
		}
		amountVal = fields["amount"]
		liveUntilVal = fields["live_until_ledger"]
	default:
		return nil, fmt.Errorf("approve data must be ScVec or ScMap, got %v", data.Type)
	}

	amt, err := extractI128(amountVal)
	if err != nil {
		return nil, fmt.Errorf("decoding approve amount: %w", err)
	}
	liveUntil, ok := liveUntilVal.GetU32()
	if !ok {
		return nil, fmt.Errorf("approve live_until_ledger must be u32, got %v", liveUntilVal.Type)
	}

	return &ApproveEvent{
		From:            from,
		Spender:         spender,
		Amount:          amt,
		LiveUntilLedger: uint32(liveUntil),
	}, nil
}

// eventTopics validates a contract event's shape and returns its topics slice when it
// starts with symName. wantTopics lists every acceptable topic count — mint and clawback
// pass both 2 and 3 because the legacy Soroban Token Interface emits the longer form
// ([sym, admin, addr]) while the normalized SEP-41 form ([sym, addr]) emits the shorter.
func eventTopics(event xdr.ContractEvent, symName string, wantTopics ...int) ([]xdr.ScVal, error) {
	if event.Type != xdr.ContractEventTypeContract {
		return nil, fmt.Errorf("event type must be Contract, got %v", event.Type)
	}
	if event.Body.V != 0 {
		return nil, fmt.Errorf("unsupported event body version %d", event.Body.V)
	}
	topics := event.Body.V0.Topics
	matched := false
	for _, want := range wantTopics {
		if len(topics) == want {
			matched = true
			break
		}
	}
	if !matched {
		return nil, fmt.Errorf("expected one of %v topics for %s, got %d", wantTopics, symName, len(topics))
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

// extractAmount decodes the two SEP-41 amount data formats used by burn and
// clawback events: a single i128 value or a Symbol-keyed map containing amount.
func extractAmount(val xdr.ScVal) (*big.Int, error) {
	switch val.Type {
	case xdr.ScValTypeScvI128:
		return extractI128(val)
	case xdr.ScValTypeScvMap:
		fields, err := extractRequiredMapFields(val, "amount")
		if err != nil {
			return nil, err
		}
		amt, err := extractI128(fields["amount"])
		if err != nil {
			return nil, fmt.Errorf("map amount: %w", err)
		}
		return amt, nil
	default:
		return nil, fmt.Errorf("amount must be i128 or ScMap, got %v", val.Type)
	}
}

// extractRequiredMapFields validates a SEP-41 map data value and returns the
// requested fields. SEP-41 permits additional fields, but requires every map
// key to be a Symbol.
func extractRequiredMapFields(val xdr.ScVal, requiredFields ...string) (map[string]xdr.ScVal, error) {
	if val.Type != xdr.ScValTypeScvMap {
		return nil, fmt.Errorf("event data must be ScMap, got %v", val.Type)
	}
	if val.Map == nil {
		return nil, fmt.Errorf("event data map was nil")
	}
	m, ok := val.GetMap()
	if !ok || m == nil {
		return nil, fmt.Errorf("event data map was nil")
	}

	required := make(map[string]struct{}, len(requiredFields))
	for _, field := range requiredFields {
		required[field] = struct{}{}
	}

	fields := make(map[string]xdr.ScVal, len(requiredFields))
	seen := make(map[string]struct{}, len(*m))
	for _, entry := range *m {
		keySym, ok := entry.Key.GetSym()
		if !ok {
			return nil, fmt.Errorf("event data map key must be Symbol, got %v", entry.Key.Type)
		}
		key := string(keySym)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("event data map contains duplicate key %q", key)
		}
		seen[key] = struct{}{}
		if _, ok := required[key]; ok {
			fields[key] = entry.Val
		}
	}

	for _, field := range requiredFields {
		if _, ok := fields[field]; !ok {
			return nil, fmt.Errorf("event data map missing %q", field)
		}
	}
	return fields, nil
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
//
// A muxed address (SC_ADDRESS_TYPE_MUXED_ACCOUNT) is reduced to its base account: the
// multiplexing id is off-chain routing metadata rather than account identity, so a
// balance or allowance belongs to the underlying G-account. This mirrors the classic
// ingestion path (MuxedAccount.ToAccountId()) and keeps every downstream key on the base
// account. When a token carries a muxed id it travels separately in the event's
// to_muxed_id data field (see extractAmountAndMuxedID), which is where it is surfaced for
// history.
func extractAddressFromScVal(val xdr.ScVal) (string, error) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", fmt.Errorf("invalid address")
	}
	if addr.Type == xdr.ScAddressTypeScAddressTypeMuxedAccount {
		muxed := addr.MustMuxedAccount()
		s, err := strkey.Encode(strkey.VersionByteAccountID, muxed.Ed25519[:])
		if err != nil {
			return "", fmt.Errorf("encoding muxed account base address: %w", err)
		}
		return s, nil
	}
	s, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("converting address to string: %w", err)
	}
	return s, nil
}
