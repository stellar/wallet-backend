// Package blend implements the BLEND v2 lending protocol's on-chain
// interface: matching candidate WASM signatures against the Blend Pool and
// Backstop contract interfaces (validator.go), decoding their ContractData
// ledger entries into typed payloads (entries.go), and, in later tasks,
// decoding Blend contract events into state changes.
//
// scval.go collects the xdr.ScVal decoding helpers validator.go and
// entries.go build on. Every helper follows the SDK's Get*/ok convention — a
// wrong-typed or malformed value reports ok=false rather than panicking, so a
// caller can log-and-skip a single bad field, or wrap the failure with
// context, without the helper itself aborting anything.
package blend

import (
	"math/big"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// mapGet returns the value keyed by the Symbol sym in an ScMap, and whether
// the key was present. Soroban SDK #[contracttype] structs with named fields
// serialize as an ScMap keyed by field-name Symbols, so this is the primary
// way to pull a single field out of a decoded UDT return value.
func mapGet(m *xdr.ScMap, sym string) (xdr.ScVal, bool) {
	if m == nil {
		return xdr.ScVal{}, false
	}
	for _, entry := range *m {
		key, ok := entry.Key.GetSym()
		if ok && string(key) == sym {
			return entry.Val, true
		}
	}
	return xdr.ScVal{}, false
}

// i128String decodes an ScVal holding an i128 into its base-10 string
// representation, preserving the full 128-bit value with no decimal scaling
// (Blend amounts are raw on-chain integers, not classic 7-decimal amounts).
func i128String(v xdr.ScVal) (string, bool) {
	parts, ok := v.GetI128()
	if !ok {
		return "", false
	}
	// value = Hi * 2^64 + Lo, where Hi is signed and Lo is unsigned.
	bi := big.NewInt(int64(parts.Hi))
	bi.Lsh(bi, 64)
	bi.Add(bi, new(big.Int).SetUint64(uint64(parts.Lo)))
	return bi.String(), true
}

// addrString decodes an ScVal holding an Address into its strkey-encoded
// string form (G... for accounts, C... for contracts).
func addrString(v xdr.ScVal) (string, bool) {
	addr, ok := v.GetAddress()
	if !ok {
		return "", false
	}
	s, err := addr.String()
	if err != nil {
		return "", false
	}
	return s, true
}

// u32Val decodes an ScVal holding a u32.
func u32Val(v xdr.ScVal) (uint32, bool) {
	u, ok := v.GetU32()
	if !ok {
		return 0, false
	}
	return uint32(u), true
}

// u64Val decodes an ScVal holding a u64.
func u64Val(v xdr.ScVal) (uint64, bool) {
	u, ok := v.GetU64()
	if !ok {
		return 0, false
	}
	return uint64(u), true
}

// boolVal decodes an ScVal holding a bool.
func boolVal(v xdr.ScVal) (bool, bool) {
	return v.GetB()
}

// stringVal decodes an ScVal holding a Soroban String (distinct from a
// Symbol) into a Go string.
func stringVal(v xdr.ScVal) (string, bool) {
	s, ok := v.GetStr()
	if !ok {
		return "", false
	}
	return string(s), true
}

// vecVal decodes an ScVal holding a Vec into its element slice.
func vecVal(v xdr.ScVal) ([]xdr.ScVal, bool) {
	vec, ok := v.GetVec()
	if !ok {
		return nil, false
	}
	if vec == nil {
		return []xdr.ScVal{}, true
	}
	return []xdr.ScVal(*vec), true
}

// mapU32I128 decodes an ScVal holding a Map<u32, i128> — the shape used by a
// Blend Positions entry's collateral/liabilities/supply fields, keyed by
// reserve_index — into a Go map from reserve index to the i128's base-10
// string. An empty on-chain map decodes to an empty (non-nil) Go map.
func mapU32I128(v xdr.ScVal) (map[uint32]string, bool) {
	m, ok := v.GetMap()
	if !ok {
		return nil, false
	}
	if m == nil {
		return map[uint32]string{}, true
	}
	out := make(map[uint32]string, len(*m))
	for _, entry := range *m {
		k, ok := u32Val(entry.Key)
		if !ok {
			return nil, false
		}
		val, ok := i128String(entry.Val)
		if !ok {
			return nil, false
		}
		out[k] = val
	}
	return out, true
}

// mapAddrI128 decodes an ScVal holding a Map<Address, i128> — the shape used
// by a Blend fill_auction event's AuctionData bid/lot fields, keyed by asset
// contract address — into a Go map from strkey address to the i128's base-10
// string. An empty on-chain map decodes to an empty (non-nil) Go map.
func mapAddrI128(v xdr.ScVal) (map[string]string, bool) {
	m, ok := v.GetMap()
	if !ok {
		return nil, false
	}
	if m == nil {
		return map[string]string{}, true
	}
	out := make(map[string]string, len(*m))
	for _, entry := range *m {
		k, ok := addrString(entry.Key)
		if !ok {
			return nil, false
		}
		val, ok := i128String(entry.Val)
		if !ok {
			return nil, false
		}
		out[k] = val
	}
	return out, true
}
