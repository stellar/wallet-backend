// Package blend implements the BLEND v2 lending protocol's on-chain
// interface: matching candidate WASM signatures against the Blend Pool and
// Backstop contract interfaces (this file's caller, validator.go) and, in
// later tasks, decoding Blend contract events into state changes.
//
// scval.go collects the minimal xdr.ScVal decoding helpers the validator
// needs to turn a get_config() PoolConfig map into a blend_pools row. Every
// helper follows the SDK's Get*/ok convention — a wrong-typed or malformed
// value reports ok=false rather than panicking, so a caller can log-and-skip
// a single bad field without aborting the whole decode.
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
