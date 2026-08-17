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
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/strkey"
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
	return i128ToBigInt(parts).String(), true
}

// i128ToBigInt converts an XDR Int128Parts — Hi the signed high 64 bits, Lo
// the unsigned low 64 bits — into the equivalent signed big.Int:
// value = Hi*2^64 + Lo. Unlike i128String, this returns the big.Int itself
// so a caller (decodePriceData) can do further arithmetic on the value
// rather than just display it.
func i128ToBigInt(parts xdr.Int128Parts) *big.Int {
	bi := big.NewInt(int64(parts.Hi))
	bi.Lsh(bi, 64)
	bi.Add(bi, new(big.Int).SetUint64(uint64(parts.Lo)))
	return bi
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

// contractIDLen is the raw byte length of a strkey-decoded contract address
// (a 32-byte sha256 hash), matching xdr.ContractId's array size.
const contractIDLen = 32

// contractAddressScVal builds the bare ScVal encoding of a contract C-address
// as a Soroban `Address` value — the shape any `token: Address` (or similar)
// function parameter expects, e.g. Comet's get_balance(token)/
// get_normalized_weight(token) (see comet.go). buildSep40StellarAsset wraps
// this same encoding inside a 2-element ScVec for the SEP-40
// Asset::Stellar(Address) enum payload.
func contractAddressScVal(contractAddress string) (xdr.ScVal, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("blend: decoding contract address %q: %w", contractAddress, err)
	}
	if len(raw) != contractIDLen {
		return xdr.ScVal{}, fmt.Errorf("blend: contract address %q decoded to %d bytes, want %d", contractAddress, len(raw), contractIDLen)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &cid,
		},
	}, nil
}

// buildSep40StellarAsset builds the SEP-40 `Asset::Stellar(Address)`
// argument ScVal for a lastprice(asset) call against a SEP-40-compatible
// oracle (e.g. Reflector), given the strkey C-address of the priced Stellar
// Asset Contract. A Soroban SDK #[contracttype] tuple-variant enum
// serializes as a 2-element ScVec — the variant's Symbol name followed by
// its single payload value — the same convention decodeVecKeyEntry already
// relies on for Blend's own persistent storage keys.
//
// Verified against SEP-40 (stellar-protocol ecosystem/sep-0040.md):
// `enum Asset { Stellar(Address), Other(Symbol) }`,
// `fn lastprice(asset: Asset) -> Option<PriceData>`. Live-confirmed by
// dumping `stellar contract info interface` against the deployed Reflector
// external-CEX/DEX oracle (mainnet
// CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN), which shows
// the identical Rust shapes, and by simulating
// lastprice(Asset::Stellar(<XLM SAC>)) against the Reflector Stellar-DEX
// oracle (mainnet CALI2BYU2JE6WVRUFYTS6MSBNEHGJ35P4AVCZYF3B6QOE3QKOB2PLE6M),
// which round-tripped to a real Some(PriceData{...}) response. See
// TestBuildSep40StellarAsset for the full verification note.
func buildSep40StellarAsset(contractAddress string) (xdr.ScVal, error) {
	addrVal, err := contractAddressScVal(contractAddress)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("blend: building SEP-40 Asset::Stellar(%q): %w", contractAddress, err)
	}

	sym := xdr.ScSymbol("Stellar")
	vec := &xdr.ScVec{
		{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
		addrVal,
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}, nil
}

// PriceData is the decoded form of a SEP-40 `PriceData` struct: the raw
// on-chain i128 price (scaled by the oracle's own decimals(), which the
// caller must fetch separately — this package does no rescaling) and the
// Unix-seconds timestamp the price was recorded at.
type PriceData struct {
	Price     *big.Int
	Timestamp uint64
}

// decodePriceData decodes a SEP-40 lastprice(asset) return value — an
// Option<PriceData> — into a *PriceData. ScvVoid (Rust's None, meaning the
// oracle has no price for the asset) decodes to (nil, nil), the same
// no-error/no-value convention DecodeEntry's optional fields use elsewhere
// in this package. A populated PriceData is a #[contracttype] struct, so it
// serializes as an ScMap keyed by field-name Symbols (see mapGet). Any other
// top-level shape, or a present-but-malformed/missing field, is reported as
// an error rather than silently treated as "no price" — a caller must not
// mistake a decode failure for a legitimate absent price.
func decodePriceData(val xdr.ScVal) (*PriceData, error) {
	if val.Type == xdr.ScValTypeScvVoid {
		return nil, nil
	}

	m, ok := val.GetMap()
	if !ok {
		return nil, fmt.Errorf("blend: PriceData: expected ScMap or ScvVoid, got %v", val.Type)
	}

	priceVal, ok := mapGet(m, "price")
	if !ok {
		return nil, fmt.Errorf("blend: PriceData: missing %q field", "price")
	}
	parts, ok := priceVal.GetI128()
	if !ok {
		return nil, fmt.Errorf("blend: PriceData: %q field is not an i128 (got %v)", "price", priceVal.Type)
	}

	tsVal, ok := mapGet(m, "timestamp")
	if !ok {
		return nil, fmt.Errorf("blend: PriceData: missing %q field", "timestamp")
	}
	ts, ok := u64Val(tsVal)
	if !ok {
		return nil, fmt.Errorf("blend: PriceData: %q field is not a u64 (got %v)", "timestamp", tsVal.Type)
	}

	return &PriceData{Price: i128ToBigInt(parts), Timestamp: ts}, nil
}
