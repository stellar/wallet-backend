// Package infrastructure provides Soroban transaction helpers for integration tests.
//
// This file adds the pure ScVal builder functions for the Blend/SEP-40 contract UDTs;
// transactions built from them are driven by executeSorobanOperation
// (soroban_transactions.go), which takes the acting keypair as its source parameter.
package infrastructure

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
)

// BlendRequest mirrors the Blend v2 pool Request struct. RequestType encodes the pool action:
// 0=Supply, 1=Withdraw, 2=SupplyCollateral, 3=WithdrawCollateral, 4=Borrow, 5=Repay,
// 6=FillUserLiquidationAuction, 7=FillBadDebtAuction, 8=FillInterestAuction,
// 9=DeleteLiquidationAuction.
type BlendRequest struct {
	RequestType uint32
	Address     string
	Amount      *big.Int
}

// BlendReserveConfig mirrors the Blend v2 pool ReserveConfig struct.
type BlendReserveConfig struct {
	CFactor    uint32
	Decimals   uint32
	Enabled    bool
	Index      uint32
	LFactor    uint32
	MaxUtil    uint32
	RBase      uint32
	ROne       uint32
	RThree     uint32
	RTwo       uint32
	Reactivity uint32
	SupplyCap  *big.Int
	Util       uint32
}

// BlendEmissionMetadata mirrors the Blend v2 pool ReserveEmissionMetadata struct.
type BlendEmissionMetadata struct {
	ResIndex uint32
	ResType  uint32
	Share    uint64
}

// scMapEntry is a single symbol-keyed entry destined for an xdr.ScMap. Callers must pass entries
// to scMap in ascending symbol-sort order (Soroban encodes UDT structs as symbol-sorted ScMaps).
type scMapEntry struct {
	key string
	val xdr.ScVal
}

// scAddr converts a G- or C-address into an ScvAddress ScVal.
func scAddr(t *testing.T, addr string) xdr.ScVal {
	t.Helper()
	scAddress, err := parseAddressToScAddress(addr)
	require.NoError(t, err, "parsing address %s", addr)
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddress}
}

// maxI128 and minI128 bound the signed 128-bit range int128MagnitudeBytes encodes into:
// [-2^127, 2^127-1].
var (
	maxI128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	minI128 = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
)

// int128MagnitudeBytes converts v into its 16-byte, big-endian, 128-bit two's complement
// representation, or returns an error if v is outside the signed i128 range
// [-2^127, 2^127-1] — a byte-length check alone would let magnitudes in
// (2^127-1, 2^128-1] still fit 16 bytes and silently reinterpret as negative.
// Split into a pure function (rather than inlined in scI128) so the
// overflow/negative-encoding logic can be unit tested directly, without
// needing to trigger scI128's t.Fatal path.
func int128MagnitudeBytes(v *big.Int) ([16]byte, error) {
	if v.Cmp(maxI128) > 0 || v.Cmp(minI128) < 0 {
		return [16]byte{}, fmt.Errorf("i128 value %s outside the signed 128-bit range", v.String())
	}

	mag := new(big.Int)
	if v.Sign() < 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), 128)
		mag.Add(modulus, v)
	} else {
		mag.Set(v)
	}

	b := mag.Bytes()
	var buf [16]byte
	copy(buf[16-len(b):], b)
	return buf, nil
}

// scI128 encodes v as an ScvI128 ScVal, correctly handling magnitudes above 2^63 and negative
// values via 128-bit two's complement. Fails the test if v does not fit in 128 bits.
func scI128(t *testing.T, v *big.Int) xdr.ScVal {
	t.Helper()

	buf, err := int128MagnitudeBytes(v)
	if err != nil {
		t.Fatal(err)
	}

	hi := int64(binary.BigEndian.Uint64(buf[0:8]))
	lo := binary.BigEndian.Uint64(buf[8:16])

	parts := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

// scI128FromInt64 encodes a signed 64-bit value as an ScvI128 ScVal, sign-extending into the high
// 64 bits.
func scI128FromInt64(v int64) xdr.ScVal {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	parts := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(uint64(v))} //nolint:gosec // intentional two's complement bit-pattern reinterpretation
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

// scU32 encodes v as an ScvU32 ScVal.
func scU32(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

// scU64 encodes v as an ScvU64 ScVal.
func scU64(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

// scString encodes s as an ScvString ScVal.
func scString(t *testing.T, s string) xdr.ScVal {
	t.Helper()
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

// scSymbol encodes s as an ScvSymbol ScVal.
func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

// scBytes32 encodes b as an ScvBytes ScVal.
func scBytes32(b [32]byte) xdr.ScVal {
	bs := xdr.ScBytes(b[:])
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bs}
}

// scBool encodes v as an ScvBool ScVal.
func scBool(v bool) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &v}
}

// scVec builds an ScvVec ScVal from the given values, in order.
func scVec(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// mapEntriesSortedError returns a descriptive error if entries are not in strictly ascending
// symbol-sort order (Soroban UDT ScMaps are always symbol-sorted, and duplicate keys are also
// rejected), or nil if they are already sorted. Split out from scMap so the ordering check can be
// unit tested directly, without needing to trigger scMap's t.Fatal path.
func mapEntriesSortedError(entries []scMapEntry) error {
	for i := 1; i < len(entries); i++ {
		if entries[i-1].key >= entries[i].key {
			return fmt.Errorf("scMap entries not symbol-sorted: %q must sort before %q at index %d", entries[i-1].key, entries[i].key, i)
		}
	}
	return nil
}

// scMap builds an ScvMap ScVal (a Soroban UDT struct) from the given entries. Entries MUST be
// passed in ascending symbol-sort order; scMap fails the test otherwise, since Soroban UDT ScMaps
// are always symbol-sorted and an out-of-order map would not match what a real contract call
// produces or expects.
func scMap(t *testing.T, entries ...scMapEntry) xdr.ScVal {
	t.Helper()

	if err := mapEntriesSortedError(entries); err != nil {
		t.Fatal(err)
	}

	m := make(xdr.ScMap, 0, len(entries))
	for _, e := range entries {
		sym := xdr.ScSymbol(e.key)
		m = append(m, xdr.ScMapEntry{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			Val: e.val,
		})
	}
	mPtr := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mPtr}
}

// scRequestVec builds a Vec<Request> ScVal from the given Blend requests. Each Request struct
// encodes as a symbol-sorted map with keys "address", "amount", "request_type" (alphabetical
// order, which also matches the struct's declared field order).
func scRequestVec(t *testing.T, reqs []BlendRequest) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(reqs))
	for _, r := range reqs {
		vals = append(vals, scMap(t,
			scMapEntry{key: "address", val: scAddr(t, r.Address)},
			scMapEntry{key: "amount", val: scI128(t, r.Amount)},
			scMapEntry{key: "request_type", val: scU32(r.RequestType)},
		))
	}
	return scVec(vals...)
}

// scReserveConfig builds a ReserveConfig ScVal. The struct encodes as a symbol-sorted map with 13
// keys; notably "r_three" sorts before "r_two" (byte 'h' < 'w'), and "r_two" sorts before
// "reactivity" ('_' < 'e' in ASCII).
func scReserveConfig(t *testing.T, cfg BlendReserveConfig) xdr.ScVal {
	t.Helper()

	return scMap(t,
		scMapEntry{key: "c_factor", val: scU32(cfg.CFactor)},
		scMapEntry{key: "decimals", val: scU32(cfg.Decimals)},
		scMapEntry{key: "enabled", val: scBool(cfg.Enabled)},
		scMapEntry{key: "index", val: scU32(cfg.Index)},
		scMapEntry{key: "l_factor", val: scU32(cfg.LFactor)},
		scMapEntry{key: "max_util", val: scU32(cfg.MaxUtil)},
		scMapEntry{key: "r_base", val: scU32(cfg.RBase)},
		scMapEntry{key: "r_one", val: scU32(cfg.ROne)},
		scMapEntry{key: "r_three", val: scU32(cfg.RThree)},
		scMapEntry{key: "r_two", val: scU32(cfg.RTwo)},
		scMapEntry{key: "reactivity", val: scU32(cfg.Reactivity)},
		scMapEntry{key: "supply_cap", val: scI128(t, cfg.SupplyCap)},
		scMapEntry{key: "util", val: scU32(cfg.Util)},
	)
}

// scEmissionMetadataVec builds a Vec<ReserveEmissionMetadata> ScVal. Each entry encodes as a
// symbol-sorted map with keys "res_index", "res_type", "share" (share is u64, not u32).
func scEmissionMetadataVec(t *testing.T, metas []BlendEmissionMetadata) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(metas))
	for _, m := range metas {
		vals = append(vals, scMap(t,
			scMapEntry{key: "res_index", val: scU32(m.ResIndex)},
			scMapEntry{key: "res_type", val: scU32(m.ResType)},
			scMapEntry{key: "share", val: scU64(m.Share)},
		))
	}
	return scVec(vals...)
}

// scSep40Asset builds the SEP-40 Asset::Stellar(Address) enum variant, which encodes as
// Vec[Symbol("Stellar"), Address].
func scSep40Asset(t *testing.T, addr string) xdr.ScVal {
	t.Helper()
	return scVec(scSymbol("Stellar"), scAddr(t, addr))
}

// scSep40OtherAsset builds the SEP-40 Asset::Other(Symbol) enum variant, which encodes as
// Vec[Symbol("Other"), Symbol].
func scSep40OtherAsset(sym string) xdr.ScVal {
	return scVec(scSymbol("Other"), scSymbol(sym))
}

// scSep40StellarAssetVec builds a Vec of SEP-40 Asset::Stellar(Address) variants, one per address.
func scSep40StellarAssetVec(t *testing.T, addrs []string) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(addrs))
	for _, a := range addrs {
		vals = append(vals, scSep40Asset(t, a))
	}
	return scVec(vals...)
}
