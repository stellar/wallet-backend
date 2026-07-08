package blend

import (
	"math"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test fixtures -----------------------------------------------------------

func symScVal(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func u32ScVal(n uint32) xdr.ScVal {
	v := xdr.Uint32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &v}
}

func i128ScVal(n int64) xdr.ScVal {
	var parts xdr.Int128Parts
	if n >= 0 {
		parts = xdr.Int128Parts{Hi: xdr.Int64(0), Lo: xdr.Uint64(n)}
	} else {
		parts = xdr.Int128Parts{Hi: xdr.Int64(-1), Lo: xdr.Uint64(n)}
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

func mapScVal(entries ...xdr.ScMapEntry) *xdr.ScMap {
	m := xdr.ScMap(entries)
	return &m
}

func contractAddrScVal(t *testing.T, cAddr string) xdr.ScVal {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, cAddr)
	require.NoError(t, err)
	var cid xdr.ContractId
	copy(cid[:], raw)
	scAddr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &cid}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}
}

func accountAddrScVal(t *testing.T, gAddr string) xdr.ScVal {
	t.Helper()
	accountID, err := xdr.AddressToAccountId(gAddr)
	require.NoError(t, err)
	scAddr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accountID}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}
}

// tests ---------------------------------------------------------------------

func TestMapGet(t *testing.T) {
	m := mapScVal(
		xdr.ScMapEntry{Key: symScVal("status"), Val: u32ScVal(1)},
		xdr.ScMapEntry{Key: symScVal("bstop_rate"), Val: u32ScVal(2500)},
	)

	t.Run("returns the value for a present key", func(t *testing.T) {
		v, ok := mapGet(m, "status")
		require.True(t, ok)
		u, ok := u32Val(v)
		require.True(t, ok)
		assert.Equal(t, uint32(1), u)
	})

	t.Run("returns false for a missing key", func(t *testing.T) {
		_, ok := mapGet(m, "oracle")
		assert.False(t, ok)
	})

	t.Run("returns false for a nil map", func(t *testing.T) {
		_, ok := mapGet(nil, "status")
		assert.False(t, ok)
	})
}

func TestI128String(t *testing.T) {
	t.Run("decodes a positive value", func(t *testing.T) {
		s, ok := i128String(i128ScVal(1_000_000))
		require.True(t, ok)
		assert.Equal(t, "1000000", s)
	})

	t.Run("decodes zero", func(t *testing.T) {
		s, ok := i128String(i128ScVal(0))
		require.True(t, ok)
		assert.Equal(t, "0", s)
	})

	t.Run("decodes a negative value", func(t *testing.T) {
		s, ok := i128String(i128ScVal(-42))
		require.True(t, ok)
		assert.Equal(t, "-42", s)
	})

	t.Run("returns false for a non-i128 value", func(t *testing.T) {
		_, ok := i128String(u32ScVal(1))
		assert.False(t, ok)
	})
}

func TestAddrString(t *testing.T) {
	const (
		contractAddr = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		accountAddr  = "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
	)

	t.Run("decodes a contract address", func(t *testing.T) {
		s, ok := addrString(contractAddrScVal(t, contractAddr))
		require.True(t, ok)
		assert.Equal(t, contractAddr, s)
	})

	t.Run("decodes an account address", func(t *testing.T) {
		s, ok := addrString(accountAddrScVal(t, accountAddr))
		require.True(t, ok)
		assert.Equal(t, accountAddr, s)
	})

	t.Run("returns false for a non-address value", func(t *testing.T) {
		_, ok := addrString(u32ScVal(1))
		assert.False(t, ok)
	})
}

func TestU32Val(t *testing.T) {
	t.Run("decodes a u32 value", func(t *testing.T) {
		u, ok := u32Val(u32ScVal(42))
		require.True(t, ok)
		assert.Equal(t, uint32(42), u)
	})

	t.Run("returns false for a non-u32 value", func(t *testing.T) {
		_, ok := u32Val(i128ScVal(1))
		assert.False(t, ok)
	})
}

// TestBuildSep40StellarAsset verification note ----------------------------
//
// SEP-40 (stellar-protocol ecosystem/sep-0040.md, fetched 2026-07-08) defines:
//
//	#[contracttype]
//	enum Asset { Stellar(Address), Other(Symbol) }
//	fn lastprice(env: Env, asset: Asset) -> Option<PriceData>;
//	#[contracttype]
//	pub struct PriceData { price: i128, timestamp: u64 }
//
// The tuple-variant enum's 2-element-ScVec([Symbol, payload]) encoding
// matches this package's existing decodeVecKeyEntry convention for Blend's
// own persistent storage keys.
//
// Live-confirmed on 2026-07-08 against real mainnet contracts (stellar-cli
// 27.0.0, rpc-url https://mainnet.sorobanrpc.com):
//   - `stellar contract info interface` against the deployed Reflector
//     "external CEXs & DEXs" oracle
//     (CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN) dumped:
//     `pub enum Asset { Stellar(soroban_sdk::Address), Other(soroban_sdk::Symbol) }`,
//     `pub struct PriceData { pub price: i128, pub timestamp: u64 }`,
//     `fn lastprice(env: soroban_sdk::Env, asset: Asset) -> Option<PriceData>;`
//     — an exact match for the SEP-40 shapes above.
//   - A live simulation of lastprice(Asset::Stellar(<XLM SAC
//     CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA>)) against the
//     Reflector "Stellar Mainnet DEX" oracle
//     (CALI2BYU2JE6WVRUFYTS6MSBNEHGJ35P4AVCZYF3B6QOE3QKOB2PLE6M) round-tripped
//     to a real `Some(PriceData{price, timestamp})` response, proving the
//     Asset::Stellar(Address) argument encoding this test asserts is what a
//     production oracle actually expects.
func TestBuildSep40StellarAsset(t *testing.T) {
	const contractAddr = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

	t.Run("encodes Asset::Stellar(Address) as a 2-elem ScVec", func(t *testing.T) {
		got, err := buildSep40StellarAsset(contractAddr)
		require.NoError(t, err)

		wantVec := &xdr.ScVec{
			symScVal("Stellar"),
			contractAddrScVal(t, contractAddr),
		}
		want := xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &wantVec}

		assert.Equal(t, want, got)
	})

	t.Run("returns an error for a malformed address", func(t *testing.T) {
		_, err := buildSep40StellarAsset("not-a-strkey-address")
		assert.Error(t, err)
	})

	t.Run("returns an error for a well-formed but non-contract strkey address", func(t *testing.T) {
		// A valid G... (account) strkey decodes cleanly but fails the
		// VersionByteContract check inside strkey.Decode.
		_, err := buildSep40StellarAsset("GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE")
		assert.Error(t, err)
	})
}

func TestDecodePriceData(t *testing.T) {
	t.Run("decodes Some(PriceData)", func(t *testing.T) {
		// Field-name Symbols in alphabetical order, matching how a Soroban
		// SDK #[contracttype] struct actually serializes.
		v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		m := mapScVal(
			xdr.ScMapEntry{Key: symScVal("price"), Val: i128ScVal(1_000_000)},
			xdr.ScMapEntry{Key: symScVal("timestamp"), Val: u64ScVal(1_700_000_000)},
		)
		v.Map = &m

		pd, err := decodePriceData(v)
		require.NoError(t, err)
		require.NotNil(t, pd)
		assert.Equal(t, big.NewInt(1_000_000), pd.Price)
		assert.Equal(t, uint64(1_700_000_000), pd.Timestamp)
	})

	t.Run("decodes None (ScvVoid) to (nil, nil)", func(t *testing.T) {
		pd, err := decodePriceData(voidScVal())
		require.NoError(t, err)
		assert.Nil(t, pd)
	})

	t.Run("returns an error for a value that is neither a map nor void", func(t *testing.T) {
		_, err := decodePriceData(u32ScVal(1))
		assert.Error(t, err)
	})

	t.Run("returns an error when the price field is missing", func(t *testing.T) {
		v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		m := mapScVal(
			xdr.ScMapEntry{Key: symScVal("timestamp"), Val: u64ScVal(1_700_000_000)},
		)
		v.Map = &m

		_, err := decodePriceData(v)
		assert.Error(t, err)
	})

	t.Run("returns an error when the timestamp field is missing", func(t *testing.T) {
		v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		m := mapScVal(
			xdr.ScMapEntry{Key: symScVal("price"), Val: i128ScVal(1_000_000)},
		)
		v.Map = &m

		_, err := decodePriceData(v)
		assert.Error(t, err)
	})

	t.Run("returns an error when timestamp is not a u64", func(t *testing.T) {
		v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		m := mapScVal(
			xdr.ScMapEntry{Key: symScVal("price"), Val: i128ScVal(1_000_000)},
			xdr.ScMapEntry{Key: symScVal("timestamp"), Val: i128ScVal(1_700_000_000)},
		)
		v.Map = &m

		_, err := decodePriceData(v)
		assert.Error(t, err)
	})

	t.Run("returns an error when price is not an i128", func(t *testing.T) {
		v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		m := mapScVal(
			xdr.ScMapEntry{Key: symScVal("price"), Val: u32ScVal(1)},
			xdr.ScMapEntry{Key: symScVal("timestamp"), Val: u64ScVal(1_700_000_000)},
		)
		v.Map = &m

		_, err := decodePriceData(v)
		assert.Error(t, err)
	})
}

func TestI128ToBigInt(t *testing.T) {
	t.Run("decodes zero", func(t *testing.T) {
		got := i128ToBigInt(xdr.Int128Parts{Hi: 0, Lo: 0})
		assert.Equal(t, big.NewInt(0), got)
	})

	t.Run("decodes a small positive value", func(t *testing.T) {
		got := i128ToBigInt(xdr.Int128Parts{Hi: 0, Lo: 42})
		assert.Equal(t, big.NewInt(42), got)
	})

	t.Run("decodes a value greater than 2^64 (Hi > 0)", func(t *testing.T) {
		got := i128ToBigInt(xdr.Int128Parts{Hi: 1, Lo: 0})
		want, ok := new(big.Int).SetString("18446744073709551616", 10) // 2^64
		require.True(t, ok)
		assert.Equal(t, want, got)
	})

	t.Run("decodes -1 (Hi=-1, Lo=MaxUint64 two's-complement encoding)", func(t *testing.T) {
		got := i128ToBigInt(xdr.Int128Parts{Hi: -1, Lo: xdr.Uint64(math.MaxUint64)})
		assert.Equal(t, big.NewInt(-1), got)
	})

	t.Run("decodes a negative value with Hi < -1", func(t *testing.T) {
		got := i128ToBigInt(xdr.Int128Parts{Hi: -2, Lo: 0})
		want, ok := new(big.Int).SetString("-36893488147419103232", 10) // -2 * 2^64
		require.True(t, ok)
		assert.Equal(t, want, got)
	})

	t.Run("round-trips against independently computed big.Int arithmetic", func(t *testing.T) {
		parts := xdr.Int128Parts{Hi: 5, Lo: 12345}
		got := i128ToBigInt(parts)

		want := new(big.Int).Lsh(big.NewInt(int64(parts.Hi)), 64)
		want.Add(want, new(big.Int).SetUint64(uint64(parts.Lo)))
		assert.Equal(t, want, got)
	})
}
