package blend

import (
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
