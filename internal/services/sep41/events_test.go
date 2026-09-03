package sep41

import (
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test fixtures -----------------------------------------------------------------------

const (
	testAccountA = "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
	testAccountB = "GDSL6NQIMQ76EOJZ7Y7MUQJYKL4UTFR4TSCSOQEKUB2F7M4KRAW3NGFH"
)

func mustAddressScVal(t *testing.T, strkeyAddr string) xdr.ScVal {
	t.Helper()
	accountID, err := xdr.AddressToAccountId(strkeyAddr)
	require.NoError(t, err)
	scAddr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accountID}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}
}

func symScVal(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
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

func u32ScVal(n uint32) xdr.ScVal {
	v := xdr.Uint32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &v}
}

func u64ScVal(n uint64) xdr.ScVal {
	v := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &v}
}

func strScVal(s string) xdr.ScVal {
	v := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &v}
}

func mapScVal(entries ...xdr.ScMapEntry) xdr.ScVal {
	m := xdr.ScMap(entries)
	mp := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
}

func vecScVal(vals ...xdr.ScVal) xdr.ScVal {
	v := xdr.ScVec(vals)
	vp := &v
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vp}
}

func contractEvent(topics []xdr.ScVal, data xdr.ScVal) xdr.ContractEvent {
	var cid xdr.ContractId
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   data,
			},
		},
	}
}

// tests -----------------------------------------------------------------------

func TestParseTransferEvent(t *testing.T) {
	t.Run("parses a classic 3-topic [sym, from, to] transfer with int128 amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventTransfer),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			i128ScVal(1_000_000),
		)

		got, err := ParseTransferEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, testAccountB, got.To)
		assert.Equal(t, big.NewInt(1_000_000), got.Amount)
		assert.Nil(t, got.ToMuxedID)
	})

	t.Run("parses a CAP-67 map data payload with amount and to_muxed_id", func(t *testing.T) {
		dataMap := mapScVal(
			xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(42)},
			xdr.ScMapEntry{Key: symScVal("to_muxed_id"), Val: u64ScVal(7)},
		)
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventTransfer),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			dataMap,
		)

		got, err := ParseTransferEvent(event)
		require.NoError(t, err)
		assert.Equal(t, big.NewInt(42), got.Amount)
		require.NotNil(t, got.ToMuxedID)
		assert.Equal(t, uint64(7), *got.ToMuxedID)
	})

	t.Run("rejects a CAP-67 map with a non-Symbol key", func(t *testing.T) {
		dataMap := mapScVal(
			xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(42)},
			xdr.ScMapEntry{Key: strScVal("extension"), Val: u32ScVal(1)},
		)
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventTransfer),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			dataMap,
		)

		_, err := ParseTransferEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects a CAP-67 map with duplicate keys", func(t *testing.T) {
		dataMap := mapScVal(
			xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(42)},
			xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(43)},
		)
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventTransfer),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			dataMap,
		)

		_, err := ParseTransferEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects a transfer event whose topic count is not 3", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventTransfer), mustAddressScVal(t, testAccountA)},
			i128ScVal(1),
		)
		_, err := ParseTransferEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects an event whose leading topic is not the transfer symbol", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{
				symScVal("not_transfer"),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			i128ScVal(1),
		)
		_, err := ParseTransferEvent(event)
		assert.Error(t, err)
	})
}

func TestParseMintEvent(t *testing.T) {
	t.Run("parses the soroban-sdk 25.x [sym, to] topic shape", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventMint), mustAddressScVal(t, testAccountB)},
			i128ScVal(999),
		)
		got, err := ParseMintEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountB, got.To)
		assert.Equal(t, big.NewInt(999), got.Amount)
	})

	t.Run("parses the legacy SAC [sym, admin, to] topic shape with the admin slot ignored", func(t *testing.T) {
		// Legacy SAC / soroban-sdk <=24.x shape `[sym("mint"), admin: Address, to: Address]`;
		// `to` must be read from the last topic.
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventMint),
				mustAddressScVal(t, testAccountA), // admin (ignored)
				mustAddressScVal(t, testAccountB), // to
			},
			i128ScVal(1_234_567),
		)
		got, err := ParseMintEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountB, got.To)
		assert.Equal(t, big.NewInt(1_234_567), got.Amount)
	})

	t.Run("rejects a single-topic emit with no recipient", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventMint)},
			i128ScVal(1),
		)
		_, err := ParseMintEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects a 3-topic shape whose admin slot is not an Address", func(t *testing.T) {
		// Not the legacy SEP-41 mint shape, so we shouldn't silently accept it just
		// because the last topic happens to be an address.
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventMint),
				symScVal("not_an_address"),        // wrong type in admin slot
				mustAddressScVal(t, testAccountB), // valid recipient
			},
			i128ScVal(42),
		)
		_, err := ParseMintEvent(event)
		assert.Error(t, err)
	})
}

func TestParseBurnEvent(t *testing.T) {
	t.Run("parses a bare i128 amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventBurn), mustAddressScVal(t, testAccountA)},
			i128ScVal(50),
		)
		got, err := ParseBurnEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, big.NewInt(50), got.Amount)
	})

	t.Run("parses an OpenZeppelin map amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventBurn), mustAddressScVal(t, testAccountA)},
			mapScVal(
				xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(75)},
				xdr.ScMapEntry{Key: symScVal("extension"), Val: u32ScVal(1)},
			),
		)
		got, err := ParseBurnEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, big.NewInt(75), got.Amount)
	})

	t.Run("rejects a map without amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventBurn), mustAddressScVal(t, testAccountA)},
			mapScVal(xdr.ScMapEntry{Key: symScVal("extension"), Val: u32ScVal(1)}),
		)
		_, err := ParseBurnEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects a map with a non-i128 amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventBurn), mustAddressScVal(t, testAccountA)},
			mapScVal(xdr.ScMapEntry{Key: symScVal("amount"), Val: u32ScVal(75)}),
		)
		_, err := ParseBurnEvent(event)
		assert.Error(t, err)
	})
}

func TestParseClawbackEvent(t *testing.T) {
	t.Run("parses the soroban-sdk 25.x [sym, from] topic shape", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventClawback), mustAddressScVal(t, testAccountA)},
			i128ScVal(25),
		)
		got, err := ParseClawbackEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, big.NewInt(25), got.Amount)
	})

	t.Run("parses the legacy SAC [sym, admin, from] topic shape with the admin slot ignored", func(t *testing.T) {
		// Legacy 3-topic shape `[sym("clawback"), admin: Address, from: Address]`;
		// `from` must be read from the last topic.
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventClawback),
				mustAddressScVal(t, testAccountB), // admin (ignored)
				mustAddressScVal(t, testAccountA), // from
			},
			i128ScVal(77),
		)
		got, err := ParseClawbackEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, big.NewInt(77), got.Amount)
	})

	t.Run("parses a SEP-41 map amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventClawback), mustAddressScVal(t, testAccountA)},
			mapScVal(xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(25)}),
		)
		got, err := ParseClawbackEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, big.NewInt(25), got.Amount)
	})

	t.Run("rejects a map without amount", func(t *testing.T) {
		event := contractEvent(
			[]xdr.ScVal{symScVal(EventClawback), mustAddressScVal(t, testAccountA)},
			mapScVal(xdr.ScMapEntry{Key: symScVal("extension"), Val: u32ScVal(1)}),
		)
		_, err := ParseClawbackEvent(event)
		assert.Error(t, err)
	})

	t.Run("rejects a 3-topic shape whose admin slot is not an Address", func(t *testing.T) {
		// Mirrors the mint guard: a 3-topic clawback whose admin slot isn't an Address
		// isn't the legacy shape and must be rejected.
		event := contractEvent(
			[]xdr.ScVal{
				symScVal(EventClawback),
				symScVal("not_an_address"),
				mustAddressScVal(t, testAccountA),
			},
			i128ScVal(11),
		)
		_, err := ParseClawbackEvent(event)
		assert.Error(t, err)
	})
}

func TestParseApproveEvent(t *testing.T) {
	topics := []xdr.ScVal{
		symScVal(EventApprove),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}

	t.Run("parses the positional ScVec format", func(t *testing.T) {
		event := contractEvent(topics, vecScVal(i128ScVal(500), u32ScVal(1234)))
		got, err := ParseApproveEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, testAccountB, got.Spender)
		assert.Equal(t, big.NewInt(500), got.Amount)
		assert.Equal(t, uint32(1234), got.LiveUntilLedger)
	})

	t.Run("parses the OpenZeppelin map format", func(t *testing.T) {
		event := contractEvent(topics, mapScVal(
			xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(750)},
			xdr.ScMapEntry{Key: symScVal("extension"), Val: u32ScVal(1)},
			xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: u32ScVal(4321)},
		))
		got, err := ParseApproveEvent(event)
		require.NoError(t, err)
		assert.Equal(t, testAccountA, got.From)
		assert.Equal(t, testAccountB, got.Spender)
		assert.Equal(t, big.NewInt(750), got.Amount)
		assert.Equal(t, uint32(4321), got.LiveUntilLedger)
	})

	tests := []struct {
		name string
		data xdr.ScVal
	}{
		{
			name: "map missing amount",
			data: mapScVal(xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: u32ScVal(4321)}),
		},
		{
			name: "map missing live_until_ledger",
			data: mapScVal(xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(750)}),
		},
		{
			name: "map with non-i128 amount",
			data: mapScVal(
				xdr.ScMapEntry{Key: symScVal("amount"), Val: u32ScVal(750)},
				xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: u32ScVal(4321)},
			),
		},
		{
			name: "map with non-u32 live_until_ledger",
			data: mapScVal(
				xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(750)},
				xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: i128ScVal(4321)},
			),
		},
		{
			name: "map with a non-Symbol key",
			data: mapScVal(
				xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(750)},
				xdr.ScMapEntry{Key: strScVal("extension"), Val: u32ScVal(1)},
				xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: u32ScVal(4321)},
			),
		},
		{
			name: "map with a duplicate field",
			data: mapScVal(
				xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(750)},
				xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(751)},
				xdr.ScMapEntry{Key: symScVal("live_until_ledger"), Val: u32ScVal(4321)},
			),
		},
		{
			name: "nil map",
			data: xdr.ScVal{Type: xdr.ScValTypeScvMap},
		},
		{
			name: "unrelated data type",
			data: i128ScVal(750),
		},
	}

	for _, tt := range tests {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			_, err := ParseApproveEvent(contractEvent(topics, tt.data))
			assert.Error(t, err)
		})
	}
}

func TestContractIDString(t *testing.T) {
	var cid xdr.ContractId
	cid[0] = 0xDE
	cid[31] = 0xAD
	want, err := strkey.Encode(strkey.VersionByteContract, cid[:])
	require.NoError(t, err)

	event := xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: []xdr.ScVal{}, Data: i128ScVal(0)},
		},
	}
	got, err := ContractIDString(event)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// mustMuxedAddressScVal builds an ScVal holding a muxed ScAddress over baseG with the
// given sub-id, as it appears in a transfer to/from topic.
func mustMuxedAddressScVal(t *testing.T, baseG string, id uint64) xdr.ScVal {
	t.Helper()
	var ed xdr.Uint256
	copy(ed[:], strkey.MustDecode(strkey.VersionByteAccountID, baseG))
	scAddr := xdr.ScAddress{
		Type:         xdr.ScAddressTypeScAddressTypeMuxedAccount,
		MuxedAccount: &xdr.MuxedEd25519Account{Id: xdr.Uint64(id), Ed25519: ed},
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}
}

// TestParseTransferEvent_MuxedTopicNormalizedToBase verifies that a muxed address in a
// transfer topic is decoded to its base account rather than its full M-strkey.
func TestParseTransferEvent_MuxedTopicNormalizedToBase(t *testing.T) {
	base := testAccountA
	muxed := muxedStrkey(t, base, 9)
	require.Equal(t, byte('M'), muxed[0])

	event := contractEvent(
		[]xdr.ScVal{symScVal(EventTransfer), mustAddressScVal(t, testAccountB), mustMuxedAddressScVal(t, base, 9)},
		i128ScVal(500),
	)

	got, err := ParseTransferEvent(event)
	require.NoError(t, err)
	assert.Equal(t, base, got.To, "muxed `to` topic must decode to its base account")
	assert.NotEqual(t, muxed, got.To, "the full M-strkey must not survive into the key")
}

// muxedStrkey returns the M... strkey for a muxed account over baseG with the given sub-id.
func muxedStrkey(t *testing.T, baseG string, id uint64) string {
	t.Helper()
	var ed xdr.Uint256
	copy(ed[:], strkey.MustDecode(strkey.VersionByteAccountID, baseG))
	m := xdr.MuxedAccount{Type: xdr.CryptoKeyTypeKeyTypeMuxedEd25519, Med25519: &xdr.MuxedAccountMed25519{Id: xdr.Uint64(id), Ed25519: ed}}
	addr, err := m.GetAddress()
	require.NoError(t, err)
	return addr
}
