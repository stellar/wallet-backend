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

func TestParseTransferEvent_Classic(t *testing.T) {
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
}

func TestParseTransferEvent_CAP67Map(t *testing.T) {
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
}

// TestParseMintEvent_Normalized covers the soroban-sdk 25.x topic shape: [sym, to].
func TestParseMintEvent_Normalized(t *testing.T) {
	event := contractEvent(
		[]xdr.ScVal{symScVal(EventMint), mustAddressScVal(t, testAccountB)},
		i128ScVal(999),
	)
	got, err := ParseMintEvent(event)
	require.NoError(t, err)
	assert.Equal(t, testAccountB, got.To)
	assert.Equal(t, big.NewInt(999), got.Amount)
}

// TestParseMintEvent_LegacyAdminTopic covers the legacy SAC / soroban-sdk <=24.x shape
// `[sym("mint"), admin: Address, to: Address]`. `to` must be read from the last topic.
func TestParseMintEvent_LegacyAdminTopic(t *testing.T) {
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
}

// TestParseMintEvent_RejectsUnsupportedTopicCount guards the parser from accepting
// shapes it doesn't actually understand (e.g., a single-topic emit).
func TestParseMintEvent_RejectsUnsupportedTopicCount(t *testing.T) {
	event := contractEvent(
		[]xdr.ScVal{symScVal(EventMint)},
		i128ScVal(1),
	)
	_, err := ParseMintEvent(event)
	assert.Error(t, err)
}

// TestParseMintEvent_Rejects3TopicWithNonAddressAdmin rejects a 3-topic shape where
// the middle (admin) topic isn't an Address — this isn't the legacy SEP-41 mint
// shape, so we shouldn't silently accept it just because the last topic happens
// to be an address.
func TestParseMintEvent_Rejects3TopicWithNonAddressAdmin(t *testing.T) {
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
}

func TestParseBurnEvent(t *testing.T) {
	event := contractEvent(
		[]xdr.ScVal{symScVal(EventBurn), mustAddressScVal(t, testAccountA)},
		i128ScVal(50),
	)
	got, err := ParseBurnEvent(event)
	require.NoError(t, err)
	assert.Equal(t, testAccountA, got.From)
	assert.Equal(t, big.NewInt(50), got.Amount)
}

func TestParseClawbackEvent_Normalized(t *testing.T) {
	event := contractEvent(
		[]xdr.ScVal{symScVal(EventClawback), mustAddressScVal(t, testAccountA)},
		i128ScVal(25),
	)
	got, err := ParseClawbackEvent(event)
	require.NoError(t, err)
	assert.Equal(t, testAccountA, got.From)
	assert.Equal(t, big.NewInt(25), got.Amount)
}

// TestParseClawbackEvent_LegacyAdminTopic covers the legacy 3-topic shape
// `[sym("clawback"), admin: Address, from: Address]`. `from` must be the last topic.
func TestParseClawbackEvent_LegacyAdminTopic(t *testing.T) {
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
}

// TestParseClawbackEvent_Rejects3TopicWithNonAddressAdmin mirrors the mint guard:
// 3-topic clawback events whose admin slot isn't an Address are rejected.
func TestParseClawbackEvent_Rejects3TopicWithNonAddressAdmin(t *testing.T) {
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
}

func TestParseApproveEvent(t *testing.T) {
	data := vecScVal(i128ScVal(500), u32ScVal(1234))
	event := contractEvent(
		[]xdr.ScVal{
			symScVal(EventApprove),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		},
		data,
	)
	got, err := ParseApproveEvent(event)
	require.NoError(t, err)
	assert.Equal(t, testAccountA, got.From)
	assert.Equal(t, testAccountB, got.Spender)
	assert.Equal(t, big.NewInt(500), got.Amount)
	assert.Equal(t, uint32(1234), got.LiveUntilLedger)
}

func TestParseTransferEvent_WrongTopicCount(t *testing.T) {
	event := contractEvent(
		[]xdr.ScVal{symScVal(EventTransfer), mustAddressScVal(t, testAccountA)},
		i128ScVal(1),
	)
	_, err := ParseTransferEvent(event)
	assert.Error(t, err)
}

func TestParseTransferEvent_WrongSymbol(t *testing.T) {
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
