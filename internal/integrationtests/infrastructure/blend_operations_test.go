package infrastructure

import (
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
)

// unwrapVec dereferences an ScvVec ScVal down to its underlying xdr.ScVec.
func unwrapVec(t *testing.T, v xdr.ScVal) xdr.ScVec {
	t.Helper()
	require.Equal(t, xdr.ScValTypeScvVec, v.Type)
	require.NotNil(t, v.Vec)
	require.NotNil(t, *v.Vec)
	return **v.Vec
}

// unwrapMap dereferences an ScvMap ScVal down to its underlying xdr.ScMap.
func unwrapMap(t *testing.T, v xdr.ScVal) xdr.ScMap {
	t.Helper()
	require.Equal(t, xdr.ScValTypeScvMap, v.Type)
	require.NotNil(t, v.Map)
	require.NotNil(t, *v.Map)
	return **v.Map
}

// mapKeys extracts the ordered symbol keys of an xdr.ScMap.
func mapKeys(t *testing.T, m xdr.ScMap) []string {
	t.Helper()
	keys := make([]string, len(m))
	for i, e := range m {
		require.Equal(t, xdr.ScValTypeScvSymbol, e.Key.Type)
		require.NotNil(t, e.Key.Sym)
		keys[i] = string(*e.Key.Sym)
	}
	return keys
}

func TestBlendScValAddr(t *testing.T) {
	g := keypair.MustRandom().Address()
	v := scAddr(t, g)
	require.Equal(t, xdr.ScValTypeScvAddress, v.Type)
	require.NotNil(t, v.Address)
	require.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, v.Address.Type)
	require.Equal(t, g, v.Address.AccountId.Address())
}

func TestBlendScValI128LargeMagnitude(t *testing.T) {
	// (1<<80) + 5: bit 80 falls in the high 64 bits (bit 16 of Hi), so Hi=1<<16=65536, Lo=5.
	val := new(big.Int).Lsh(big.NewInt(1), 80)
	val.Add(val, big.NewInt(5))

	v := scI128(t, val)
	require.Equal(t, xdr.ScValTypeScvI128, v.Type)
	require.NotNil(t, v.I128)
	require.Equal(t, xdr.Int64(65536), v.I128.Hi)
	require.Equal(t, xdr.Uint64(5), v.I128.Lo)
}

func TestBlendScValI128NegativeOne(t *testing.T) {
	v := scI128(t, big.NewInt(-1))
	require.Equal(t, xdr.ScValTypeScvI128, v.Type)
	require.NotNil(t, v.I128)
	require.Equal(t, xdr.Int64(-1), v.I128.Hi)
	require.Equal(t, xdr.Uint64(0xFFFFFFFFFFFFFFFF), v.I128.Lo)
}

func TestBlendScValI128Zero(t *testing.T) {
	v := scI128(t, big.NewInt(0))
	require.Equal(t, xdr.Int64(0), v.I128.Hi)
	require.Equal(t, xdr.Uint64(0), v.I128.Lo)
}

// int128MagnitudeBytes is the pure validation logic scI128 delegates to before calling t.Fatal on
// overflow; testing it directly (rather than exercising scI128's t.Fatal path via a subtest) keeps
// this suite green, since a failed subtest in Go always propagates failure to its parent test and
// therefore to the whole package's exit code (testing.common.Fail walks up to every ancestor).
func TestInt128MagnitudeBytesOverflowFails(t *testing.T) {
	huge := new(big.Int).Lsh(big.NewInt(1), 129) // exceeds 128 bits
	_, err := int128MagnitudeBytes(huge)
	require.Error(t, err)
}

func TestInt128MagnitudeBytesFitsExactly128Bits(t *testing.T) {
	maxUint128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	buf, err := int128MagnitudeBytes(maxUint128)
	require.NoError(t, err)
	require.Equal(t, [16]byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}, buf)
}

func TestBlendScValI128FromInt64(t *testing.T) {
	positive := scI128FromInt64(42)
	require.Equal(t, xdr.Int64(0), positive.I128.Hi)
	require.Equal(t, xdr.Uint64(42), positive.I128.Lo)

	negative := scI128FromInt64(-1)
	require.Equal(t, xdr.Int64(-1), negative.I128.Hi)
	require.Equal(t, xdr.Uint64(0xFFFFFFFFFFFFFFFF), negative.I128.Lo)
}

func TestBlendScValU32(t *testing.T) {
	v := scU32(7)
	require.Equal(t, xdr.ScValTypeScvU32, v.Type)
	require.Equal(t, xdr.Uint32(7), *v.U32)
}

func TestBlendScValU64(t *testing.T) {
	v := scU64(9_000_000_000)
	require.Equal(t, xdr.ScValTypeScvU64, v.Type)
	require.Equal(t, xdr.Uint64(9_000_000_000), *v.U64)
}

func TestBlendScValString(t *testing.T) {
	v := scString(t, "hello")
	require.Equal(t, xdr.ScValTypeScvString, v.Type)
	require.Equal(t, xdr.ScString("hello"), *v.Str)
}

func TestBlendScValSymbol(t *testing.T) {
	v := scSymbol("Stellar")
	require.Equal(t, xdr.ScValTypeScvSymbol, v.Type)
	require.Equal(t, xdr.ScSymbol("Stellar"), *v.Sym)
}

func TestBlendScValBytes32(t *testing.T) {
	var b [32]byte
	b[0] = 0xAB
	b[31] = 0xCD
	v := scBytes32(b)
	require.Equal(t, xdr.ScValTypeScvBytes, v.Type)
	require.Equal(t, xdr.ScBytes(b[:]), *v.Bytes)
}

func TestBlendScValBool(t *testing.T) {
	v := scBool(true)
	require.Equal(t, xdr.ScValTypeScvBool, v.Type)
	require.True(t, *v.B)

	v = scBool(false)
	require.False(t, *v.B)
}

func TestBlendScValVec(t *testing.T) {
	v := scVec(scU32(1), scU32(2), scU32(3))
	vec := unwrapVec(t, v)
	require.Len(t, vec, 3)
	require.Equal(t, xdr.Uint32(1), *vec[0].U32)
	require.Equal(t, xdr.Uint32(2), *vec[1].U32)
	require.Equal(t, xdr.Uint32(3), *vec[2].U32)
}

func TestBlendScValVecEmpty(t *testing.T) {
	v := scVec()
	vec := unwrapVec(t, v)
	require.Empty(t, vec)
}

func TestBlendScValMapSorted(t *testing.T) {
	v := scMap(t,
		scMapEntry{key: "a", val: scU32(1)},
		scMapEntry{key: "b", val: scU32(2)},
	)
	m := unwrapMap(t, v)
	require.Equal(t, []string{"a", "b"}, mapKeys(t, m))
	require.Equal(t, xdr.Uint32(1), *m[0].Val.U32)
	require.Equal(t, xdr.Uint32(2), *m[1].Val.U32)
}

// mapEntriesSortedError is the pure validation logic scMap delegates to before calling t.Fatal on
// unsorted/duplicate keys; testing it directly (rather than exercising scMap's t.Fatal path via a
// subtest) keeps this suite green — see TestInt128MagnitudeBytesOverflowFails for why.
func TestMapEntriesSortedErrorDetectsUnsortedKeys(t *testing.T) {
	err := mapEntriesSortedError([]scMapEntry{
		{key: "b", val: scU32(1)},
		{key: "a", val: scU32(2)},
	})
	require.Error(t, err)
}

func TestMapEntriesSortedErrorDetectsDuplicateKeys(t *testing.T) {
	err := mapEntriesSortedError([]scMapEntry{
		{key: "a", val: scU32(1)},
		{key: "a", val: scU32(2)},
	})
	require.Error(t, err)
}

func TestMapEntriesSortedErrorAcceptsSortedKeys(t *testing.T) {
	err := mapEntriesSortedError([]scMapEntry{
		{key: "a", val: scU32(1)},
		{key: "b", val: scU32(2)},
		{key: "c", val: scU32(3)},
	})
	require.NoError(t, err)
}

func TestBlendScValRequestVec(t *testing.T) {
	addr1 := keypair.MustRandom().Address()
	addr2 := keypair.MustRandom().Address()
	reqs := []BlendRequest{
		{RequestType: 0, Address: addr1, Amount: big.NewInt(100)},
		{RequestType: 4, Address: addr2, Amount: big.NewInt(200)},
	}

	v := scRequestVec(t, reqs)
	vec := unwrapVec(t, v)
	require.Len(t, vec, 2)

	for i, req := range reqs {
		m := unwrapMap(t, vec[i])
		require.Equal(t, []string{"address", "amount", "request_type"}, mapKeys(t, m))

		require.Equal(t, xdr.ScValTypeScvAddress, m[0].Val.Type)
		require.Equal(t, req.Address, m[0].Val.Address.AccountId.Address())

		require.Equal(t, xdr.ScValTypeScvI128, m[1].Val.Type)
		require.Equal(t, xdr.Uint64(req.Amount.Uint64()), m[1].Val.I128.Lo) //nolint:gosec // test fixture amounts are small and non-negative

		require.Equal(t, xdr.Uint32(req.RequestType), *m[2].Val.U32)
	}
}

func TestBlendScValReserveConfig(t *testing.T) {
	cfg := BlendReserveConfig{
		CFactor:    900000,
		Decimals:   7,
		Enabled:    true,
		Index:      0,
		LFactor:    950000,
		MaxUtil:    950000,
		RBase:      10000,
		ROne:       40000,
		RThree:     1500000,
		RTwo:       100000,
		Reactivity: 1000,
		SupplyCap:  big.NewInt(1_000_000_000),
		Util:       500000,
	}

	v := scReserveConfig(t, cfg)
	m := unwrapMap(t, v)

	require.Equal(t, []string{
		"c_factor", "decimals", "enabled", "index", "l_factor", "max_util",
		"r_base", "r_one", "r_three", "r_two", "reactivity", "supply_cap", "util",
	}, mapKeys(t, m))

	require.Equal(t, xdr.Uint32(cfg.CFactor), *m[0].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.Decimals), *m[1].Val.U32)
	require.True(t, *m[2].Val.B)
	require.Equal(t, xdr.Uint32(cfg.Index), *m[3].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.LFactor), *m[4].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.MaxUtil), *m[5].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.RBase), *m[6].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.ROne), *m[7].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.RThree), *m[8].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.RTwo), *m[9].Val.U32)
	require.Equal(t, xdr.Uint32(cfg.Reactivity), *m[10].Val.U32)
	require.Equal(t, xdr.ScValTypeScvI128, m[11].Val.Type)
	require.Equal(t, xdr.Uint32(cfg.Util), *m[12].Val.U32)
}

func TestBlendScValEmissionMetadataVec(t *testing.T) {
	metas := []BlendEmissionMetadata{
		{ResIndex: 0, ResType: 1, Share: 500_000_000},
		{ResIndex: 1, ResType: 0, Share: 1_500_000_000},
	}

	v := scEmissionMetadataVec(t, metas)
	vec := unwrapVec(t, v)
	require.Len(t, vec, 2)

	for i, meta := range metas {
		m := unwrapMap(t, vec[i])
		require.Equal(t, []string{"res_index", "res_type", "share"}, mapKeys(t, m))
		require.Equal(t, xdr.Uint32(meta.ResIndex), *m[0].Val.U32)
		require.Equal(t, xdr.Uint32(meta.ResType), *m[1].Val.U32)
		require.Equal(t, xdr.ScValTypeScvU64, m[2].Val.Type)
		require.Equal(t, xdr.Uint64(meta.Share), *m[2].Val.U64)
	}
}

func TestBlendScValSep40Asset(t *testing.T) {
	addr := keypair.MustRandom().Address()
	v := scSep40Asset(t, addr)
	vec := unwrapVec(t, v)
	require.Len(t, vec, 2)
	require.Equal(t, xdr.ScValTypeScvSymbol, vec[0].Type)
	require.Equal(t, xdr.ScSymbol("Stellar"), *vec[0].Sym)
	require.Equal(t, xdr.ScValTypeScvAddress, vec[1].Type)
	require.Equal(t, addr, vec[1].Address.AccountId.Address())
}

func TestBlendScValSep40OtherAsset(t *testing.T) {
	v := scSep40OtherAsset("USD")
	vec := unwrapVec(t, v)
	require.Len(t, vec, 2)
	require.Equal(t, xdr.ScSymbol("Other"), *vec[0].Sym)
	require.Equal(t, xdr.ScSymbol("USD"), *vec[1].Sym)
}

func TestBlendScValSep40StellarAssetVec(t *testing.T) {
	addr1 := keypair.MustRandom().Address()
	addr2 := keypair.MustRandom().Address()
	v := scSep40StellarAssetVec(t, []string{addr1, addr2})
	vec := unwrapVec(t, v)
	require.Len(t, vec, 2)

	for i, addr := range []string{addr1, addr2} {
		inner := unwrapVec(t, vec[i])
		require.Equal(t, xdr.ScSymbol("Stellar"), *inner[0].Sym)
		require.Equal(t, addr, inner[1].Address.AccountId.Address())
	}
}

func TestScAddressToString(t *testing.T) {
	g := keypair.MustRandom().Address()
	scAddress, err := parseAddressToScAddress(g)
	require.NoError(t, err)

	got, err := scAddressToString(scAddress)
	require.NoError(t, err)
	require.Equal(t, g, got)
}

// fakeAuthEntry builds a minimal, well-formed SorobanAuthorizationEntry for exercising
// signAuthEntriesAs. RootInvocation is a throwaway contract-fn invocation; its content doesn't
// matter for these tests beyond being valid enough to marshal into the signing preimage.
func fakeAuthEntry(credType xdr.SorobanCredentialsType, address xdr.ScAddress, nonce int64) xdr.SorobanAuthorizationEntry {
	creds := xdr.SorobanCredentials{Type: credType}
	if credType == xdr.SorobanCredentialsTypeSorobanCredentialsAddress {
		creds.Address = &xdr.SorobanAddressCredentials{
			Address: address,
			Nonce:   xdr.Int64(nonce),
		}
	}

	var contractID xdr.ContractId
	return xdr.SorobanAuthorizationEntry{
		Credentials: creds,
		RootInvocation: xdr.SorobanAuthorizedInvocation{
			Function: xdr.SorobanAuthorizedFunction{
				Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
				ContractFn: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
					FunctionName:    "test",
				},
			},
		},
	}
}

func TestSignAuthEntriesAsEmpty(t *testing.T) {
	signed, err := signAuthEntriesAs(nil, nil, 100)
	require.NoError(t, err)
	require.Empty(t, signed)
}

func TestSignAuthEntriesAsSourceAccountPassthrough(t *testing.T) {
	entry := fakeAuthEntry(xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount, xdr.ScAddress{}, 0)
	signer := keypair.MustRandom()

	signed, err := signAuthEntriesAs([]xdr.SorobanAuthorizationEntry{entry}, []*keypair.Full{signer}, 100)
	require.NoError(t, err)
	require.Equal(t, []xdr.SorobanAuthorizationEntry{entry}, signed)
}

func TestSignAuthEntriesAsSignsMatchingAddressPreservingNonce(t *testing.T) {
	signer := keypair.MustRandom()
	addr, err := parseAddressToScAddress(signer.Address())
	require.NoError(t, err)

	const nonce = int64(424242)
	const latestLedger = int64(1000)
	entry := fakeAuthEntry(xdr.SorobanCredentialsTypeSorobanCredentialsAddress, addr, nonce)

	signed, err := signAuthEntriesAs([]xdr.SorobanAuthorizationEntry{entry}, []*keypair.Full{signer}, latestLedger)
	require.NoError(t, err)
	require.Len(t, signed, 1)
	require.Equal(t, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, signed[0].Credentials.Type)
	require.NotNil(t, signed[0].Credentials.Address)
	require.Equal(t, xdr.Int64(nonce), signed[0].Credentials.Address.Nonce, "must preserve the simulation-assigned nonce, not hardcode 0")
	require.Equal(t, xdr.Uint32(latestLedger+LedgerValidityBuffer), signed[0].Credentials.Address.SignatureExpirationLedger)
	require.Equal(t, xdr.ScValTypeScvVec, signed[0].Credentials.Address.Signature.Type, "AuthorizeEntry should have populated a signature")
}

func TestSignAuthEntriesAsMatchesExtraSigner(t *testing.T) {
	primary := keypair.MustRandom()
	extra := keypair.MustRandom()
	addr, err := parseAddressToScAddress(extra.Address())
	require.NoError(t, err)

	entry := fakeAuthEntry(xdr.SorobanCredentialsTypeSorobanCredentialsAddress, addr, 1)

	signed, err := signAuthEntriesAs([]xdr.SorobanAuthorizationEntry{entry}, []*keypair.Full{primary, extra}, 100)
	require.NoError(t, err)
	require.Len(t, signed, 1)
	require.Equal(t, xdr.ScValTypeScvVec, signed[0].Credentials.Address.Signature.Type)
}

func TestSignAuthEntriesAsNoMatchingSignerFailsNamingAddress(t *testing.T) {
	required := keypair.MustRandom()
	other := keypair.MustRandom()
	addr, err := parseAddressToScAddress(required.Address())
	require.NoError(t, err)

	entry := fakeAuthEntry(xdr.SorobanCredentialsTypeSorobanCredentialsAddress, addr, 1)

	_, err = signAuthEntriesAs([]xdr.SorobanAuthorizationEntry{entry}, []*keypair.Full{other}, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), required.Address())
}

// executeSorobanOperationAs drives a live Soroban transaction (RPC sequence lookup, simulate,
// sign, submit, poll for confirmation) and is exercised end-to-end by a later Blend
// contract-wrapper task's container-backed integration run, which this package's plain unit tests
// (no Docker containers, no RPC) cannot reach. This method-value reference exists solely so static
// analysis sees it as intentionally-used infrastructure rather than dead code; it is never invoked
// from this file.
var _ = (*SharedContainers).executeSorobanOperationAs
