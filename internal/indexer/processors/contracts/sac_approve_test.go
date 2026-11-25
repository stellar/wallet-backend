package contracts

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestSACApproveStateChanges(t *testing.T) {
	owner := keypair.MustRandom().Address()
	spender := keypair.MustRandom().Address()
	issuer := keypair.MustRandom().Address()
	native := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	credit := xdr.MustNewCreditAsset("USDC", issuer)
	builder := processors.NewStateChangeBuilder(100, 0, 1).WithOperationID(2)

	t.Run("native and credit assets produce allowance rows", func(t *testing.T) {
		events := []xdr.ContractEvent{
			sacApproveTestEvent(t, sacContractID(t, native), owner, spender, "native", approveData(5_000_000, 3_000_000)),
			sacApproveTestEvent(t, sacContractID(t, credit), owner, spender, "USDC:"+issuer, approveData(0, 0)),
		}

		got := SACApproveStateChanges(networkPassphrase, events, builder)
		require.Len(t, got, 2)

		grant := got[0]
		assert.Equal(t, types.StateChangeCategoryAllowance, grant.StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUpdate, grant.StateChangeReason)
		assert.Equal(t, owner, string(grant.AccountID))
		assert.Equal(t, spender, grant.SpenderAccountID.String())
		assert.Equal(t, sacContractStrkey(t, native), grant.TokenID.String())
		assert.Equal(t, "5000000", grant.Amount.String)
		assert.Equal(t, uint32(3_000_000), grant.KeyValue["live_until_ledger"])
		assert.Equal(t, int64(2), grant.OperationID)

		revoke := got[1]
		assert.Equal(t, sacContractStrkey(t, credit), revoke.TokenID.String())
		assert.Equal(t, "0", revoke.Amount.String)
		assert.Equal(t, uint32(0), revoke.KeyValue["live_until_ledger"])
	})

	t.Run("amounts above int64 keep full precision", func(t *testing.T) {
		data := scValVec(
			xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: 1, Lo: 0}},
			scValU32(10),
		)
		got := SACApproveStateChanges(networkPassphrase,
			[]xdr.ContractEvent{sacApproveTestEvent(t, sacContractID(t, native), owner, spender, "native", data)}, builder)
		require.Len(t, got, 1)
		assert.Equal(t, "18446744073709551616", got[0].Amount.String)
	})

	t.Run("skipped events", func(t *testing.T) {
		nativeID := sacContractID(t, native)
		valid := sacApproveTestEvent(t, nativeID, owner, spender, "native", approveData(1, 1))

		otherContract := valid
		otherID := xdr.ContractId{1, 2, 3}
		otherContract.ContractId = &otherID

		threeTopics := sacApproveTestEvent(t, nativeID, owner, spender, "native", approveData(1, 1))
		threeTopics.Body.V0.Topics = threeTopics.Body.V0.Topics[:3]

		otherFunction := sacApproveTestEvent(t, nativeID, owner, spender, "native", approveData(1, 1))
		otherFunction.Body.V0.Topics[0] = scValSymbol("transfer")

		diagnostic := valid
		diagnostic.Type = xdr.ContractEventTypeDiagnostic

		cases := map[string]xdr.ContractEvent{
			"emitted by a contract that is not the asset's SAC": otherContract,
			"asset string names a different asset":              sacApproveTestEvent(t, nativeID, owner, spender, "USDC:"+issuer, approveData(1, 1)),
			"asset string is malformed":                         sacApproveTestEvent(t, nativeID, owner, spender, "not an asset", approveData(1, 1)),
			"three topics":                                      threeTopics,
			"other function":                                    otherFunction,
			"diagnostic event":                                  diagnostic,
			"data is not a vector":                              sacApproveTestEvent(t, nativeID, owner, spender, "native", scValU32(1)),
			"data has one element":                              sacApproveTestEvent(t, nativeID, owner, spender, "native", scValVec(scValU32(1))),
			"amount is not i128":                                sacApproveTestEvent(t, nativeID, owner, spender, "native", scValVec(scValU32(1), scValU32(1))),
		}
		for name, event := range cases {
			t.Run(name, func(t *testing.T) {
				assert.Empty(t, SACApproveStateChanges(networkPassphrase, []xdr.ContractEvent{event}, builder))
			})
		}
	})
}

func sacContractID(t *testing.T, asset xdr.Asset) xdr.ContractId {
	t.Helper()
	id, err := asset.ContractID(networkPassphrase)
	require.NoError(t, err)
	return xdr.ContractId(id)
}

func sacContractStrkey(t *testing.T, asset xdr.Asset) string {
	t.Helper()
	id := sacContractID(t, asset)
	return strkey.MustEncode(strkey.VersionByteContract, id[:])
}

func sacApproveTestEvent(t *testing.T, contractID xdr.ContractId, owner, spender, asset string, data xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	assetStr := xdr.ScString(asset)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{
					scValSymbol("approve"),
					scValAccount(owner),
					scValAccount(spender),
					{Type: xdr.ScValTypeScvString, Str: &assetStr},
				},
				Data: data,
			},
		},
	}
}

func approveData(amount uint64, liveUntilLedger uint32) xdr.ScVal {
	return scValVec(
		xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Lo: xdr.Uint64(amount)}},
		scValU32(liveUntilLedger),
	)
}

func scValSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scValAccount(address string) xdr.ScVal {
	accountID := xdr.MustAddress(address)
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accountID}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func scValU32(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

func scValVec(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}
