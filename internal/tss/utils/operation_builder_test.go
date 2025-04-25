package utils

import (
	"testing"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/utils"
)

func Test_BuildOperations(t *testing.T) {
	srcAccount := keypair.MustRandom().Address()
	dstAccount := keypair.MustRandom().Address()

	operations := []txnbuild.Operation{
		&txnbuild.CreateAccount{
			Destination:   dstAccount,
			Amount:        "10.0000000",
			SourceAccount: srcAccount,
		},
		&txnbuild.Payment{
			Destination:   dstAccount,
			Amount:        "10.0000000",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			OfferID:       int64(1234),
			SourceAccount: srcAccount,
			Price:         xdr.Price{N: 10, D: 10},
		},
		&txnbuild.CreatePassiveSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			Price:         xdr.Price{N: 10, D: 10},
			SourceAccount: srcAccount,
		},
		&txnbuild.SetOptions{
			SourceAccount: srcAccount,
		},
		&txnbuild.AccountMerge{
			Destination:   dstAccount,
			SourceAccount: srcAccount,
		},
		&txnbuild.Inflation{
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageData{
			Name:          "foo",
			SourceAccount: srcAccount,
		},
		&txnbuild.BumpSequence{
			BumpTo:        int64(100),
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageBuyOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			Price:         xdr.Price{N: 10, D: 10},
			OfferID:       int64(100),
			SourceAccount: srcAccount,
		},
		&txnbuild.PathPaymentStrictSend{
			SendAsset:     txnbuild.NativeAsset{},
			SendAmount:    "10.0000000",
			Destination:   dstAccount,
			DestAsset:     txnbuild.NativeAsset{},
			DestMin:       "1.0000000",
			Path:          []txnbuild.Asset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.CreateClaimableBalance{
			Amount:        "10.0000000",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: srcAccount,
		},
		&txnbuild.LiquidityPoolDeposit{
			SourceAccount: srcAccount,
			MaxAmountA:    "10.0000000",
			MaxAmountB:    "10.0000000",
			MinPrice:      xdr.Price{N: 10, D: 10},
			MaxPrice:      xdr.Price{N: 10, D: 10},
		},
		&txnbuild.LiquidityPoolWithdraw{
			SourceAccount: srcAccount,
			Amount:        "10.0000000",
			MinAmountA:    "10.0000000",
			MinAmountB:    "10.0000000",
		},
		&txnbuild.ExtendFootprintTtl{
			ExtendTo:      uint32(10),
			SourceAccount: srcAccount,
		},
		&txnbuild.RestoreFootprint{
			SourceAccount: srcAccount,
		},
	}

	opXDRs := make([]string, len(operations))
	for i, op := range operations {
		opXDR, err := op.BuildXDR()
		require.NoError(t, err)
		opXDRs[i], err = utils.OperationXDRToBase64(opXDR)
		require.NoError(t, err)
	}

	ops, err := BuildOperations(opXDRs)
	require.NoError(t, err)

	assert.Equal(t, operations, ops)
}

func Test_BuildOperations_EnforceSourceAccountForClassicOps(t *testing.T) {
	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)

	accountID, err := xdr.AddressToAccountId(keypair.MustRandom().Address())
	require.NoError(t, err)
	scAddress := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID,
	}

	testCases := []struct {
		name            string
		op              txnbuild.Operation
		wantErrContains string
	}{
		{
			name: "🔴fails_without_op_source_account",
			op: &txnbuild.Payment{
				Destination: keypair.MustRandom().Address(),
				Amount:      "10.0000000",
				Asset:       txnbuild.NativeAsset{},
			},
			wantErrContains: "all Stellar Classic operations must have a source account explicitly set",
		},
		{
			name: "🟢succeeds_with_op_source_account",
			op: &txnbuild.Payment{
				Destination:   keypair.MustRandom().Address(),
				Amount:        "10.0000000",
				Asset:         txnbuild.NativeAsset{},
				SourceAccount: keypair.MustRandom().Address(),
			},
		},
		{
			name: "🟡ignore_empty_source_account_in_Soroban_ops",
			op: &txnbuild.InvokeHostFunction{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: &nativeAssetContractID,
						},
						FunctionName: "transfer",
						Args: xdr.ScVec{
							{
								Type:    xdr.ScValTypeScvAddress,
								Address: &scAddress,
							},
							{
								Type:    xdr.ScValTypeScvAddress,
								Address: &scAddress,
							},
							{
								Type: xdr.ScValTypeScvI128,
								I128: &xdr.Int128Parts{
									Hi: xdr.Int64(0),
									Lo: xdr.Uint64(uint64(amount.MustParse("10"))),
								},
							},
						},
					},
				},
				SourceAccount: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opXDR, err := tc.op.BuildXDR()
			require.NoError(t, err)
			opXDRStr, err := utils.OperationXDRToBase64(opXDR)
			require.NoError(t, err)

			ops, err := BuildOperations([]string{opXDRStr})
			if tc.wantErrContains != "" {
				require.Nil(t, ops)
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.op, ops[0])
			}
		})
	}
}
