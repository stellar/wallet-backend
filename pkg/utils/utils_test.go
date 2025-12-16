package utils

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ConvertingObjectTypes_BetweenXDRAndTxnbuild(t *testing.T) {
	account1 := keypair.MustRandom().Address()
	account2 := keypair.MustRandom().Address()
	usdcAsset := txnbuild.CreditAsset{
		Code:   "USDC",
		Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}
	nativeAsset := txnbuild.NativeAsset{}

	liquidityPoolID, err := txnbuild.NewLiquidityPoolId(nativeAsset, usdcAsset)
	require.NoError(t, err)

	contractIDBytes, err := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	require.NoError(t, err)
	contractID := xdr.ContractId(contractIDBytes)

	testCases := []struct {
		name            string
		operation       txnbuild.Operation
		wantErrContains string
	}{
		{
			name: "create_account",
			operation: &txnbuild.CreateAccount{
				SourceAccount: account1,
				Destination:   account2,
				Amount:        "1.0000000",
			},
		},
		{
			name: "payment",
			operation: &txnbuild.Payment{
				SourceAccount: account1,
				Destination:   account2,
				Amount:        "1000000000.0000000",
				Asset:         usdcAsset,
			},
		},
		{
			name: "manage_sell_offer",
			operation: &txnbuild.ManageSellOffer{
				Selling: nativeAsset,
				Buying:  usdcAsset,
				Amount:  "10.0000000",
				OfferID: int64(1234),
				Price:   xdr.Price{N: 10, D: 10},
			},
		},
		{
			name: "create_passive_sell_offer",
			operation: &txnbuild.CreatePassiveSellOffer{
				Selling: nativeAsset,
				Buying:  usdcAsset,
				Amount:  "10.0000000",
				Price:   xdr.Price{N: 10, D: 10},
			},
		},
		{
			name: "set_options",
			operation: &txnbuild.SetOptions{
				SourceAccount: account1,
			},
		},
		{
			name: "account_merge",
			operation: &txnbuild.AccountMerge{
				Destination:   account2,
				SourceAccount: account1,
			},
		},
		{
			name: "inflation",
			operation: &txnbuild.Inflation{
				SourceAccount: account1,
			},
		},
		{
			name: "manage_data",
			operation: &txnbuild.ManageData{
				Name:          "foo",
				Value:         []byte("bar"),
				SourceAccount: account1,
			},
		},
		{
			name: "bump_sequence",
			operation: &txnbuild.BumpSequence{
				BumpTo:        int64(100),
				SourceAccount: account1,
			},
		},
		{
			name: "manage_buy_offer",
			operation: &txnbuild.ManageBuyOffer{
				Selling:       nativeAsset,
				Buying:        usdcAsset,
				Amount:        "10.0000000",
				Price:         xdr.Price{N: 10, D: 10},
				OfferID:       int64(1234),
				SourceAccount: account1,
			},
		},
		{
			name: "path_payment_strict_send",
			operation: &txnbuild.PathPaymentStrictSend{
				SendAsset:     nativeAsset,
				SendAmount:    "10.0000000",
				Destination:   account2,
				DestAsset:     usdcAsset,
				DestMin:       "1.0000000",
				Path:          []txnbuild.Asset{},
				SourceAccount: account1,
			},
		},
		{
			name: "path_payment_strict_receive",
			operation: &txnbuild.PathPaymentStrictReceive{
				SendAsset:     nativeAsset,
				DestAmount:    "1.0000000",
				Destination:   account2,
				DestAsset:     usdcAsset,
				SendMax:       "10.0000000",
				Path:          []txnbuild.Asset{},
				SourceAccount: account1,
			},
		},
		{
			name: "create_claimable_balance",
			operation: &txnbuild.CreateClaimableBalance{
				Amount:        "10.0000000",
				Asset:         usdcAsset,
				SourceAccount: account1,
				Destinations: []txnbuild.Claimant{
					{
						Destination: account2,
						Predicate: xdr.ClaimPredicate{
							Type: xdr.ClaimPredicateTypeClaimPredicateUnconditional,
						},
					},
				},
			},
		},
		{
			name: "claim_claimable_balance",
			operation: &txnbuild.ClaimClaimableBalance{
				BalanceID:     "00000000e35667f1a0678fefceec77c4cce0be5310c7a7fb10f6cd5172c257b50a29199c",
				SourceAccount: account2,
			},
		},
		{
			name: "change_trust",
			operation: &txnbuild.ChangeTrust{
				Line:          usdcAsset.MustToChangeTrustAsset(),
				Limit:         "1000.0000000",
				SourceAccount: account1,
			},
		},
		{
			name: "allow_trust",
			//nolint:staticcheck // this test is used to prevent the usage of the deprecated operation
			operation: &txnbuild.AllowTrust{
				Trustor:       account2,
				Type:          usdcAsset,
				Authorize:     true,
				SourceAccount: account1,
			},
			wantErrContains: "the allow trust operation is deprecated, use SetTrustLineFlags instead",
		},
		{
			name: "begin_sponsoring_future_reserves",
			operation: &txnbuild.BeginSponsoringFutureReserves{
				SourceAccount: account1,
				SponsoredID:   account2,
			},
		},
		{
			name: "end_sponsoring_future_reserves",
			operation: &txnbuild.EndSponsoringFutureReserves{
				SourceAccount: account1,
			},
		},
		{
			name: "revoke_sponsorship",
			operation: &txnbuild.RevokeSponsorship{
				SourceAccount:   account1,
				Account:         &account2,
				SponsorshipType: txnbuild.RevokeSponsorshipTypeAccount,
			},
		},
		{
			name: "clawback",
			operation: &txnbuild.Clawback{
				SourceAccount: account1,
				From:          account2,
				Asset:         usdcAsset,
				Amount:        "10.0000000",
			},
		},
		{
			name: "clawback_claimable_balance",
			operation: &txnbuild.ClawbackClaimableBalance{
				BalanceID:     "00000000e35667f1a0678fefceec77c4cce0be5310c7a7fb10f6cd5172c257b50a29199c",
				SourceAccount: account1,
			},
		},
		{
			name: "liquidity_pool_deposit",
			operation: &txnbuild.LiquidityPoolDeposit{
				LiquidityPoolID: liquidityPoolID,
				SourceAccount:   account1,
				MaxAmountA:      "10.0000000",
				MaxAmountB:      "10.0000000",
				MinPrice:        xdr.Price{N: 10, D: 10},
				MaxPrice:        xdr.Price{N: 100, D: 100},
			},
		},
		{
			name: "liquidity_pool_withdraw",
			operation: &txnbuild.LiquidityPoolWithdraw{
				SourceAccount:   account1,
				Amount:          "10.0000000",
				MinAmountA:      "9.0000000",
				MinAmountB:      "11.0000000",
				LiquidityPoolID: liquidityPoolID,
			},
		},
		{
			name: "contracts/extend_footprint_ttl",
			operation: &txnbuild.ExtendFootprintTtl{
				SourceAccount: account1,
				ExtendTo:      10,
			},
		},
		{
			name: "contracts/restore_footprint",
			operation: &txnbuild.RestoreFootprint{
				SourceAccount: account1,
			},
		},
		{
			name: "set_trustline_flags",
			operation: &txnbuild.SetTrustLineFlags{
				SourceAccount: account1,
				Trustor:       account2,
				Asset:         usdcAsset,
				SetFlags:      []txnbuild.TrustLineFlag{txnbuild.TrustLineAuthorized},
				ClearFlags:    []txnbuild.TrustLineFlag{txnbuild.TrustLineClawbackEnabled},
			},
		},
		{
			name: "invoke_host_function",
			operation: &txnbuild.InvokeHostFunction{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: &contractID,
						},
						FunctionName: "bulk_transfer",
						Args: xdr.ScVec{
							xdr.ScVal{
								Type: xdr.ScValTypeScvAddress,
								Address: &xdr.ScAddress{
									Type:      xdr.ScAddressTypeScAddressTypeAccount,
									AccountId: xdr.MustAddressPtr(account1),
								},
							},
							xdr.ScVal{
								Type: xdr.ScValTypeScvAddress,
								Address: &xdr.ScAddress{
									Type:       xdr.ScAddressTypeScAddressTypeContract,
									ContractId: &contractID,
								},
							},
						},
					},
				},
				SourceAccount: account1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// txnbuild -> xdr
			opXDR, err := tc.operation.BuildXDR()
			require.NoError(t, err)
			// xdr -> base64
			b64OpXDR, err := OperationXDRToBase64(opXDR)
			require.NoError(t, err)
			// base64 -> xdr
			opXDRFromB64, err := OperationXDRFromBase64(b64OpXDR)
			require.NoError(t, err)

			assert.Equal(t, opXDR, opXDRFromB64) // assert: xdr -> base64 -> xdr

			// xdr -> txnbuild
			txnBuildOp, err := OperationXDRToTxnBuildOp(opXDRFromB64)
			if tc.wantErrContains == "" {
				require.NoError(t, err)

				assert.Equal(t, tc.operation, txnBuildOp) // assert: txnbuild -> xdr -> base64 -> xdr -> txnbuild
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
			}
		})
	}
}

func Test_IsSorobanXDROp(t *testing.T) {
	nonSorobanTypes := []xdr.OperationType{
		xdr.OperationTypeCreateAccount,
		xdr.OperationTypePayment,
		xdr.OperationTypePathPaymentStrictReceive,
		xdr.OperationTypeManageSellOffer,
		xdr.OperationTypeCreatePassiveSellOffer,
		xdr.OperationTypeSetOptions,
		xdr.OperationTypeChangeTrust,
		xdr.OperationTypeAllowTrust,
		xdr.OperationTypeAccountMerge,
		xdr.OperationTypeInflation,
		xdr.OperationTypeManageData,
		xdr.OperationTypeBumpSequence,
		xdr.OperationTypeManageBuyOffer,
		xdr.OperationTypePathPaymentStrictSend,
		xdr.OperationTypeCreateClaimableBalance,
		xdr.OperationTypeClaimClaimableBalance,
		xdr.OperationTypeBeginSponsoringFutureReserves,
		xdr.OperationTypeEndSponsoringFutureReserves,
		xdr.OperationTypeRevokeSponsorship,
		xdr.OperationTypeClawback,
		xdr.OperationTypeClawbackClaimableBalance,
		xdr.OperationTypeSetTrustLineFlags,
		xdr.OperationTypeLiquidityPoolDeposit,
		xdr.OperationTypeLiquidityPoolWithdraw,
	}
	for _, opType := range nonSorobanTypes {
		t.Run("🔴"+opType.String(), func(t *testing.T) {
			op := xdr.Operation{
				Body: xdr.OperationBody{Type: opType},
			}
			assert.False(t, IsSorobanXDROp(op))
		})
	}

	sorobanTypes := []xdr.OperationType{
		xdr.OperationTypeInvokeHostFunction,
		xdr.OperationTypeExtendFootprintTtl,
		xdr.OperationTypeRestoreFootprint,
	}
	for _, opType := range sorobanTypes {
		t.Run("🟢"+opType.String(), func(t *testing.T) {
			op := xdr.Operation{
				Body: xdr.OperationBody{Type: opType},
			}
			assert.True(t, IsSorobanXDROp(op))
		})
	}
}

func Test_IsSorobanTxnbuildOp(t *testing.T) {
	nonSorobanOps := []txnbuild.Operation{
		&txnbuild.CreateAccount{},
		&txnbuild.Payment{},
		&txnbuild.PathPaymentStrictReceive{},
		&txnbuild.ManageSellOffer{},
		&txnbuild.CreatePassiveSellOffer{},
		&txnbuild.SetOptions{},
		&txnbuild.ChangeTrust{},
		//nolint:staticcheck // this is just a test, we know it's deprecated.
		&txnbuild.AllowTrust{},
		&txnbuild.AccountMerge{},
		&txnbuild.Inflation{},
		&txnbuild.ManageData{},
		&txnbuild.BumpSequence{},
		&txnbuild.ManageBuyOffer{},
		&txnbuild.PathPaymentStrictSend{},
		&txnbuild.CreateClaimableBalance{},
		&txnbuild.ClaimClaimableBalance{},
		&txnbuild.BeginSponsoringFutureReserves{},
		&txnbuild.EndSponsoringFutureReserves{},
		&txnbuild.RevokeSponsorship{},
		&txnbuild.Clawback{},
		&txnbuild.ClawbackClaimableBalance{},
		&txnbuild.SetTrustLineFlags{},
		&txnbuild.LiquidityPoolDeposit{},
		&txnbuild.LiquidityPoolWithdraw{},
	}
	for _, op := range nonSorobanOps {
		t.Run(fmt.Sprintf("🔴%T", op), func(t *testing.T) {
			assert.False(t, IsSorobanTxnbuildOp(op))
		})
	}

	sorobanOps := []txnbuild.Operation{
		&txnbuild.InvokeHostFunction{},
		&txnbuild.ExtendFootprintTtl{},
		&txnbuild.RestoreFootprint{},
	}
	for _, op := range sorobanOps {
		t.Run(fmt.Sprintf("🟢%T", op), func(t *testing.T) {
			assert.True(t, IsSorobanTxnbuildOp(op))
		})
	}
}
