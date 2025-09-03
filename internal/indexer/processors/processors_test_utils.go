// Token transfer processor test utilities and shared test data
// Contains helper functions and constants for testing token transfer processing

package processors

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/contractevents"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

// Test constants and shared data
var (
	networkPassphrase = "Public Global Stellar Network ; September 2015"
	someTxAccount     = xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ")
	someTxHash        = xdr.Hash{1, 1, 1, 1}

	accountA = xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	accountB = xdr.MustMuxedAddress("GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z")
	accountC = xdr.MustMuxedAddress("GD4I7AFSLZGTDL34TQLWJOM2NHLIIOEKD5RHHZUW54HERBLSIRKUOXRR")

	oneUnit = xdr.Int64(1e7)

	nativeContractAddress = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	usdcIssuer          = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	usdcContractAddress = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"
	usdcAccount         = xdr.MustMuxedAddress(usdcIssuer)
	usdcAsset           = xdr.MustNewCreditAsset("USDC", usdcIssuer)

	xlmAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeNative,
	}

	ethIssuer          = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	ethContractAddress = "CALRGFTIOIMM5475GTIAIX24SKD5HIVQV6CA2LWBESXTFZREDIP7WBB3"
	ethAsset           = xdr.MustNewCreditAsset("ETH", ethIssuer)

	btcIssuer          = "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN"
	btcContractAddress = "CBJLNMRJL7B5E2OZXZTRI3XIYLFCT4BKQNOIF4X4HZ3A3PZCB4XFV2CV"
	btcAccount         = xdr.MustMuxedAddress(btcIssuer)
	btcAsset           = xdr.MustNewCreditAsset("BTC", btcIssuer)

	lpBtcEthID, _  = xdr.NewPoolId(btcAsset, ethAsset, xdr.LiquidityPoolFeeV18)  //nolint:errcheck
	lpEthUsdcID, _ = xdr.NewPoolId(ethAsset, usdcAsset, xdr.LiquidityPoolFeeV18) //nolint:errcheck

	someBalanceID = xdr.ClaimableBalanceId{
		Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
		V0:   &xdr.Hash{1, 2, 3, 4, 5},
	}

	anotherBalanceID = xdr.ClaimableBalanceId{
		Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
		V0:   &xdr.Hash{6, 7, 8, 9, 10},
	}

	someLcm = xdr.LedgerCloseMeta{
		V: int32(0),
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerVersion: 20,
					LedgerSeq:     xdr.Uint32(12345),
					ScpValue:      xdr.StellarValue{CloseTime: xdr.TimePoint(12345 * 100)},
				},
			},
			TxSet:              xdr.TransactionSet{},
			TxProcessing:       nil,
			UpgradesProcessing: nil,
			ScpInfo:            nil,
		},
		V1: nil,
	}

	someTx = ingest.LedgerTransaction{
		Index:  1,
		Ledger: someLcm,
		Hash:   someTxHash,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: someTxAccount,
					SeqNum:        xdr.SequenceNumber(54321),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: someTxHash,
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(100),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}},
			},
		},
	}

	trustor    = xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY")
	setFlags   = xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	clearFlags = xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag | xdr.TrustLineFlagsAuthorizedFlag)

	creator           = xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H")
	created           = xdr.MustAddress("GCQZP3IU7XU6EJ63JZXKCQOYT2RNXN3HB5CNHENNUEUHSMA4VUJJJSEN")
	sponsor           = xdr.MustAddress("GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A")
	sponsor2          = xdr.MustAddress("GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX")
	createAccountMeta = &xdr.TransactionMeta{
		V: 1,
		V1: &xdr.TransactionMetaV1{
			TxChanges: xdr.LedgerEntryChanges{
				{
					Type: 3,
					State: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 0x39,
						Data: xdr.LedgerEntryData{
							Type: 0,
							Account: &xdr.AccountEntry{
								AccountId:     creator,
								Balance:       800152377009533292,
								SeqNum:        25,
								InflationDest: &creator,
								Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
							},
						},
					},
				},
				{
					Type: 1,
					Updated: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 0x39,
						Data: xdr.LedgerEntryData{
							Type: 0,
							Account: &xdr.AccountEntry{
								AccountId:     creator,
								Balance:       800152377009533292,
								SeqNum:        26,
								InflationDest: &creator,
							},
						},
						Ext: xdr.LedgerEntryExt{},
					},
				},
			},
			Operations: []xdr.OperationMeta{
				{
					Changes: xdr.LedgerEntryChanges{
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
							State: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:     creator,
										Balance:       800152367009533292,
										SeqNum:        26,
										InflationDest: &creator,
										Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor2,
									},
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
							Removed: &xdr.LedgerKey{
								Type: xdr.LedgerEntryTypeAccount,
								Account: &xdr.LedgerKeyAccount{
									AccountId: created,
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
							State: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:     creator,
										Balance:       800152367009533292,
										SeqNum:        26,
										InflationDest: &creator,
										Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor,
									},
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
							Updated: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:     creator,
										Balance:       800152367009533292,
										SeqNum:        26,
										InflationDest: &creator,
										Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor2,
									},
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
							State: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:     creator,
										Balance:       800152377009533292,
										SeqNum:        26,
										InflationDest: &creator,
										Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor,
									},
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
							Updated: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:     creator,
										Balance:       800152367009533292,
										SeqNum:        26,
										InflationDest: &creator,
										Thresholds:    xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor,
									},
								},
							},
						},
						{
							Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
							Created: &xdr.LedgerEntry{
								LastModifiedLedgerSeq: 0x39,
								Data: xdr.LedgerEntryData{
									Type: xdr.LedgerEntryTypeAccount,
									Account: &xdr.AccountEntry{
										AccountId:  created,
										Balance:    10000000000,
										SeqNum:     244813135872,
										Thresholds: xdr.Thresholds{0x1, 0x0, 0x0, 0x0},
									},
								},
								Ext: xdr.LedgerEntryExt{
									V: 1,
									V1: &xdr.LedgerEntryExtensionV1{
										SponsoringId: &sponsor,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	createAccountMetaB64, _ = xdr.MarshalBase64(createAccountMeta) //nolint:errcheck
)

type testTransaction struct {
	Index         uint32
	EnvelopeXDR   string
	ResultXDR     string
	FeeChangesXDR string
	MetaXDR       string
	Hash          string
}

func buildTransactionFromXDR(t *testing.T, txn testTransaction) ingest.LedgerTransaction {
	transaction := ingest.LedgerTransaction{
		Index:      txn.Index,
		Ledger:     someLcm,
		Envelope:   xdr.TransactionEnvelope{},
		Result:     xdr.TransactionResultPair{},
		FeeChanges: xdr.LedgerEntryChanges{},
		UnsafeMeta: xdr.TransactionMeta{},
	}

	tt := assert.New(t)

	err := xdr.SafeUnmarshalBase64(txn.EnvelopeXDR, &transaction.Envelope)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(txn.ResultXDR, &transaction.Result.Result)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(txn.MetaXDR, &transaction.UnsafeMeta)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(txn.FeeChangesXDR, &transaction.FeeChanges)
	tt.NoError(err)

	_, err = hex.Decode(transaction.Result.TransactionHash[:], []byte(txn.Hash))
	tt.NoError(err)

	return transaction
}

// Transaction creation helpers
func createSorobanTx(feeChanges xdr.LedgerEntryChanges, txApplyAfterChanges xdr.LedgerEntryChanges, isFailed bool) ingest.LedgerTransaction {
	resp := someTx
	resp.FeeChanges = feeChanges
	resp.Envelope = xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: someTxAccount,
				SeqNum:        xdr.SequenceNumber(54321),
				Ext: xdr.TransactionExt{
					V: 1,
					SorobanData: &xdr.SorobanTransactionData{
						Ext: xdr.SorobanTransactionDataExt{
							V: 0,
						},
						Resources: xdr.SorobanResources{
							Footprint: xdr.LedgerFootprint{
								ReadOnly:  []xdr.LedgerKey{},
								ReadWrite: []xdr.LedgerKey{},
							},
						},
						ResourceFee: 100,
					},
				},
			},
		},
	}
	resp.UnsafeMeta = xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			Operations:     []xdr.OperationMeta{{}},
			TxChangesAfter: txApplyAfterChanges,
		},
	}

	if isFailed {
		resp.Result.Result.Result.Code = xdr.TransactionResultCodeTxFailed
	} else {
		resp.Result.Result.Result.Code = xdr.TransactionResultCodeTxSuccess
	}

	return resp
}

// makeInvocationTransaction returns a single transaction containing a single
// invokeHostFunction operation that generates the specified Stellar Asset
// Contract events in its txmeta.
func createInvocationTx(
	from, to, admin string,
	asset xdr.Asset,
	amount *big.Int,
	opResult *xdr.OperationResult,
	types ...contractevents.EventType,
) ingest.LedgerTransaction {
	meta := xdr.TransactionMetaV3{
		// irrelevant for contract invocations: only events are inspected
		Operations: []xdr.OperationMeta{},
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: make([]xdr.ContractEvent, len(types)),
		},
	}

	for idx, type_ := range types {
		event := contractevents.GenerateEvent(
			type_,
			from, to, admin,
			asset,
			amount,
			networkPassphrase,
		)
		meta.SorobanMeta.Events[idx] = event
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	resp := ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 3, V3: &meta},
	}
	if opResult != nil {
		resp.Result.Result.Result.Results = &[]xdr.OperationResult{*opResult}
	}
	return resp
}

func createTx(op xdr.Operation, changes xdr.LedgerEntryChanges, opResult *xdr.OperationResult, isFailed bool) ingest.LedgerTransaction {
	resp := someTx

	if isFailed {
		resp.Result.Result.Result.Code = xdr.TransactionResultCodeTxFailed
	} else {
		resp.Result.Result.Result.Code = xdr.TransactionResultCodeTxSuccess
	}

	resp.Envelope.V1.Tx.Operations = []xdr.Operation{op}
	if changes != nil {
		resp.UnsafeMeta.V3.Operations = []xdr.OperationMeta{{
			Changes: changes,
		}}
	}

	if opResult != nil {
		resp.Result.Result.Result.Results = &[]xdr.OperationResult{*opResult}
	}
	return resp
}

// Account entry helpers
func generateAccountEntryChangeState(accountEntry *xdr.AccountEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: accountEntry,
			},
		},
	}
}

func generateAccountEntryUpdatedChange(accountEntry *xdr.AccountEntry, newBalance xdr.Int64) xdr.LedgerEntryChange {
	accountEntry.Balance = newBalance
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: accountEntry,
			},
		},
	}
}

func accountEntry(acc xdr.MuxedAccount, balance xdr.Int64) *xdr.AccountEntry {
	return &xdr.AccountEntry{
		AccountId: acc.ToAccountId(),
		Balance:   balance,
		SeqNum:    xdr.SequenceNumber(12345),
	}
}

// Claimable balance operation helpers
func claimCBOp(balanceID xdr.ClaimableBalanceId, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeClaimClaimableBalance,
			ClaimClaimableBalanceOp: &xdr.ClaimClaimableBalanceOp{
				BalanceId: balanceID,
			},
		},
	}
}

func setTrustlineFlagsOp() xdr.Operation {
	aid := xdr.MustAddress("GDRW375MAYR46ODGF2WGANQC2RRZL7O246DYHHCGWTV2RE7IHE2QUQLD")
	source := aid.ToMuxedAccount()
	return xdr.Operation{
		SourceAccount: &source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeSetTrustLineFlags,
			SetTrustLineFlagsOp: &xdr.SetTrustLineFlagsOp{
				Trustor:    trustor,
				Asset:      xdr.MustNewCreditAsset("USD", "GDRW375MAYR46ODGF2WGANQC2RRZL7O246DYHHCGWTV2RE7IHE2QUQLD"),
				ClearFlags: clearFlags,
				SetFlags:   setFlags,
			},
		},
	}
}

func createCBOp(asset xdr.Asset, amount xdr.Int64, claimants []xdr.Claimant, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeCreateClaimableBalance,
			CreateClaimableBalanceOp: &xdr.CreateClaimableBalanceOp{
				Asset:     asset,
				Amount:    amount,
				Claimants: claimants,
			},
		},
	}
}

func cbLedgerEntry(id xdr.ClaimableBalanceId, asset xdr.Asset, amount xdr.Int64) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeClaimableBalance,
			ClaimableBalance: &xdr.ClaimableBalanceEntry{
				BalanceId: id,
				Asset:     asset,
				Amount:    amount,
			},
		},
	}
}

func generateCBEntryChangeState(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &entry,
	}
}

func generateCBEntryRemovedChange(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeClaimableBalance,
			ClaimableBalance: &xdr.LedgerKeyClaimableBalance{
				BalanceId: entry.Data.ClaimableBalance.BalanceId,
			},
		},
	}
}

// Clawback operation helpers
func clawbackOp(asset xdr.Asset, amount xdr.Int64, from xdr.MuxedAccount, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeClawback,
			ClawbackOp: &xdr.ClawbackOp{
				Asset:  asset,
				From:   from,
				Amount: amount,
			},
		},
	}
}

func clawbackClaimableBalanceOp(balanceID xdr.ClaimableBalanceId, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeClawbackClaimableBalance,
			ClawbackClaimableBalanceOp: &xdr.ClawbackClaimableBalanceOp{
				BalanceId: balanceID,
			},
		},
	}
}

// Liquidity pool operation helpers
func lpDepositOp(poolID xdr.PoolId, maxAmountA, maxAmountB, minPrice, maxPrice xdr.Int64, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeLiquidityPoolDeposit,
			LiquidityPoolDepositOp: &xdr.LiquidityPoolDepositOp{
				LiquidityPoolId: poolID,
				MaxAmountA:      maxAmountA,
				MaxAmountB:      maxAmountB,
				MinPrice:        xdr.Price{N: xdr.Int32(minPrice), D: 1},
				MaxPrice:        xdr.Price{N: xdr.Int32(maxPrice), D: 1},
			},
		},
	}
}

func lpWithdrawOp(poolID xdr.PoolId, amount, minAmountA, minAmountB xdr.Int64, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeLiquidityPoolWithdraw,
			LiquidityPoolWithdrawOp: &xdr.LiquidityPoolWithdrawOp{
				LiquidityPoolId: poolID,
				Amount:          amount,
				MinAmountA:      minAmountA,
				MinAmountB:      minAmountB,
			},
		},
	}
}

func lpLedgerEntry(poolID xdr.PoolId, assetA, assetB xdr.Asset, reserveA, reserveB xdr.Int64) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LiquidityPoolEntry{
				LiquidityPoolId: poolID,
				Body: xdr.LiquidityPoolEntryBody{
					Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
					ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
						Params: xdr.LiquidityPoolConstantProductParameters{
							AssetA: assetA,
							AssetB: assetB,
							Fee:    xdr.LiquidityPoolFeeV18,
						},
						ReserveA:                 reserveA,
						ReserveB:                 reserveB,
						TotalPoolShares:          xdr.Int64(1000000000),
						PoolSharesTrustLineCount: 1,
					},
				},
			},
		},
	}
}

func generateLpEntryCreatedChange(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &entry,
	}
}

func generateLpEntryChangeState(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &entry,
	}
}

func generateLpEntryRemovedChange(poolID xdr.PoolId) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LedgerKeyLiquidityPool{
				LiquidityPoolId: poolID,
			},
		},
	}
}

// Manage offer operation helpers
func manageBuyOfferOp(source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type:             xdr.OperationTypeManageBuyOffer,
			ManageBuyOfferOp: &xdr.ManageBuyOfferOp{},
		},
	}
}

func manageSellOfferOp(source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type:              xdr.OperationTypeManageSellOffer,
			ManageSellOfferOp: &xdr.ManageSellOfferOp{},
		},
	}
}

// Path payment operation helpers
func pathPaymentStrictSendOp(sendAsset xdr.Asset, sendAmount xdr.Int64, destination xdr.MuxedAccount, destAsset xdr.Asset, destMin xdr.Int64, path []xdr.Asset, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePathPaymentStrictSend,
			PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{
				SendAsset:   sendAsset,
				SendAmount:  sendAmount,
				Destination: destination,
				DestAsset:   destAsset,
				DestMin:     destMin,
				Path:        path,
			},
		},
	}
}

func pathPaymentStrictReceiveOp(sendAsset xdr.Asset, sendMax xdr.Int64, destination xdr.MuxedAccount, destAsset xdr.Asset, destAmount xdr.Int64, path []xdr.Asset, source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePathPaymentStrictReceive,
			PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
				SendAsset:   sendAsset,
				SendMax:     sendMax,
				Destination: destination,
				DestAsset:   destAsset,
				DestAmount:  destAmount,
				Path:        path,
			},
		},
	}
}

func generateClaimAtom(claimAtomType xdr.ClaimAtomType, sellerID *xdr.MuxedAccount, lpID *xdr.PoolId, assetSold xdr.Asset, amountSold xdr.Int64, assetBought xdr.Asset, amountBought xdr.Int64) xdr.ClaimAtom {
	claimAtom := xdr.ClaimAtom{
		Type: claimAtomType,
	}

	switch claimAtomType {
	case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
		claimAtom.LiquidityPool = &xdr.ClaimLiquidityAtom{
			LiquidityPoolId: *lpID,
			AssetBought:     assetBought,
			AmountBought:    amountBought,
			AssetSold:       assetSold,
			AmountSold:      amountSold,
		}
	case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
		claimAtom.OrderBook = &xdr.ClaimOfferAtom{
			SellerId:     sellerID.ToAccountId(),
			AssetBought:  assetBought,
			AmountBought: amountBought,
			AssetSold:    assetSold,
			AmountSold:   amountSold,
		}
	case xdr.ClaimAtomTypeClaimAtomTypeV0:
		// V0 claim atoms are not supported in this test helper
		panic("ClaimAtomTypeV0 is not supported")
	}
	return claimAtom
}

// Test helper functions
func processTransaction(t *testing.T, processor *TokenTransferProcessor, tx ingest.LedgerTransaction) []types.StateChange {
	t.Helper()
	changes, err := processor.ProcessTransaction(context.Background(), tx)
	require.NoError(t, err)
	return changes
}

func requireEventCount(t *testing.T, changes []types.StateChange, expectedCount int) {
	t.Helper()
	require.Len(t, changes, expectedCount)
}

// Base assertion function that handles common checks
func assertStateChangeBase(t *testing.T, change types.StateChange, category types.StateChangeCategory, expectedAccount string, expectedAmount string, expectedToken string) {
	t.Helper()
	require.Equal(t, category, change.StateChangeCategory)
	require.Equal(t, expectedAccount, change.AccountID)
	if expectedAmount != "" {
		require.Equal(t, utils.SQLNullString(expectedAmount), change.Amount)
	}
	if expectedToken != "" {
		require.Equal(t, utils.SQLNullString(expectedToken), change.TokenID)
	}
	if change.OperationID != 0 {
		require.Equal(t, change.OperationID, change.ToID)
	}
}

// Assertion helpers for common patterns
func assertFeeEvent(t *testing.T, change types.StateChange, expectedAmount string) {
	t.Helper()
	assertStateChangeBase(t, change, types.StateChangeCategoryBalance, someTxAccount.ToAccountId().Address(), expectedAmount, nativeContractAddress)
	assert.Equal(t, types.StateChangeReasonDebit, *change.StateChangeReason)
	assert.Equal(t, change.TxID, change.ToID)
}

func assertDebitEvent(t *testing.T, change types.StateChange, expectedAccount string, expectedAmount string, expectedToken string) {
	t.Helper()
	assertStateChangeBase(t, change, types.StateChangeCategoryBalance, expectedAccount, expectedAmount, expectedToken)
	assert.Equal(t, types.StateChangeReasonDebit, *change.StateChangeReason)
}

func assertCreditEvent(t *testing.T, change types.StateChange, expectedAccount string, expectedAmount string, expectedToken string) {
	t.Helper()
	assertStateChangeBase(t, change, types.StateChangeCategoryBalance, expectedAccount, expectedAmount, expectedToken)
	assert.Equal(t, types.StateChangeReasonCredit, *change.StateChangeReason)
}

func assertMintEvent(t *testing.T, change types.StateChange, expectedAccount string, expectedAmount string, expectedToken string) {
	t.Helper()
	assertStateChangeBase(t, change, types.StateChangeCategoryBalance, expectedAccount, expectedAmount, expectedToken)
	assert.Equal(t, types.StateChangeReasonMint, *change.StateChangeReason)
}

func assertBurnEvent(t *testing.T, change types.StateChange, expectedAccount string, expectedAmount string, expectedToken string) {
	t.Helper()
	assertStateChangeBase(t, change, types.StateChangeCategoryBalance, expectedAccount, expectedAmount, expectedToken)
	assert.Equal(t, types.StateChangeReasonBurn, *change.StateChangeReason)
}

func assertContractEvent(t *testing.T, change types.StateChange, category types.StateChangeCategory, reason types.StateChangeReason, expectedAccount string, expectedAmount string, expectedContractID string) {
	t.Helper()
	assertStateChangeBase(t, change, category, expectedAccount, expectedAmount, expectedContractID)
	require.Equal(t, reason, *change.StateChangeReason)
}
