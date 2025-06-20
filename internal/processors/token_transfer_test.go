package processors

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	networkPassphrase = "Public Global Stellar Network ; September 2015"
	someTxAccount     = xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ")
	someTxHash        = xdr.Hash{1, 1, 1, 1}

	accountA = xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	accountB = xdr.MustMuxedAddress("GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z")
	accountC = xdr.MustMuxedAddress("GD4I7AFSLZGTDL34TQLWJOM2NHLIIOEKD5RHHZUW54HERBLSIRKUOXRR")

	oneUnit = xdr.Int64(1e7)

	usdcIssuer  = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	usdcAccount = xdr.MustMuxedAddress(usdcIssuer)
	usdcAsset   = xdr.MustNewCreditAsset("USDC", usdcIssuer)

	xlmAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeNative,
	}

	ethIssuer = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	ethAsset  = xdr.MustNewCreditAsset("ETH", ethIssuer)

	btcIssuer  = "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN"
	btcAccount = xdr.MustMuxedAddress(btcIssuer)
	btcAsset   = xdr.MustNewCreditAsset("BTC", btcIssuer)

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
)

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
						Ext: xdr.ExtensionPoint{
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

func generateAccountEntryChangState(accountEntry *xdr.AccountEntry) xdr.LedgerEntryChange {
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

// Helper function to create claim claimable balance operation
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

// Helper function to create claimable balance operation
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

// Helper function to create claimable balance ledger entry
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

// Helper function to generate claimable balance entry change state
func generateCBEntryChangeState(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &entry,
	}
}

// Helper function to generate claimable balance entry removed change
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

// Helper function to create clawback operation
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

// Helper function to create clawback claimable balance operation
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

// Helper function to create liquidity pool deposit operation
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

// Helper function to create liquidity pool withdraw operation
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

// Helper function to create liquidity pool ledger entry
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

// Helper function to generate liquidity pool entry created change
func generateLpEntryCreatedChange(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &entry,
	}
}

// Helper function to generate liquidity pool entry state change
func generateLpEntryChangeState(entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &entry,
	}
}

// Helper function to generate liquidity pool entry updated change

// Helper function to generate liquidity pool entry removed change
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

func lpIDToStrkey(lpID xdr.PoolId) string {
	return strkey.MustEncode(strkey.VersionByteLiquidityPool, lpID[:])
}

// Helper function to create manage buy offer operation
func manageBuyOfferOp(source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type:             xdr.OperationTypeManageBuyOffer,
			ManageBuyOfferOp: &xdr.ManageBuyOfferOp{},
		},
	}
}

// Helper function to create manage sell offer operation
func manageSellOfferOp(source *xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{
		SourceAccount: source,
		Body: xdr.OperationBody{
			Type:              xdr.OperationTypeManageSellOffer,
			ManageSellOfferOp: &xdr.ManageSellOfferOp{},
		},
	}
}

// Helper function to create path payment strict send operation
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

// Helper function to create path payment strict receive operation
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

func TestTokenTransferProcessor_Process(t *testing.T) {
	t.Run("Fee - extracts only fee event for failed txn", func(t *testing.T) {
		createAccountOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					Destination:     accountB.ToAccountId(),
					StartingBalance: 100 * oneUnit,
				},
			},
		}
		createAccountResult := &xdr.OperationResult{}
		tx := createTx(createAccountOp, nil, createAccountResult, true)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// We get only 1 fee event for txn source account
		require.Len(t, changes, 1)
		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)
	})
	t.Run("Fee - extracts credit state change for fee refund", func(t *testing.T) {
		tx := createSorobanTx(
			xdr.LedgerEntryChanges{
				// Fee is 300 units
				generateAccountEntryChangState(accountEntry(someTxAccount, 1000*oneUnit)),
				generateAccountEntryUpdatedChange(accountEntry(someTxAccount, 1000*oneUnit), 700*oneUnit),
			},
			// This is txApplyAfterChanges
			xdr.LedgerEntryChanges{
				// Refund is 30 units
				generateAccountEntryChangState(accountEntry(someTxAccount, 700*oneUnit)),
				generateAccountEntryUpdatedChange(accountEntry(someTxAccount, 700*oneUnit), 730*oneUnit),
			}, true)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Two fee events
		require.Len(t, changes, 2)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "3000000000"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryCredit, changes[1].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "300000000"}, changes[1].Amount)
	})

	t.Run("CreateAccount - extracts state changes for successful account creation", func(t *testing.T) {
		createAccountOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					Destination:     accountB.ToAccountId(),
					StartingBalance: 100 * oneUnit,
				},
			},
		}
		createAccountResult := &xdr.OperationResult{}
		tx := createTx(createAccountOp, nil, createAccountResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// We get 1 fee event for txn source account and 2 events for the account creation - 1 debit and 1 credit
		require.Len(t, changes, 3)
		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryDebit, changes[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[1].Amount)

		require.Equal(t, types.StateChangeCategoryCredit, changes[2].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), changes[2].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[2].Amount)
	})

	// Account Merge Events
	t.Run("AccountMerge - extracts state changes for successful account merge with balance", func(t *testing.T) {
		accountMergeOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &accountB,
			},
		}
		hundredUnits := 100 * oneUnit
		accountMergeResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAccountMerge,
				AccountMergeResult: &xdr.AccountMergeResult{
					Code:                 xdr.AccountMergeResultCodeAccountMergeSuccess,
					SourceAccountBalance: &hundredUnits,
				},
			},
		}
		tx := createTx(accountMergeOp, nil, accountMergeResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + transfer event for account merge
		require.Len(t, changes, 3)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryDebit, changes[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "native"}, changes[1].Token)

		require.Equal(t, types.StateChangeCategoryCredit, changes[2].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), changes[2].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[2].Amount)
		require.Equal(t, sql.NullString{String: "native"}, changes[2].Token)
	})

	t.Run("AccountMerge - no events for empty account merge", func(t *testing.T) {
		accountMergeOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &accountB,
			},
		}
		accountMergeResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAccountMerge,
				AccountMergeResult: &xdr.AccountMergeResult{
					Code:                 xdr.AccountMergeResultCodeAccountMergeSuccess,
					SourceAccountBalance: nil, // No balance to transfer
				},
			},
		}
		tx := createTx(accountMergeOp, nil, accountMergeResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Only fee event - no transfer event since no balance
		require.Len(t, changes, 1)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)
	})

	// Payment Operations Tests
	t.Run("Payment - G to G XLM transfer", func(t *testing.T) {
		paymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
					Amount:      50 * oneUnit,
				},
			},
		}
		paymentResult := &xdr.OperationResult{}
		tx := createTx(paymentOp, nil, paymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + debit + credit
		require.Len(t, changes, 3)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)

		require.Equal(t, types.StateChangeCategoryDebit, changes[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, changes[1].Amount)

		require.Equal(t, types.StateChangeCategoryCredit, changes[2].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), changes[2].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, changes[2].Amount)
	})

	t.Run("Payment - G to G USDC transfer", func(t *testing.T) {
		paymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       usdcAsset,
					Amount:      100 * oneUnit,
				},
			},
		}
		paymentResult := &xdr.OperationResult{}
		tx := createTx(paymentOp, nil, paymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + debit + credit
		require.Len(t, changes, 3)

		require.Equal(t, types.StateChangeCategoryDebit, changes[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[1].Token)

		require.Equal(t, types.StateChangeCategoryCredit, changes[2].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), changes[2].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[2].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[2].Token)
	})

	// Mint Events - Payments FROM issuer accounts
	t.Run("Payment - G (issuer) to G mints USDC", func(t *testing.T) {
		mintPaymentOp := xdr.Operation{
			SourceAccount: &usdcAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       usdcAsset,
					Amount:      100 * oneUnit,
				},
			},
		}
		mintPaymentResult := &xdr.OperationResult{}
		tx := createTx(mintPaymentOp, nil, mintPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + mint event
		require.Len(t, changes, 2)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryMint, changes[1].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[1].Token)
	})

	// Burn Events - Payments TO issuer accounts
	t.Run("Payment - G to G (issuer) burns USDC", func(t *testing.T) {
		burnPaymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: usdcAccount,
					Asset:       usdcAsset,
					Amount:      75 * oneUnit,
				},
			},
		}
		burnPaymentResult := &xdr.OperationResult{}
		tx := createTx(burnPaymentOp, nil, burnPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + burn event + debit event
		require.Len(t, changes, 3)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryBurn, changes[1].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "750000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[1].Token)

		require.Equal(t, types.StateChangeCategoryDebit, changes[2].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[2].AccountID)
		require.Equal(t, sql.NullString{String: "750000000"}, changes[2].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[2].Token)
	})

	// Claim Claimable Balance Tests
	t.Run("ClaimClaimableBalance - extracts state changes for claiming USDC balance", func(t *testing.T) {
		claimOp := claimCBOp(anotherBalanceID, &accountB)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		// Create USDC claimable balance entry
		cbEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 50*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + credit event for the claim
		require.Len(t, stateChanges, 2)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)
		require.Equal(t, anotherBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})
	t.Run("ClaimClaimableBalance - extracts state changes for claiming USDC balance by the issuer", func(t *testing.T) {
		claimOp := claimCBOp(anotherBalanceID, &usdcAccount)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		// Create USDC claimable balance entry
		cbEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 50*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + burn event for the claim
		require.Len(t, stateChanges, 2)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[1].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)
		require.Equal(t, anotherBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	t.Run("ClaimClaimableBalance - multiple claims in single transaction", func(t *testing.T) {
		// First claim operation
		claimOp1 := claimCBOp(someBalanceID, &accountA)

		// Second claim operation - different account claiming different balance
		claimOp2 := claimCBOp(anotherBalanceID, &accountC)

		// Create a transaction with two operations
		tx := someTx
		tx.Envelope.V1.Tx.Operations = []xdr.Operation{claimOp1, claimOp2}

		// Create claimable balance entries
		cbEntry1 := cbLedgerEntry(someBalanceID, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 100*oneUnit)
		cbEntry2 := cbLedgerEntry(anotherBalanceID, ethAsset, 25*oneUnit)

		// Set up meta for two operations
		tx.UnsafeMeta.V3.Operations = []xdr.OperationMeta{
			{
				Changes: xdr.LedgerEntryChanges{
					generateCBEntryChangeState(cbEntry1),
					generateCBEntryRemovedChange(cbEntry1),
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					generateCBEntryChangeState(cbEntry2),
					generateCBEntryRemovedChange(cbEntry2),
				},
			},
		}

		// Set up results for both operations
		tx.Result.Result.Result.Results = &[]xdr.OperationResult{
			{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypeClaimClaimableBalance,
					ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
						Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
					},
				},
			},
			{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypeClaimClaimableBalance,
					ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
						Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
					},
				},
			},
		}

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + 2 credit events
		require.Len(t, stateChanges, 3)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[1].Token)
		require.Equal(t, someBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountC.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "250000000"}, stateChanges[2].Amount)
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[2].Token)
		require.Equal(t, anotherBalanceID.MustEncodeToStrkey(), stateChanges[2].ClaimableBalanceID.String)
	})

	t.Run("ClaimClaimableBalance - no source account uses transaction source", func(t *testing.T) {
		// Create claim op without source account (nil source)
		claimOp := claimCBOp(someBalanceID, nil)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		// Create claimable balance entry
		cbEntry := cbLedgerEntry(someBalanceID, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 200*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + credit event for the claim
		require.Len(t, stateChanges, 2)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		// The claim should credit the transaction source account
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "2000000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[1].Token)
		require.Equal(t, someBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	// Create Claimable Balance Tests
	t.Run("CreateClaimableBalance - extracts state changes for creating USDC balance", func(t *testing.T) {
		// Create claimants
		claimants := []xdr.Claimant{
			{
				Type: xdr.ClaimantTypeClaimantTypeV0,
				V0: &xdr.ClaimantV0{
					Destination: accountB.ToAccountId(),
					Predicate: xdr.ClaimPredicate{
						Type: xdr.ClaimPredicateTypeClaimPredicateUnconditional,
					},
				},
			},
		}

		// Create the operation using helper function
		createOp := createCBOp(usdcAsset, 75*oneUnit, claimants, &accountA)

		// Create operation result with the claimable balance ID
		createResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateClaimableBalance,
				CreateClaimableBalanceResult: &xdr.CreateClaimableBalanceResult{
					Code:      xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess,
					BalanceId: &anotherBalanceID,
				},
			},
		}

		// Create the new claimable balance entry
		newCBEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 75*oneUnit)
		changes := xdr.LedgerEntryChanges{
			xdr.LedgerEntryChange{
				Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Created: &newCBEntry,
			},
		}

		tx := createTx(createOp, changes, createResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + debit event for the creation
		require.Len(t, stateChanges, 2)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "750000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)
		require.Equal(t, anotherBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	// Clawback Events Tests
	t.Run("Clawback - USDC clawback generates debit and burn events", func(t *testing.T) {
		clawbackOperation := clawbackOp(usdcAsset, 50*oneUnit, accountA, &usdcAccount)
		clawbackResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawback,
				ClawbackResult: &xdr.ClawbackResult{
					Code: xdr.ClawbackResultCodeClawbackSuccess,
				},
			},
		}
		tx := createTx(clawbackOperation, nil, clawbackResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + debit and burn events for the clawback
		require.Len(t, stateChanges, 3)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[1].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, stateChanges[2].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[2].Token)
	})

	t.Run("ClawbackClaimableBalance - extracts state changes for USDC claimable balance clawback", func(t *testing.T) {
		clawbackCBOperation := clawbackClaimableBalanceOp(someBalanceID, &usdcAccount)
		clawbackCBResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawbackClaimableBalance,
				ClawbackClaimableBalanceResult: &xdr.ClawbackClaimableBalanceResult{
					Code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess,
				},
			},
		}

		// Create USDC claimable balance entry that will be clawed back
		cbEntry := cbLedgerEntry(someBalanceID, usdcAsset, 25*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(clawbackCBOperation, changes, clawbackCBResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + burn event for the claimable balance clawback
		require.Len(t, stateChanges, 2)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[1].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "250000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)
		require.Equal(t, someBalanceID.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	// Liquidity Pool Events Tests
	t.Run("LiquidityPoolDeposit - extracts state changes for new LP creation with transfer events", func(t *testing.T) {
		lpDepositOperation := lpDepositOp(lpBtcEthID, 10*oneUnit, 20*oneUnit, 1, 10, &accountA)
		lpDepositResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolDeposit,
				LiquidityPoolDepositResult: &xdr.LiquidityPoolDepositResult{
					Code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess,
				},
			},
		}

		// Create LP entry with initial deposits
		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 15*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryCreatedChange(lpEntry),
		}

		tx := createTx(lpDepositOperation, changes, lpDepositResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + 2 transfer events (BTC and ETH to LP)
		require.Len(t, stateChanges, 3)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "BTC:" + btcIssuer}, stateChanges[1].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[1].LiquidityPoolID.String)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "150000000"}, stateChanges[2].Amount)
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[2].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[2].LiquidityPoolID.String)
	})
	t.Run("LiquidityPoolWithdraw - LP removal generates transfer events", func(t *testing.T) {
		lpWithdrawOperation := lpWithdrawOp(lpBtcEthID, 100*oneUnit, 2*oneUnit, 5*oneUnit, &accountA)
		lpWithdrawResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawResult: &xdr.LiquidityPoolWithdrawResult{
					Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess,
				},
			},
		}

		// Create LP entry state changes showing removal
		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 12*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryChangeState(lpEntry),
			generateLpEntryRemovedChange(lpBtcEthID),
		}

		tx := createTx(lpWithdrawOperation, changes, lpWithdrawResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + 2 credit events (all BTC and ETH from LP)
		require.Len(t, stateChanges, 3)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[1].Amount) // All 5 BTC
		require.Equal(t, sql.NullString{String: "BTC:" + btcIssuer}, stateChanges[1].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[1].LiquidityPoolID.String)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "120000000"}, stateChanges[2].Amount) // All 12 ETH
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[2].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[2].LiquidityPoolID.String)
	})
	t.Run("LiquidityPoolWithdraw - LP removal by asset issuer generates burn event", func(t *testing.T) {
		lpWithdrawOperation := lpWithdrawOp(lpBtcEthID, 100*oneUnit, 2*oneUnit, 5*oneUnit, &btcAccount)
		lpWithdrawResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawResult: &xdr.LiquidityPoolWithdrawResult{
					Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess,
				},
			},
		}

		// Create LP entry state changes showing removal
		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 12*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryChangeState(lpEntry),
			generateLpEntryRemovedChange(lpBtcEthID),
		}

		tx := createTx(lpWithdrawOperation, changes, lpWithdrawResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)
		// Fee event + burn event for the LP removal
		require.Len(t, stateChanges, 3)

		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)

		// Burn event for the LP removal since the asset issuer is withdrawing from the LP.
		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[1].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "BTC:" + btcIssuer}, stateChanges[1].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[1].LiquidityPoolID.String)

		// Credit event for the other asset.
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "120000000"}, stateChanges[2].Amount)
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[2].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[2].LiquidityPoolID.String)
	})

	// Path Payment Events Tests
	t.Run("PathPaymentStrictSend - A (BTC Issuer) sends BTC to B as USDC - 2 LP sweeps (BTC/ETH, ETH/USDC) - Mint and Transfer events", func(t *testing.T) {
		// Path: BTC -> ETH -> USDC (using ETH as intermediary)
		path := []xdr.Asset{ethAsset}

		pathPaymentOp := pathPaymentStrictSendOp(
			btcAsset,    // send asset (BTC)
			oneUnit,     // send amount (1 BTC)
			accountB,    // destination
			usdcAsset,   // destination asset (USDC)
			10*oneUnit,  // destination min (10 USDC)
			path,        // path through ETH
			&btcAccount, // source (BTC issuer)
		)

		pathPaymentResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
					Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
					Success: &xdr.PathPaymentStrictSendResultSuccess{
						Offers: []xdr.ClaimAtom{
							// source Account traded against the BtcEth Liquidity pool and acquired Eth and sold BTC(minted)
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpBtcEthID, ethAsset, 5*oneUnit, btcAsset, oneUnit),
							// source Account then traded against the EthUsdc pool and acquired USDC
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpEthUsdcID, usdcAsset, 10*oneUnit, ethAsset, 3*oneUnit),
						},
						Last: xdr.SimplePaymentResult{
							Destination: accountB.ToAccountId(),
							Asset:       usdcAsset,
							Amount:      10 * oneUnit, // Final amount received (10 USDC)
						},
					},
				},
			},
		}

		tx := createTx(pathPaymentOp, nil, pathPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)

		// Fee state change + 5 operation state changes = 6 total state changes
		// (LP transfers are now single state changes with LiquidityPoolID field)
		require.Len(t, stateChanges, 6)

		// Event 0: Fee event
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		// Event 1: Transfer ETH from LP-BTC-ETH to BTC issuer (Credit to BTC issuer)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[1].Amount) // 5 ETH
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[1].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[1].LiquidityPoolID.String)

		// Event 2: Transfer USDC from LP-ETH-USDC to BTC issuer (Credit to BTC issuer)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[2].Amount) // 10 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[2].Token)
		require.Equal(t, lpIDToStrkey(lpEthUsdcID), stateChanges[3].LiquidityPoolID.String)

		// Event 3: Transfer ETH from BTC issuer to LP-ETH-USDC (Debit from BTC issuer)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[3].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[3].AccountID)
		require.Equal(t, sql.NullString{String: "30000000"}, stateChanges[3].Amount) // 30 ETH
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[3].Token)
		require.Equal(t, lpIDToStrkey(lpEthUsdcID), stateChanges[3].LiquidityPoolID.String)

		// Event 4: Transfer USDC from BTC issuer to accountB (debit)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[4].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[4].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[4].Amount) // 10 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[4].Token)

		// Event 5: Transfer USDC from BTC issuer to accountB (credit)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[5].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), stateChanges[5].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[5].Amount) // 10 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[5].Token)
	})

	t.Run("PathPaymentStrictReceive - A (BTC Issuer) sends BTC to B (USDC Issuer) as USDC - 2 LP sweeps (BTC/ETH, ETH/USDC) - Mint, Transfer and Burn events", func(t *testing.T) {
		// Path: BTC -> ETH -> USDC (using ETH as intermediary)
		path := []xdr.Asset{ethAsset}

		pathPaymentOp := pathPaymentStrictReceiveOp(
			btcAsset,    // send asset (BTC)
			1*oneUnit,   // send max (1 BTC)
			usdcAccount, // destination (USDC issuer)
			usdcAsset,   // destination asset (USDC)
			6*oneUnit,   // destination amount (6 USDC exact)
			path,        // path through ETH
			&btcAccount, // source (BTC issuer)
		)

		pathPaymentResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Offers: []xdr.ClaimAtom{
							// BTC issuer traded against the BtcEth Liquidity pool, sold BTC (minted) and acquired ETH
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpBtcEthID, ethAsset, 2*oneUnit, btcAsset, 1*oneUnit),
							// BTC issuer then traded against the EthUsdc pool, sold ETH and acquired USDC
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpEthUsdcID, usdcAsset, 6*oneUnit, ethAsset, 2*oneUnit),
						},
						Last: xdr.SimplePaymentResult{
							Destination: usdcAccount.ToAccountId(),
							Asset:       usdcAsset,
							Amount:      6 * oneUnit, // Final amount received (6 USDC exact)
						},
					},
				},
			},
		}

		tx := createTx(pathPaymentOp, nil, pathPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)

		// Fee state change + 5 operation state changes = 5 total state changes
		// Since both source and destination are issuers, LP transfers generate single state changes
		// and final transfer is a burn event (to USDC issuer)
		require.Len(t, stateChanges, 6)

		// Event 0: Fee event
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		// Event 1: Transfer ETH from LP-BTC-ETH to BTC issuer (Credit to BTC issuer)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[1].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "20000000"}, stateChanges[1].Amount) // 2 ETH
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[1].Token)
		require.Equal(t, lpIDToStrkey(lpBtcEthID), stateChanges[1].LiquidityPoolID.String)

		// Event 2: Transfer USDC from LP-ETH-USDC to BTC issuer (Credit to BTC issuer)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "60000000"}, stateChanges[2].Amount) // 6 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[2].Token)
		require.Equal(t, lpIDToStrkey(lpEthUsdcID), stateChanges[2].LiquidityPoolID.String)

		// Event 3: Transfer ETH from BTC issuer to LP-ETH-USDC (Debit from BTC issuer)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[3].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[3].AccountID)
		require.Equal(t, sql.NullString{String: "20000000"}, stateChanges[3].Amount) // 2 ETH
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[3].Token)
		require.Equal(t, lpIDToStrkey(lpEthUsdcID), stateChanges[3].LiquidityPoolID.String)

		// Event 4: Burn event for USD Issuer
		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[4].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[4].AccountID)
		require.Equal(t, sql.NullString{String: "60000000"}, stateChanges[4].Amount) // 6 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[4].Token)

		// Event 5: Burn USDC from BTC issuer to USDC issuer (Burn event since destination is USDC issuer)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[5].StateChangeCategory)
		require.Equal(t, btcAccount.ToAccountId().Address(), stateChanges[5].AccountID)
		require.Equal(t, sql.NullString{String: "60000000"}, stateChanges[5].Amount) // 6 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[5].Token)
	})

	// Manage Offer Operations Tests
	t.Run("ManageBuyOffer - Buy USDC for XLM (Source is USDC issuer)", func(t *testing.T) {
		manageBuyOp := manageBuyOfferOp(&usdcAccount)

		manageBuyResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							// 1 USDC == 5 XLM
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountA, nil, usdcAsset, oneUnit, xlmAsset, 5*oneUnit),
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountB, nil, usdcAsset, 2*oneUnit, xlmAsset, 10*oneUnit),
						},
					},
				},
			},
		}

		tx := createTx(manageBuyOp, nil, manageBuyResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)

		// Fee event + 8 transfer events (2 trades × 4 transfers each: BURN + DEBIT + CREDIT)
		require.Len(t, stateChanges, 9)

		// Event 0: Fee event
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		// Event 1: BURN USDC from USDC issuer
		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[1].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "10000000"}, stateChanges[1].Amount) // 1 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[1].Token)

		// Event 2: DEBIT USDC from accountA
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "10000000"}, stateChanges[2].Amount) // 1 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[2].Token)

		// Event 3: DEBIT XLM from USDC issuer (5 XLM paid in first trade)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[3].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[3].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[3].Amount) // 5 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[3].Token)

		// Event 4: CREDIT XLM to accountA (5 XLM received from first trade)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[4].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[4].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[4].Amount) // 5 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[4].Token)

		// Event 5: BURN USDC from USDC issuer
		require.Equal(t, types.StateChangeCategoryBurn, stateChanges[5].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[5].AccountID)
		require.Equal(t, sql.NullString{String: "20000000"}, stateChanges[5].Amount) // 2 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[5].Token)

		// Event 6: DEBIT USDC from accountB
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[6].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), stateChanges[6].AccountID)
		require.Equal(t, sql.NullString{String: "20000000"}, stateChanges[6].Amount) // 2 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[6].Token)

		// Event 7: DEBIT XLM from USDC issuer (10 XLM paid in second trade)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[7].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[7].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[7].Amount) // 10 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[7].Token)

		// Event 8: CREDIT XLM to accountB (10 XLM received from second trade)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[8].StateChangeCategory)
		require.Equal(t, accountB.ToAccountId().Address(), stateChanges[8].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[8].Amount) // 10 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[8].Token)
	})

	t.Run("ManageSellOffer - Sell USDC for XLM (Source is USDC issuer)", func(t *testing.T) {
		manageSellOp := manageSellOfferOp(&usdcAccount)

		manageSellResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							// 1 USDC = 5 XLM
							// First trade: USDC issuer gives 1 USDC, gets 5 XLM from accountA
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountA, nil, xlmAsset, 5*oneUnit, usdcAsset, oneUnit),
							// Second trade: USDC issuer gives 2 USDC, gets 10 XLM from accountC
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountC, nil, xlmAsset, 10*oneUnit, usdcAsset, 2*oneUnit),
						},
					},
				},
			},
		}

		tx := createTx(manageSellOp, nil, manageSellResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges, err := processor.Process(context.Background(), tx)
		require.NoError(t, err)

		// Fee event + 6 transfer events (2 trades × 3 transfers each: DEBIT + CREDIT + MINT)
		require.Len(t, stateChanges, 7)

		// Event 0: Fee event
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), stateChanges[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, stateChanges[0].Amount)

		// Event 1: DEBIT XLM from accountA (5 XLM given to USDC issuer in first trade)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[1].Amount) // 5 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[1].Token)

		// Event 2: CREDIT XLM to USDC issuer (5 XLM received from first trade)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "50000000"}, stateChanges[2].Amount) // 5 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[2].Token)

		// Event 3: MINT USDC to accountA (1 USDC given in first trade - MINT because source is USDC issuer)
		require.Equal(t, types.StateChangeCategoryMint, stateChanges[3].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[3].AccountID)
		require.Equal(t, sql.NullString{String: "10000000"}, stateChanges[3].Amount) // 1 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[3].Token)

		// Event 4: DEBIT XLM from accountC (10 XLM given to USDC issuer in second trade)
		require.Equal(t, types.StateChangeCategoryDebit, stateChanges[4].StateChangeCategory)
		require.Equal(t, accountC.ToAccountId().Address(), stateChanges[4].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[4].Amount) // 10 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[4].Token)

		// Event 5: CREDIT XLM to USDC issuer (10 XLM received from second trade)
		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[5].StateChangeCategory)
		require.Equal(t, usdcAccount.ToAccountId().Address(), stateChanges[5].AccountID)
		require.Equal(t, sql.NullString{String: "100000000"}, stateChanges[5].Amount) // 10 XLM
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[5].Token)

		// Event 6: MINT USDC to accountC (2 USDC given in second trade - MINT because source is USDC issuer)
		require.Equal(t, types.StateChangeCategoryMint, stateChanges[6].StateChangeCategory)
		require.Equal(t, accountC.ToAccountId().Address(), stateChanges[6].AccountID)
		require.Equal(t, sql.NullString{String: "20000000"}, stateChanges[6].Amount) // 2 USDC
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, stateChanges[6].Token)
	})
}
