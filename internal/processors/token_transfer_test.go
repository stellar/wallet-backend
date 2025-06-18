package processors

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/ingest"
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
	// memoA    = uint64(123)
	// memoB    = uint64(234)
	// muxedAccountA, _ = xdr.MuxedAccountFromAccountId(accountA.Address(), memoA) //nolint:errcheck
	// muxedAccountB, _ = xdr.MuxedAccountFromAccountId(accountB.Address(), memoB) //nolint:errcheck

	oneUnit = xdr.Int64(1e7)

	// unitsToStr = func(v xdr.Int64) string {
	// 	return amount.String64Raw(v)
	// }

	usdcIssuer  = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	usdcAccount = xdr.MustMuxedAddress(usdcIssuer)
	usdcAsset   = xdr.MustNewCreditAsset("USDC", usdcIssuer)

	ethIssuer  = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	ethAccount = xdr.MustMuxedAddress(ethIssuer)
	ethAsset   = xdr.MustNewCreditAsset("ETH", ethIssuer)

	// btcIsuer   = "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN"
	// btcAccount = xdr.MustMuxedAddress(btcIsuer)
	// btcAsset   = xdr.MustNewCreditAsset("BTC", btcIsuer)

	// lpBtcEthId, _  = xdr.NewPoolId(btcAsset, ethAsset, xdr.LiquidityPoolFeeV18)  //nolint:errcheck
	// lpEthUsdcId, _ = xdr.NewPoolId(ethAsset, usdcAsset, xdr.LiquidityPoolFeeV18) //nolint:errcheck

	someBalanceId = xdr.ClaimableBalanceId{
		Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
		V0:   &xdr.Hash{1, 2, 3, 4, 5},
	}

	anotherBalanceId = xdr.ClaimableBalanceId{
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
		Type:    xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeClaimableBalance,
			ClaimableBalance: &xdr.LedgerKeyClaimableBalance{
				BalanceId: entry.Data.ClaimableBalance.BalanceId,
			},
		},
	}
}

func TestTokenTransferProcessor_ProcessTransaction(t *testing.T) {
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
	
	t.Run("extracts only fee event for failed txn", func(t *testing.T) {
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

	// Payment Operations Tests
	t.Run("Payment - extracts state changes for native XLM payment", func(t *testing.T) {
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

	t.Run("Payment - extracts state changes for custom asset payment", func(t *testing.T) {
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
	t.Run("Payment - extracts state changes for USDC mint (issuer to account)", func(t *testing.T) {
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

	t.Run("Payment - extracts state changes for ETH mint (issuer to account)", func(t *testing.T) {
		mintPaymentOp := xdr.Operation{
			SourceAccount: &ethAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountA,
					Asset:       ethAsset,
					Amount:      50 * oneUnit,
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
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "500000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, changes[1].Token)
	})

	// Burn Events - Payments TO issuer accounts
	t.Run("Payment - extracts state changes for USDC burn (account to issuer)", func(t *testing.T) {
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
		// Fee event + burn event
		require.Len(t, changes, 2)

		require.Equal(t, types.StateChangeCategoryDebit, changes[0].StateChangeCategory)
		require.Equal(t, someTxAccount.ToAccountId().Address(), changes[0].AccountID)
		require.Equal(t, sql.NullString{String: "100"}, changes[0].Amount)

		require.Equal(t, types.StateChangeCategoryBurn, changes[1].StateChangeCategory)
		require.Equal(t, accountA.ToAccountId().Address(), changes[1].AccountID)
		require.Equal(t, sql.NullString{String: "750000000"}, changes[1].Amount)
		require.Equal(t, sql.NullString{String: "USDC:" + usdcIssuer}, changes[1].Token)
	})

	// Claim Claimable Balance Tests
	t.Run("ClaimClaimableBalance - extracts state changes for claiming native XLM balance", func(t *testing.T) {
		claimOp := claimCBOp(someBalanceId, &accountA)
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
		cbEntry := cbLedgerEntry(someBalanceId, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 100*oneUnit)
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
		require.Equal(t, accountA.ToAccountId().Address(), stateChanges[1].AccountID)
		require.Equal(t, sql.NullString{String: "1000000000"}, stateChanges[1].Amount)
		require.Equal(t, sql.NullString{String: "native"}, stateChanges[1].Token)
		require.Equal(t, someBalanceId.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	t.Run("ClaimClaimableBalance - extracts state changes for claiming USDC balance", func(t *testing.T) {
		claimOp := claimCBOp(anotherBalanceId, &accountB)
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
		cbEntry := cbLedgerEntry(anotherBalanceId, usdcAsset, 50*oneUnit)
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
		require.Equal(t, anotherBalanceId.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})

	t.Run("ClaimClaimableBalance - multiple claims in single transaction", func(t *testing.T) {
		// First claim operation
		claimOp1 := claimCBOp(someBalanceId, &accountA)
		
		// Second claim operation - different account claiming different balance
		claimOp2 := claimCBOp(anotherBalanceId, &accountC)
		
		// Create a transaction with two operations
		tx := someTx
		tx.Envelope.V1.Tx.Operations = []xdr.Operation{claimOp1, claimOp2}
		
		// Create claimable balance entries
		cbEntry1 := cbLedgerEntry(someBalanceId, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 100*oneUnit)
		cbEntry2 := cbLedgerEntry(anotherBalanceId, ethAsset, 25*oneUnit)
		
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
		require.Equal(t, someBalanceId.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)

		require.Equal(t, types.StateChangeCategoryCredit, stateChanges[2].StateChangeCategory)
		require.Equal(t, accountC.ToAccountId().Address(), stateChanges[2].AccountID)
		require.Equal(t, sql.NullString{String: "250000000"}, stateChanges[2].Amount)
		require.Equal(t, sql.NullString{String: "ETH:" + ethIssuer}, stateChanges[2].Token)
		require.Equal(t, anotherBalanceId.MustEncodeToStrkey(), stateChanges[2].ClaimableBalanceID.String)
	})

	t.Run("ClaimClaimableBalance - no source account uses transaction source", func(t *testing.T) {
		// Create claim op without source account (nil source)
		claimOp := claimCBOp(someBalanceId, nil)
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
		cbEntry := cbLedgerEntry(someBalanceId, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 200*oneUnit)
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
		require.Equal(t, someBalanceId.MustEncodeToStrkey(), stateChanges[1].ClaimableBalanceID.String)
	})
}
