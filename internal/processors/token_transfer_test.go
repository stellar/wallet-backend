package processors

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/amount"
	assetProto "github.com/stellar/go/asset"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	networkPassphrase     = "Public Global Stellar Network ; September 2015"
	someNetworkPassphrase = "some network passphrase"
	someTxAccount         = xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ")
	someTxHash            = xdr.Hash{1, 1, 1, 1}

	accountA         = xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	accountB         = xdr.MustMuxedAddress("GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z")
	memoA            = uint64(123)
	memoB            = uint64(234)
	muxedAccountA, _ = xdr.MuxedAccountFromAccountId(accountA.Address(), memoA)
	muxedAccountB, _ = xdr.MuxedAccountFromAccountId(accountB.Address(), memoB)

	oneUnit = xdr.Int64(1e7)

	unitsToStr = func(v xdr.Int64) string {
		return amount.String64Raw(v)
	}

	usdcIssuer     = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	usdcAccount    = xdr.MustMuxedAddress(usdcIssuer)
	usdcAsset      = xdr.MustNewCreditAsset("USDC", usdcIssuer)
	usdcProtoAsset = assetProto.NewProtoAsset(usdcAsset)

	ethIssuer     = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	ethAccount    = xdr.MustMuxedAddress(ethIssuer)
	ethAsset      = xdr.MustNewCreditAsset("ETH", ethIssuer)
	ethProtoAsset = assetProto.NewProtoAsset(ethAsset)

	btcIsuer      = "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN"
	btcAccount    = xdr.MustMuxedAddress(btcIsuer)
	btcAsset      = xdr.MustNewCreditAsset("BTC", btcIsuer)
	btcProtoAsset = assetProto.NewProtoAsset(btcAsset)

	lpBtcEthId, _  = xdr.NewPoolId(btcAsset, ethAsset, xdr.LiquidityPoolFeeV18)
	lpEthUsdcId, _ = xdr.NewPoolId(ethAsset, usdcAsset, xdr.LiquidityPoolFeeV18)

	someBalanceId = xdr.ClaimableBalanceId{
		Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
		V0:   &xdr.Hash{1, 2, 3, 4, 5},
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
		Index: 1,
		Ledger:        someLcm,
		Hash:          someTxHash,
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

func TestTokenTransferProcessor_ProcessTransaction(t *testing.T) {
	t.Run("extracts CreateAccount state changes for successful account creation", func(t *testing.T) {
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
		require.Equal(t, changes[0].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[0].AccountID, someTxAccount.ToAccountId().Address())
		require.Equal(t, changes[0].Amount, sql.NullString{String: "100"})

		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[1].AccountID, accountA.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "1000000000"})

		require.Equal(t, changes[2].StateChangeCategory, types.StateChangeCategoryCredit)
		require.Equal(t, changes[2].AccountID, accountB.ToAccountId().Address())
		require.Equal(t, changes[2].Amount, sql.NullString{String: "1000000000"})
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
		require.Equal(t, changes[0].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[0].AccountID, someTxAccount.ToAccountId().Address())
		require.Equal(t, changes[0].Amount, sql.NullString{String: "100"})
	})
	
	// Payment Operations Tests
	t.Run("extracts Payment state changes for native XLM payment", func(t *testing.T) {
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
		
		require.Equal(t, changes[0].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[0].AccountID, someTxAccount.ToAccountId().Address())
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[1].AccountID, accountA.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "500000000"})
		
		require.Equal(t, changes[2].StateChangeCategory, types.StateChangeCategoryCredit)
		require.Equal(t, changes[2].AccountID, accountB.ToAccountId().Address())
		require.Equal(t, changes[2].Amount, sql.NullString{String: "500000000"})
	})
	
	t.Run("extracts Payment state changes for custom asset payment", func(t *testing.T) {
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
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[1].AccountID, accountA.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "1000000000"})
		require.Equal(t, changes[1].Token, sql.NullString{String: "USDC:" + usdcIssuer})
		
		require.Equal(t, changes[2].StateChangeCategory, types.StateChangeCategoryCredit)
		require.Equal(t, changes[2].AccountID, accountB.ToAccountId().Address())
		require.Equal(t, changes[2].Amount, sql.NullString{String: "1000000000"})
		require.Equal(t, changes[2].Token, sql.NullString{String: "USDC:" + usdcIssuer})
	})

	// Mint Events - Payments FROM issuer accounts
	t.Run("extracts Payment state changes for USDC mint (issuer to account)", func(t *testing.T) {
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
		
		require.Equal(t, changes[0].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[0].AccountID, someTxAccount.ToAccountId().Address())
		require.Equal(t, changes[0].Amount, sql.NullString{String: "100"})
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryMint)
		require.Equal(t, changes[1].AccountID, accountB.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "1000000000"})
		require.Equal(t, changes[1].Token, sql.NullString{String: "USDC:" + usdcIssuer})
	})
	
	t.Run("extracts Payment state changes for ETH mint (issuer to account)", func(t *testing.T) {
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
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryMint)
		require.Equal(t, changes[1].AccountID, accountA.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "500000000"})
		require.Equal(t, changes[1].Token, sql.NullString{String: "ETH:" + ethIssuer})
	})
	
	// Burn Events - Payments TO issuer accounts
	t.Run("extracts Payment state changes for USDC burn (account to issuer)", func(t *testing.T) {
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
		
		require.Equal(t, changes[0].StateChangeCategory, types.StateChangeCategoryDebit)
		require.Equal(t, changes[0].AccountID, someTxAccount.ToAccountId().Address())
		require.Equal(t, changes[0].Amount, sql.NullString{String: "100"})
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryBurn)
		require.Equal(t, changes[1].AccountID, accountA.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "750000000"})
		require.Equal(t, changes[1].Token, sql.NullString{String: "USDC:" + usdcIssuer})
	})
	
	t.Run("extracts Payment state changes for BTC burn (account to issuer)", func(t *testing.T) {
		burnPaymentOp := xdr.Operation{
			SourceAccount: &accountB,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: btcAccount,
					Asset:       btcAsset,
					Amount:      25 * oneUnit,
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
		
		require.Equal(t, changes[1].StateChangeCategory, types.StateChangeCategoryBurn)
		require.Equal(t, changes[1].AccountID, accountB.ToAccountId().Address())
		require.Equal(t, changes[1].Amount, sql.NullString{String: "250000000"})
		require.Equal(t, changes[1].Token, sql.NullString{String: "BTC:" + btcIsuer})
	})

}
