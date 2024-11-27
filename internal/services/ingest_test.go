package services

import (
	"context"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	t.Run("all_ledger_transactions_in_single_gettransactions_call", func(t *testing.T) {
		rpcGetTransactionsResult := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 2,
				},
			},
		}
		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 1, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.NoError(t, err)
	})

	t.Run("ledger_transactions_split_between_multiple_gettransactions_calls", func(t *testing.T) {
		rpcGetTransactionsResult1 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 1,
				},
			},
		}
		rpcGetTransactionsResult2 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash3",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash4",
					Ledger: 2,
				},
			},
		}

		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult1, nil).
			Once()

		mockRPCService.
			On("GetTransactions", int64(1), "51", 50).
			Return(rpcGetTransactionsResult2, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 3, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.Equal(t, txns[1].Hash, "hash2")
		assert.Equal(t, txns[2].Hash, "hash3")
		assert.NoError(t, err)
	})

}

func TestProcessTSSTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)

	t.Run("routes_to_tss_router", func(t *testing.T) {

		transactions := []entities.Transaction{
			{
				Status:              entities.SuccessStatus,
				Hash:                "feebumphash",
				ApplicationOrder:    1,
				FeeBump:             true,
				EnvelopeXDR:         "feebumpxdr",
				ResultXDR:           "AAAAAAAAAMj////9AAAAAA==",
				ResultMetaXDR:       "meta",
				Ledger:              123456,
				DiagnosticEventsXDR: "diag",
				CreatedAt:           1695939098,
			},
		}

		_ = tssStore.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = tssStore.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus}, tss.RPCTXCode{OtherCodes: tss.NewCode}, "")

		mockRouter.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return(nil).
			Once()

		err := ingestService.processTSSTransactions(context.Background(), transactions)
		assert.NoError(t, err)

		updatedTX, _ := tssStore.GetTransaction(context.Background(), "hash")
		assert.Equal(t, string(entities.SuccessStatus), updatedTX.Status)
		updatedTry, _ := tssStore.GetTry(context.Background(), "feebumphash")
		assert.Equal(t, "AAAAAAAAAMj////9AAAAAA==", updatedTry.ResultXDR)
		assert.Equal(t, int32(xdr.TransactionResultCodeTxTooLate), updatedTry.Code)
	})
}

func TestIngestPayments(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()
	usdIssuer := keypair.MustRandom().Address()
	eurIssuer := keypair.MustRandom().Address()

	t.Run("test_op_payment", func(t *testing.T) {
		_ = models.Account.Insert(context.Background(), srcAccount)
		paymentOp := txnbuild.Payment{
			SourceAccount: srcAccount,
			Destination:   destAccount,
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
		}
		transaction, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&paymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})

		txEnvXDR, _ := transaction.Base64()

		ledgerTransaction := entities.Transaction{
			Status:           entities.SuccessStatus,
			Hash:             "abcd",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           1,
		}

		ledgerTransactions := []entities.Transaction{ledgerTransaction}

		err := ingestService.ingestPayments(context.Background(), ledgerTransactions)
		assert.NoError(t, err)

		payments, _, _, err := models.Payments.GetPaymentsPaginated(context.Background(), srcAccount, "", "", data.ASC, 1)
		assert.NoError(t, err)
		assert.Equal(t, payments[0].TransactionHash, "abcd")
	})

	t.Run("test_op_path_payment_send", func(t *testing.T) {
		err := models.Account.Insert(context.Background(), srcAccount)
		require.NoError(t, err)

		path := []txnbuild.Asset{
			txnbuild.CreditAsset{Code: "USD", Issuer: usdIssuer},
			txnbuild.CreditAsset{Code: "EUR", Issuer: eurIssuer},
		}

		pathPaymentOp := txnbuild.PathPaymentStrictSend{
			SourceAccount: srcAccount,
			Destination:   destAccount,
			DestMin:       "9",
			SendAmount:    "10",
			SendAsset:     txnbuild.NativeAsset{},
			DestAsset:     txnbuild.NativeAsset{},
			Path:          path,
		}
		transaction, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&pathPaymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})
		require.NoError(t, err)

		signer := keypair.MustRandom()
		errSigner := models.Account.Insert(context.Background(), signer.Address())
		require.NoError(t, errSigner)

		signedTx, err := transaction.Sign(network.TestNetworkPassphrase, signer)
		require.NoError(t, err)

		txEnvXDR, err := signedTx.Base64()
		require.NoError(t, err)

		ledgerTransaction := entities.Transaction{
			Status:           entities.SuccessStatus,
			Hash:             "efgh",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAANAAAAAAAAAAAAAAAAXF0V022EgeGIo6QNVbMVvHdxvvl2MfZcVZUJpfph+0QAAAAAAAAAAAX14QAAAAAA",
			Ledger:           1,
		}

		ledgerTransactions := []entities.Transaction{ledgerTransaction}

		err = ingestService.ingestPayments(context.Background(), ledgerTransactions)
		require.NoError(t, err)

		payments, _, _, err := models.Payments.GetPaymentsPaginated(context.Background(), srcAccount, "", "", data.ASC, 1)
		require.NoError(t, err)
		require.NotEmpty(t, payments, "Expected at least one payment")
		assert.Equal(t, payments[0].TransactionHash, ledgerTransaction.Hash)
		assert.Equal(t, payments[0].SrcAmount, int64(100000000))
		assert.Equal(t, payments[0].SrcAssetType, xdr.AssetTypeAssetTypeNative.String())
		assert.Equal(t, payments[0].ToAddress, destAccount)
		assert.Equal(t, payments[0].FromAddress, srcAccount)
		assert.Equal(t, payments[0].SrcAssetCode, "XLM")
		assert.Equal(t, payments[0].DestAssetCode, "XLM")
	})

	t.Run("test_op_path_payment_receive", func(t *testing.T) {
		err := models.Account.Insert(context.Background(), srcAccount)
		require.NoError(t, err)

		path := []txnbuild.Asset{
			txnbuild.CreditAsset{Code: "USD", Issuer: usdIssuer},
			txnbuild.CreditAsset{Code: "EUR", Issuer: eurIssuer},
		}

		pathPaymentOp := txnbuild.PathPaymentStrictReceive{
			SourceAccount: srcAccount,
			Destination:   destAccount,
			SendMax:       "11",
			DestAmount:    "10",
			SendAsset:     txnbuild.NativeAsset{},
			DestAsset:     txnbuild.NativeAsset{},
			Path:          path,
		}
		transaction, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&pathPaymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})
		require.NoError(t, err)

		signer := keypair.MustRandom()
		errSigner := models.Account.Insert(context.Background(), signer.Address())
		require.NoError(t, errSigner)

		signedTx, err := transaction.Sign(network.TestNetworkPassphrase, signer)
		require.NoError(t, err)

		txEnvXDR, err := signedTx.Base64()
		require.NoError(t, err)

		ledgerTransaction := entities.Transaction{
			Status:           entities.SuccessStatus,
			Hash:             "efgh",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAAAAAAAjOiEfRh4kaFVQDu/CSTZLMtnyg0DbNowZ/G2nLES3KwAAAAAAAAAAAVdSoAAAAAA",
			Ledger:           1,
		}

		ledgerTransactions := []entities.Transaction{ledgerTransaction}

		err = ingestService.ingestPayments(context.Background(), ledgerTransactions)
		require.NoError(t, err)

		payments, _, _, err := models.Payments.GetPaymentsPaginated(context.Background(), srcAccount, "", "", data.ASC, 1)
		require.NoError(t, err)
		require.NotEmpty(t, payments, "Expected at least one payment")
		assert.Equal(t, payments[0].TransactionHash, ledgerTransaction.Hash)
		assert.Equal(t, payments[0].SrcAssetType, xdr.AssetTypeAssetTypeNative.String())
		assert.Equal(t, payments[0].ToAddress, destAccount)
		assert.Equal(t, payments[0].FromAddress, srcAccount)
		assert.Equal(t, payments[0].SrcAssetCode, "XLM")
		assert.Equal(t, payments[0].DestAssetCode, "XLM")
	})
}
