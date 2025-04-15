package services

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/tss"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
)

func TestGetLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool, mockMetricsService)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore, mockMetricsService)
	t.Run("all_ledger_transactions_in_single_gettransactions_call", func(t *testing.T) {
		defer mockMetricsService.AssertExpectations(t)

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
		defer mockMetricsService.AssertExpectations(t)

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

	mockMetricsService := metrics.NewMockMetricsService()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool, mockMetricsService)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore, mockMetricsService)

	t.Run("routes_to_tss_router", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Times(2)
		mockMetricsService.On("RecordTSSTransactionStatusTransition", "NEW", "SUCCESS").Once()
		mockMetricsService.On("ObserveTSSTransactionInclusionTime", "SUCCESS", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("SetNumTssTransactionsIngestedPerLedger", "SUCCESS", float64(1)).Once()
		defer mockMetricsService.AssertExpectations(t)

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
				DiagnosticEventsXDR: []string{"diag"},
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

	mockMetricsService := metrics.NewMockMetricsService()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool, mockMetricsService)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore, mockMetricsService)
	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()
	usdIssuer := keypair.MustRandom().Address()
	eurIssuer := keypair.MustRandom().Address()

	t.Run("test_op_payment", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_payments", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "ingest_payments").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_payments", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "SELECT", "ingest_payments").Times(2)
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 1).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 0).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 0).Once()
		defer mockMetricsService.AssertExpectations(t)

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

		err = ingestService.ingestPayments(context.Background(), ledgerTransactions)
		require.NoError(t, err)

		var payments []data.Payment
		payments, _, _, err = models.Payments.GetPaymentsPaginated(context.Background(), srcAccount, "", "", data.ASC, 1)
		assert.NoError(t, err)
		assert.Equal(t, payments[0].TransactionHash, "abcd")
	})

	t.Run("test_op_path_payment_send", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_payments", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "ingest_payments").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_payments", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "SELECT", "ingest_payments").Times(2)
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 0).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 1).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 0).Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = models.Account.Insert(context.Background(), srcAccount)

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
		transaction, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&pathPaymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})

		signer := keypair.MustRandom()
		_ = models.Account.Insert(context.Background(), signer.Address())

		signedTx, _ := transaction.Sign(network.TestNetworkPassphrase, signer)

		txEnvXDR, _ := signedTx.Base64()

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

		var payments []data.Payment
		payments, _, _, err = models.Payments.GetPaymentsPaginated(context.Background(), srcAccount, "", "", data.ASC, 1)
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
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_payments", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "ingest_payments").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_payments", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "SELECT", "ingest_payments").Times(2)
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 0).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 0).Once()
		mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 1).Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = models.Account.Insert(context.Background(), srcAccount)

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
		transaction, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&pathPaymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})

		signer := keypair.MustRandom()
		_ = models.Account.Insert(context.Background(), signer.Address())

		signedTx, _ := transaction.Sign(network.TestNetworkPassphrase, signer)

		txEnvXDR, _ := signedTx.Base64()

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

func TestIngest_LatestSyncedLedgerBehindRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		_ = dbConnectionPool.Close()
		dbt.Close()
	}()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_payments", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "ingest_payments").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "ingest_store").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "ingest_store").Once()
	mockMetricsService.On("SetLatestLedgerIngested", float64(50)).Once()
	mockMetricsService.On("ObserveIngestionDuration", "payment", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("ObserveIngestionDuration", "tss", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("ObserveIngestionDuration", "total", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 1).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 0).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 0).Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}

	tssStore, err := tssstore.NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore, mockMetricsService)
	require.NoError(t, err)

	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()

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
	mockResult := entities.RPCGetTransactionsResult{
		Transactions: []entities.Transaction{{
			Status:           entities.SuccessStatus,
			Hash:             "abcd",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           50,
		}, {
			Status:           entities.SuccessStatus,
			Hash:             "abcd",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           51,
		}},
		LatestLedger:          int64(100),
		LatestLedgerCloseTime: int64(1),
		OldestLedger:          int64(50),
		OldestLedgerCloseTime: int64(1),
	}
	mockRPCService.On("GetTransactions", int64(50), "", 50).Return(mockResult, nil).Once()
	heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
	heartbeatChan <- entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 100,
		OldestLedger: 50,
	}
	mockRPCService.On("GetHeartbeatChannel").Return(heartbeatChan)

	err = ingestService.Run(ctx, uint32(49), uint32(50))
	require.NoError(t, err)

	mockRPCService.AssertNotCalled(t, "GetTransactions", int64(49), "", int64(50))
	mockRPCService.AssertExpectations(t)

	ledger, err := models.Payments.GetLatestLedgerSynced(context.Background(), "ingestionLedger")
	require.NoError(t, err)
	assert.Equal(t, uint32(50), ledger)
}

func TestIngest_LatestSyncedLedgerAheadOfRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		_ = dbConnectionPool.Close()
		dbt.Close()
		log.DefaultLogger.SetOutput(os.Stderr)
	}()

	mockMetricsService := metrics.NewMockMetricsService()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}

	tssStore, err := tssstore.NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore, mockMetricsService)
	require.NoError(t, err)

	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "ingest_store").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "ingest_store").Once()
	mockMetricsService.On("SetLatestLedgerIngested", float64(100)).Once()
	mockMetricsService.On("ObserveIngestionDuration", "payment", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("ObserveIngestionDuration", "tss", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("ObserveIngestionDuration", "total", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 0).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 0).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 0).Once()
	defer mockMetricsService.AssertExpectations(t)

	heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
	mockRPCService.On("GetHeartbeatChannel").Return(heartbeatChan)

	// Send first heartbeat showing RPC is behind
	heartbeatChan <- entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 50,
		OldestLedger: 1,
	}

	// After a delay, send second heartbeat showing RPC has caught up
	go func() {
		time.Sleep(6 * time.Second) // Sleep longer than the service's 5 second wait
		heartbeatChan <- entities.RPCGetHealthResult{
			Status:       "healthy",
			LatestLedger: 100,
			OldestLedger: 50,
		}
	}()

	// Capture debug logs to verify waiting message
	var logBuffer bytes.Buffer
	log.DefaultLogger.SetOutput(&logBuffer)
	log.SetLevel(log.DebugLevel)

	txEnvXDR := "AAAAAGL8HQvQkbK2HA3WVjRrKmjX00fG8sLI7m0ERwJW/AX3AAAACgAAAAAAAAABAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAArqN6LeOagjxMaUP96Bzfs9e0corNZXzBWJkFoK7kvkwAAAAAO5rKAAAAAAAAAAABVvwF9wAAAEAKZ7IPj/46PuWU6ZOtyMosctNAkXRNX9WCAI5RnfRk+AyxDLoDZP/9l3NvsxQtWj9juQOuoBlFLnWu8intgxQA"
	mockResult := entities.RPCGetTransactionsResult{
		Transactions: []entities.Transaction{{
			Status:           entities.SuccessStatus,
			Hash:             "abcd",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           100,
		}, {
			Status:           entities.SuccessStatus,
			Hash:             "abcd",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           101,
		}},
		LatestLedger:          int64(100),
		LatestLedgerCloseTime: int64(1),
		OldestLedger:          int64(50),
		OldestLedgerCloseTime: int64(1),
	}
	mockRPCService.On("GetTransactions", int64(100), "", 50).Return(mockResult, nil).Once()
	mockAppTracker.On("CaptureMessage", mock.Anything).Maybe().Return(nil)

	// Start ingestion at ledger 100 (ahead of RPC's initial position at 50)
	err = ingestService.Run(ctx, uint32(100), uint32(100))
	require.NoError(t, err)

	// Verify the debug log message was written
	logOutput := logBuffer.String()
	expectedLog := "waiting for RPC to catchup to ledger 100 (latest: 50)"
	assert.Contains(t, logOutput, expectedLog)

	// Verify the ledger was eventually processed
	ledger, err := models.Payments.GetLatestLedgerSynced(context.Background(), "ingestionLedger")
	require.NoError(t, err)
	assert.Equal(t, uint32(100), ledger)

	mockRPCService.AssertExpectations(t)
}
