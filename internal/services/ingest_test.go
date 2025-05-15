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
)

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
	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockMetricsService)
	require.NoError(t, err)
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

		err = models.Account.Insert(context.Background(), srcAccount)
		require.NoError(t, err)
		paymentOp := txnbuild.Payment{
			SourceAccount: srcAccount,
			Destination:   destAccount,
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
		}
		var transaction *txnbuild.Transaction
		transaction, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&paymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})
		require.NoError(t, err)
		var txEnvXDR string
		txEnvXDR, err = transaction.Base64()
		require.NoError(t, err)

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

		err = models.Account.Insert(context.Background(), srcAccount)
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
		var transaction *txnbuild.Transaction
		transaction, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: keypair.MustRandom().Address(),
			},
			Operations:    []txnbuild.Operation{&pathPaymentOp},
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		})
		require.NoError(t, err)

		signer := keypair.MustRandom()
		err = models.Account.Insert(context.Background(), signer.Address())
		require.NoError(t, err)
		var signedTx *txnbuild.Transaction
		signedTx, err = transaction.Sign(network.TestNetworkPassphrase, signer)
		require.NoError(t, err)

		var txEnvXDR string
		txEnvXDR, err = signedTx.Base64()
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

		err = models.Account.Insert(context.Background(), srcAccount)
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
		err = models.Account.Insert(context.Background(), signer.Address())
		require.NoError(t, err)

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

func TestIngest_LatestSyncedLedgerBehindRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		require.NoError(t, dbConnectionPool.Close())
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
	mockMetricsService.On("ObserveIngestionDuration", "total", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "payment", 1).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_send", 0).Once()
	mockMetricsService.On("SetNumPaymentOpsIngestedPerLedger", "path_payment_strict_receive", 0).Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.On("TrackRPCServiceHealth", ctx, mock.Anything).Once()

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockMetricsService)
	require.NoError(t, err)

	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()

	paymentOp := txnbuild.Payment{
		SourceAccount: srcAccount,
		Destination:   destAccount,
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
	}
	transaction, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.MustRandom().Address(),
		},
		Operations:    []txnbuild.Operation{&paymentOp},
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	require.NoError(t, err)
	txEnvXDR, err := transaction.Base64()
	require.NoError(t, err)
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
		require.NoError(t, dbConnectionPool.Close())
		dbt.Close()
		log.DefaultLogger.SetOutput(os.Stderr)
	}()

	mockMetricsService := metrics.NewMockMetricsService()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.On("TrackRPCServiceHealth", ctx, mock.Anything).Once()

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockMetricsService)
	require.NoError(t, err)

	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "ingest_store").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "ingest_store").Once()
	mockMetricsService.On("SetLatestLedgerIngested", float64(100)).Once()
	mockMetricsService.On("ObserveIngestionDuration", "payment", mock.AnythingOfType("float64")).Once()
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
