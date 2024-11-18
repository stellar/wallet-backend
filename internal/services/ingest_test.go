package services

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
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

	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()
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
}

func TestIngest_LatestSyncedLedgerBehindRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx := context.Background()
	defer func() {
		ctx.Done()
		_ = dbConnectionPool.Close()
		dbt.Close()
	}()

	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}

	tssStore, err := tssstore.NewStore(dbConnectionPool)
	require.NoError(t, err)

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	require.NoError(t, err)

	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()

	mockRPCService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 100,
		OldestLedger: 50,
	}, nil)
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

	ctx := context.Background()
	defer func() {
		ctx.Done()
		_ = dbConnectionPool.Close()
		dbt.Close()
		log.DefaultLogger.SetOutput(os.Stderr)
	}()

	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}

	tssStore, err := tssstore.NewStore(dbConnectionPool)
	require.NoError(t, err)

	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	require.NoError(t, err)

	// First call shows RPC is behind
	mockRPCService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 50,
		OldestLedger: 1,
	}, nil).Once()

	// Second call after sleep shows RPC has caught up
	mockRPCService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 100, // RPC has caught up to ledger 100
		OldestLedger: 1,
	}, nil).Once()

	// Capture debug logs to verify waiting message
	var logBuffer bytes.Buffer
	log.DefaultLogger.SetOutput(&logBuffer)
	log.SetLevel(log.DebugLevel)

	// Mock transaction response for when RPC catches up
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

func TestTrackRPCServiceHealth_HealthyService(t *testing.T) {
	mockRPCService := &RPCServiceMock{}
	mockAppTracker := &apptracker.MockAppTracker{}
	heartbeat := make(chan entities.RPCGetHealthResult, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()

	healthResult := entities.RPCGetHealthResult{
		Status:                "healthy",
		LatestLedger:          100,
		OldestLedger:          1,
		LedgerRetentionWindow: 0,
	}
	mockRPCService.On("GetHealth").Return(healthResult, nil)

	go func() {
		select {
		case <-ctx.Done():
			return
		default:
			trackRPCServiceHealth(context.Background(), heartbeat, nil, mockRPCService)
		}
	}()

	select {
	case result := <-heartbeat:
		assert.Equal(t, healthResult, result)
	case <-ctx.Done():
		t.Fatal("timeout waiting for heartbeat")
	}

	mockRPCService.AssertExpectations(t)
	mockAppTracker.AssertNotCalled(t, "CaptureMessage")
}

func TestTrackRPCServiceHealth_UnhealthyService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("GetHealth").Return(
		entities.RPCGetHealthResult{},
		errors.New("rpc error"),
	)

	mockAppTracker := &apptracker.MockAppTracker{}
	mockAppTracker.On("CaptureMessage", "rpc service unhealthy for over 1m0s")
	heartbeat := make(chan entities.RPCGetHealthResult, 1)

	go func() {
		select {
		case <-ctx.Done():
			return
		default:
			trackRPCServiceHealth(context.Background(), heartbeat, mockAppTracker, mockRPCService)
		}
	}()

	// Wait long enough for both warnings to trigger
	time.Sleep(65 * time.Second)

	mockRPCService.AssertExpectations(t)
	mockAppTracker.AssertExpectations(t)
}

func TestTrackRPCService_ContextCancelled(t *testing.T) {
	mockRPCService := &RPCServiceMock{}
	mockAppTracker := &apptracker.MockAppTracker{}
	heartbeat := make(chan entities.RPCGetHealthResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go trackRPCServiceHealth(ctx, heartbeat, mockAppTracker, mockRPCService)
	mockRPCService.AssertNotCalled(t, "GetHealth")
	mockAppTracker.AssertNotCalled(t, "CaptureMessage")
}
