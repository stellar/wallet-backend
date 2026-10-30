package services

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

var (
	testKP1   = keypair.MustRandom()
	testKP2   = keypair.MustRandom()
	testAddr1 = testKP1.Address()
	testAddr2 = testKP2.Address()
)

const (
	// Test hash constants for ingest tests (64-char hex strings for BYTEA storage)
	flushTxHash1 = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101"
	flushTxHash2 = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f202"
	flushTxHash3 = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f303"
	flushTxHash4 = "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f404"
	txHash1      = "1111111111111111111111111111111111111111111111111111111111111111"
	txHash2      = "2222222222222222222222222222222222222222222222222222222222222222"

	// Test fixtures for ledger metadata
)

func TestLagLedgers(t *testing.T) {
	testCases := []struct {
		name           string
		backendTip     uint32
		latestIngested uint32
		wantLag        float64
		wantOK         bool
	}{
		{
			name:           "backend not ready yet returns no measurement",
			backendTip:     0,
			latestIngested: 5000,
			wantLag:        0,
			wantOK:         false,
		},
		{
			name:           "backend ahead of consumer reports positive lag",
			backendTip:     5100,
			latestIngested: 5000,
			wantLag:        100,
			wantOK:         true,
		},
		{
			name:           "caught up reports zero lag",
			backendTip:     5000,
			latestIngested: 5000,
			wantLag:        0,
			wantOK:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lag, ok := lagLedgers(tc.backendTip, tc.latestIngested)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantLag, lag)
		})
	}
}

func Test_generateAdvisoryLockID(t *testing.T) {
	testCases := []struct {
		name     string
		network  string
		expected int
	}{
		{
			name:     "testnet_generates_consistent_id",
			network:  "testnet",
			expected: generateAdvisoryLockID("testnet"),
		},
		{
			name:     "mainnet_generates_consistent_id",
			network:  "mainnet",
			expected: generateAdvisoryLockID("mainnet"),
		},
		{
			name:    "different_networks_generate_different_ids",
			network: "testnet",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := generateAdvisoryLockID(tc.network)

			if tc.name == "different_networks_generate_different_ids" {
				mainnetID := generateAdvisoryLockID("mainnet")
				testnetID := generateAdvisoryLockID("testnet")
				assert.NotEqual(t, mainnetID, testnetID, "different networks should generate different lock IDs")
			} else {
				// Verify consistency - same network should always generate same ID
				result2 := generateAdvisoryLockID(tc.network)
				assert.Equal(t, result, result2, "same network should generate same lock ID")
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func Test_NewIngestService_ProtocolProcessorValidation(t *testing.T) {
	t.Run("nil processor returns error", func(t *testing.T) {
		cfg := IngestServiceConfig{Metrics: metrics.NewMetrics(prometheus.NewRegistry())}
		cfg.ProtocolProcessors = []ProtocolProcessor{nil}
		_, err := NewIngestService(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "protocol processor at index 0 is nil")
	})

	t.Run("duplicate ProtocolID returns error", func(t *testing.T) {
		p1 := NewProtocolProcessorMock(t)
		p1.On("ProtocolID").Return("dup-id")
		p2 := NewProtocolProcessorMock(t)
		p2.On("ProtocolID").Return("dup-id")

		cfg := IngestServiceConfig{Metrics: metrics.NewMetrics(prometheus.NewRegistry())}
		cfg.ProtocolProcessors = []ProtocolProcessor{p1, p2}
		_, err := NewIngestService(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `duplicate key "dup-id"`)
	})
}

// ==================== Test Helper Functions ====================

// setupDBCursors sets up ingest_store cursors for testing
func setupDBCursors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, latestLedger, oldestLedger uint32) {
	_, err := pool.Exec(ctx, `DELETE FROM ingest_store`)
	require.NoError(t, err)
	if latestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, data.LatestLedgerCursorName, fmt.Sprintf("%d", latestLedger))
		require.NoError(t, err)
	}
	if oldestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ingest_ledger', $1)`, fmt.Sprintf("%d", oldestLedger))
		require.NoError(t, err)
	}
}

// ptrUint32 returns a pointer to the given uint32 value

// createTestTransaction creates a transaction with required fields for testing.
func createTestTransaction(hash string, toID int64) types.Transaction {
	now := time.Now()
	return types.Transaction{
		Hash:            types.HashBytea(hash),
		ToID:            toID,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
		IngestedAt:      now,
	}
}

// createTestOperation creates an operation with required fields for testing.
func createTestOperation(id int64) types.Operation {
	now := time.Now()
	return types.Operation{
		ID:              id,
		OperationType:   types.OperationTypePayment,
		OperationXDR:    types.XDRBytea([]byte("test_operation_xdr")),
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
		IngestedAt:      now,
	}
}

// createTestStateChange creates a state change with required fields for testing.

// ==================== New Tests ====================

func Test_ingestService_initializeCursors(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name        string
		startLedger uint32
		setupDB     func(t *testing.T)
	}{
		{
			name:        "initializes_both_cursors_from_empty",
			startLedger: 1000,
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `DELETE FROM ingest_store`)
				require.NoError(t, err)
			},
		},
		{
			name:        "overwrites_existing_cursors",
			startLedger: 2000,
			setupDB: func(t *testing.T) {
				setupDBCursors(t, ctx, dbConnectionPool, 500, 500)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupDB(t)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:     IngestionModeLive,
				Models:            models,
				RPCService:        mockRPCService,
				LedgerBackend:     &LedgerBackendMock{},
				Metrics:           m,
				NetworkPassphrase: network.TestNetworkPassphrase,
				Archive:           &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			err = db.RunInTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
				return svc.initializeCursors(ctx, dbTx, tc.startLedger)
			})
			require.NoError(t, err)

			// Verify both cursors are set to the same value
			latestCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
			require.NoError(t, err)
			assert.Equal(t, tc.startLedger, latestCursor)

			oldestCursor, err := models.IngestStore.Get(ctx, "oldest_ingest_ledger")
			require.NoError(t, err)
			assert.Equal(t, tc.startLedger, oldestCursor)
		})
	}
}

func Test_ingestService_Run(t *testing.T) {
	testCases := []struct {
		name            string
		mode            string
		wantErrContains string
	}{
		{
			name:            "unsupported_mode_returns_error",
			mode:            "invalid",
			wantErrContains: "unsupported ingestion mode",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbt := dbtest.Open(t)
			defer dbt.Close()
			ctx := context.Background()
			dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
			require.NoError(t, err)
			defer dbConnectionPool.Close()

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:     tc.mode,
				Models:            models,
				RPCService:        mockRPCService,
				LedgerBackend:     &LedgerBackendMock{},
				Metrics:           m,
				NetworkPassphrase: network.TestNetworkPassphrase,
				Archive:           &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			err = svc.Run(ctx, 100)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrContains)
		})
	}
}

// ==================== Backfill Failure Scenario Tests ====================

// Test_ingestLiveLedgers_batchReachesConfiguredCap pins the sizing of the
// process↔persist buffer rotation. A buffer stays checked out until the batch
// carrying it commits, so sustaining a full batch takes the cap's worth of
// buffers for the batch in flight plus the cap's worth for the process stage to
// refill behind it. Sized any tighter, process starves on freeBuffers before the
// queue refills and the batch settles strictly below livePersistMaxBatchSize no
// matter how deep the backlog — leaving the configured cap unreachable at every
// load, which the batch-size histogram reports as a suspiciously constant value.
func Test_ingestLiveLedgers_batchReachesConfiguredCap(t *testing.T) {
	const batchCap = 3

	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const startLedger = uint32(51) // not a multiple of oldestLedgerSyncInterval (100)
	setupDBCursors(t, ctx, pool, startLedger-1, startLedger-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // trustlineChangesByTrustlineKey
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessSACBalanceChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // sacBalanceChangesByKey
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // accountChangesByAccountID
		mock.Anything, // lpShareChangesByKey
		mock.Anything, // lpChangesByPoolID
	).Return(nil).Maybe()

	// Supply is unbounded and every ledger is empty, so fetch and process both
	// outrun the persist stage's commit round-trip: a backlog is always present
	// for the batch to coalesce.
	mockBackend := &LedgerBackendMock{}
	mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(dummyLedgerMeta(1), nil).Maybe()
	mockBackend.On("GetLatestLedgerSequence", mock.Anything).
		Return(uint32(0), errors.New("lag gauge unused in this test")).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:           IngestionModeLive,
		Models:                  models,
		RPCService:              &RPCServiceMock{},
		LedgerBackend:           mockBackend,
		TokenIngestionService:   mockTokenIngestionService,
		Metrics:                 m,
		NetworkPassphrase:       network.TestNetworkPassphrase,
		Archive:                 &HistoryArchiveMock{},
		LivePersistMaxBatchSize: batchCap,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- svc.ingestLiveLedgers(ctx, startLedger, func(context.Context) error { return nil }) }()

	// Grade the mean over many commits, not a single observation: while the
	// rotation is still filling at startup the process stage can hand off more
	// than the steady state sustains, so one large batch proves nothing.
	var h *dto.Histogram
	require.Eventually(t, func() bool {
		var mt dto.Metric
		require.NoError(t, m.Ingestion.PersistBatchSize.Write(&mt))
		h = mt.GetHistogram()
		return h.GetSampleCount() >= 20
	}, 30*time.Second, 50*time.Millisecond, "pipeline did not reach 20 persist commits")

	mean := h.GetSampleSum() / float64(h.GetSampleCount())
	assert.Greaterf(t, mean, float64(batchCap)-0.5,
		"mean persist batch %.2f over %d commits fell short of the configured cap %d: the process↔persist buffer rotation cannot refill a full batch",
		mean, h.GetSampleCount(), batchCap)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingestLiveLedgers did not return after context cancellation")
	}
}

// oneLedger wraps a single ledger's persist payload as the batch slice
// persistLedgerData and persistLedgerDataWithRetry consume.
func oneLedger(seq uint32, contractData *contractDataMemo, buffer *indexer.IndexerBuffer) []processedLedger {
	return []processedLedger{{seq: seq, contractData: contractData, buffer: buffer}}
}

// Test_persistBatch_observesInsertIntoDBOncePerBatch pins insert_into_db to
// one observation per persist commit carrying the batch's wall time. One
// observation per ledger would dilute a slow batched commit into shares that
// read as healthy exactly when persist has fallen behind.
func Test_persistBatch_observesInsertIntoDBOncePerBatch(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()
	setupDBCursors(t, ctx, pool, 99, 99)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessSACBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:         IngestionModeLive,
		Models:                models,
		RPCService:            &RPCServiceMock{},
		LedgerBackend:         &LedgerBackendMock{},
		TokenIngestionService: mockTokenIngestionService,
		Metrics:               m,
		NetworkPassphrase:     network.TestNetworkPassphrase,
		Archive:               &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	sampleCount := func() uint64 {
		var dm dto.Metric
		require.NoError(t, m.Ingestion.PhaseDuration.WithLabelValues("insert_into_db").(prometheus.Histogram).Write(&dm))
		return dm.GetHistogram().GetSampleCount()
	}

	before := sampleCount()
	batch := []processedLedger{
		{seq: 100, contractData: newContractDataMemo(nil, 100), buffer: indexer.NewIndexerBuffer()},
		{seq: 101, contractData: newContractDataMemo(nil, 101), buffer: indexer.NewIndexerBuffer()},
	}
	_, err = svc.persistBatch(ctx, batch, nil)
	require.NoError(t, err)
	assert.Equal(t, before+1, sampleCount(), "a %d-ledger batch must observe insert_into_db once", len(batch))
}

func Test_persistLedgerDataWithRetry(t *testing.T) {
	t.Run("success - processes data and updates cursor", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Clean up database
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM operations`)
		require.NoError(t, err)

		// Set initial cursor to 99
		initialCursor := uint32(99)
		setupDBCursors(t, ctx, dbConnectionPool, initialCursor, initialCursor)

		m := metrics.NewMetrics(prometheus.NewRegistry())

		models, err := data.NewModels(dbConnectionPool, m.DB)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		// Mock AccountTokenService to succeed
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
		).Return(nil)
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // sacBalanceChangesByKey
		).Return(nil)
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
		).Return(nil)

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            mockRPCService,
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
		})
		require.NoError(t, err)

		// Create buffer with some data
		buffer := indexer.NewIndexerBuffer()
		buffer.PushTrustlineChange(types.TrustlineChange{
			AccountID:   "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			Operation:   types.TrustlineOpAdd,
			OperationID: 1,
		})

		// Call persistLedgerDataWithRetry - should succeed
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		err = svc.persistLedgerDataWithRetry(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)

		// Verify success
		require.NoError(t, err)

		// Verify DB cursor was updated
		finalCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), finalCursor, "cursor should be updated to 100")

		mockTokenIngestionService.AssertExpectations(t)
	})

	t.Run("DB failure rolls back transaction", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Clean up database
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM operations`)
		require.NoError(t, err)

		// Set initial cursor to 99
		initialCursor := uint32(99)
		setupDBCursors(t, ctx, dbConnectionPool, initialCursor, initialCursor)

		m := metrics.NewMetrics(prometheus.NewRegistry())

		models, err := data.NewModels(dbConnectionPool, m.DB)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		// Mock AccountTokenService to return error (simulating DB failure)
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // sacBalanceChangesByKey
		).Return(fmt.Errorf("db connection failed"))
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            mockRPCService,
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
		})
		require.NoError(t, err)

		// Create buffer with some data
		buffer := indexer.NewIndexerBuffer()
		buffer.PushTrustlineChange(types.TrustlineChange{
			AccountID:   "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			Operation:   types.TrustlineOpAdd,
			OperationID: 1,
		})

		// Call persistLedgerDataWithRetry - should fail after retries due to DB error
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		err = svc.persistLedgerDataWithRetry(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)

		// Verify error propagates with retry failure message
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed after")
		assert.Contains(t, err.Error(), "db connection failed")

		// Verify DB cursor was NOT updated (transaction rolled back)
		finalCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, err)
		assert.Equal(t, initialCursor, finalCursor, "cursor should NOT be updated when DB fails")

		mockTokenIngestionService.AssertExpectations(t)
	})

	t.Run("retries on transient error then succeeds", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Clean up database
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM operations`)
		require.NoError(t, err)

		// Set initial cursor to 99
		initialCursor := uint32(99)
		setupDBCursors(t, ctx, dbConnectionPool, initialCursor, initialCursor)

		m := metrics.NewMetrics(prometheus.NewRegistry())

		models, err := data.NewModels(dbConnectionPool, m.DB)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		// Mock AccountTokenService to fail once then succeed
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // sacBalanceChangesByKey
		).Return(fmt.Errorf("transient error")).Once()
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // sacBalanceChangesByKey
		).Return(nil).Once()
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            mockRPCService,
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
		})
		require.NoError(t, err)

		// Create buffer with some data
		buffer := indexer.NewIndexerBuffer()
		buffer.PushTrustlineChange(types.TrustlineChange{
			AccountID:   "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			Operation:   types.TrustlineOpAdd,
			OperationID: 1,
		})

		// Call persistLedgerDataWithRetry - should succeed after retry
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		err = svc.persistLedgerDataWithRetry(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)

		// Verify success after retry
		require.NoError(t, err)

		// Verify DB cursor was updated
		finalCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), finalCursor, "cursor should be updated after successful retry")

		mockTokenIngestionService.AssertExpectations(t)
	})
}

// testProtocolProcessor is a test-only ProtocolProcessor that writes sentinel
// values into ingest_store within the DB transaction, proving PersistHistory
// and PersistCurrentState were called and committed atomically.
type testProtocolProcessor struct {
	id                        string
	processLedgerCalls        int
	processedLedger           uint32
	stagedLedgerCount         int
	ingestStore               *data.IngestStoreModel
	persistCurrentStateCalls  int
	failPersistCurrentStateAt uint32
	lastContracts             []data.ProtocolContracts
}

func (p *testProtocolProcessor) ProtocolID() string { return p.id }

func (p *testProtocolProcessor) StateChangeOrdinalBase() int64 {
	return types.StateChangeOrdinalBaseSEP41
}

func (p *testProtocolProcessor) RequiresContractData() bool { return false }

func (p *testProtocolProcessor) Reset() { p.stagedLedgerCount = 0 }

func (p *testProtocolProcessor) ProcessLedger(_ context.Context, input ProtocolProcessorInput) error {
	p.processLedgerCalls++
	p.stagedLedgerCount++
	p.processedLedger = input.LedgerSequence
	p.lastContracts = input.ProtocolContracts
	return nil
}

func (p *testProtocolProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_written", p.id), p.processedLedger)
}

func (p *testProtocolProcessor) WipeCurrentState(_ context.Context, _ pgx.Tx) error {
	return nil
}

func (p *testProtocolProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	p.persistCurrentStateCalls++
	if p.failPersistCurrentStateAt != 0 && p.processedLedger == p.failPersistCurrentStateAt {
		return fmt.Errorf("simulated current state persist failure at ledger %d", p.processedLedger)
	}
	if err := p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_staged_count", p.id), uint32(p.stagedLedgerCount)); err != nil {
		return err
	}
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_current_state_written", p.id), p.processedLedger)
}

// setupProtocolCursors inserts protocol cursors into ingest_store.
// Call AFTER setupDBCursors (which wipes the table).
func setupProtocolCursors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, historyCursor, currentStateCursor uint32) {
	t.Helper()
	const protocolID = "testproto"
	_, err := pool.Exec(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		utils.ProtocolHistoryCursorName(protocolID), strconv.FormatUint(uint64(historyCursor), 10))
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		utils.ProtocolCurrentStateCursorName(protocolID), strconv.FormatUint(uint64(currentStateCursor), 10))
	require.NoError(t, err)
}

func Test_persistLedgerData_ProtocolCASGating(t *testing.T) {
	// Helper to set up common test infrastructure
	setupTest := func(t *testing.T, processors []ProtocolProcessor) (context.Context, *ingestService, *data.Models, *pgxpool.Pool) {
		t.Helper()
		dbt := dbtest.Open(t)
		t.Cleanup(func() { dbt.Close() })
		ctx := context.Background()
		pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })

		m := metrics.NewMetrics(prometheus.NewRegistry())

		models, err := data.NewModels(pool, m.DB)
		require.NoError(t, err)

		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // sacBalanceChangesByKey
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            &RPCServiceMock{},
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
			ProtocolProcessors:    processors,
		})
		require.NoError(t, err)

		return ctx, svc, models, pool
	}

	t.Run("A: CAS win — cursors at ledger-1", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})

		// Set ingestStore after models are created, and simulate ProcessLedger
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		// Both protocol cursors should advance to 100
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), histCursor)

		csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), csCursor)

		// Sentinel values prove PersistHistory/PersistCurrentState were called
		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), histSentinel)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), csSentinel)
	})

	t.Run("B: CAS lose — cursors already at ledger", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 100, 100)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		// Cursors should stay at 100 (CAS expected 99 but found 100)
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), histCursor)

		csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), csCursor)

		// No sentinels — persist methods were NOT called
		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histSentinel)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), csSentinel)
	})

	t.Run("C: cursor behind — migration hasn't caught up", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 98, 98)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		// Cursors should stay at 98 (behind, so entire block is skipped)
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(98), histCursor)

		csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(98), csCursor)

		// No sentinels
		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histSentinel)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), csSentinel)
	})

	t.Run("D: missing cursor row at CAS time — soft skip, main cursor advances", func(t *testing.T) {
		// A protocol whose cursor rows don't exist (protocol-setup / protocol-migrate
		// hasn't run yet, or an incident dropped the row) must not kill live ingest:
		// CompareAndSwap returns ErrCASCursorMissing, which casProtocolCursor folds
		// into a soft skip (Debugf + the data-layer cursor_missing metric). The main
		// cursor still advances.
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		// No protocol cursors inserted.

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		// Main cursor advances; protocol persist methods were not called and
		// protocol cursor rows remain absent.
		mainCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), mainCursor)

		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histCursor)

		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histSentinel)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), csSentinel)
	})

	t.Run("D2: only history cursor exists — only history CAS runs", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100

		setupDBCursors(t, ctx, pool, 99, 99)
		// Insert ONLY the history cursor; the current-state row stays absent, so its
		// CAS returns ErrCASCursorMissing (soft skip) and only the history CAS can win.
		_, err := pool.Exec(ctx,
			`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
			utils.ProtocolHistoryCursorName("testproto"), "99")
		require.NoError(t, err)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		err = svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), indexer.NewIndexerBuffer()), nil)
		require.NoError(t, err)

		// History CAS succeeded.
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), histCursor)

		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), histSentinel)

		// Current-state CAS was skipped: row still absent, persist not called.
		csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), csCursor)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), csSentinel)

		assert.Equal(t, 0, processor.persistCurrentStateCalls)
	})

	t.Run("D3: only current-state cursor exists — only current-state CAS runs", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100

		setupDBCursors(t, ctx, pool, 99, 99)
		// Insert ONLY the current-state cursor; the history row stays absent, so its
		// CAS returns ErrCASCursorMissing (soft skip) and only the current-state CAS wins.
		_, err := pool.Exec(ctx,
			`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
			utils.ProtocolCurrentStateCursorName("testproto"), "99")
		require.NoError(t, err)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		err = svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), indexer.NewIndexerBuffer()), nil)
		require.NoError(t, err)

		// Current-state CAS succeeded.
		csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), csCursor)

		csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), csSentinel)

		// History CAS was skipped: row still absent, persist not called.
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histCursor)

		histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
		require.NoError(t, err)
		assert.Equal(t, uint32(0), histSentinel)
	})

	t.Run("E: no processors — main cursor advances, no protocol work", func(t *testing.T) {
		ctx, svc, models, pool := setupTest(t, nil) // nil ProtocolProcessors

		setupDBCursors(t, ctx, pool, 99, 99)

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		// Main cursor should advance
		mainCursor, err := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), mainCursor)
	})

	t.Run("F: PersistCurrentState failure rolls back the cursor and a retry succeeds", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto", failPersistCurrentStateAt: 101}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// First ledger succeeds and advances the current-state cursor to 100.
		processor.processedLedger = 100
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), indexer.NewIndexerBuffer()), nil)
		require.NoError(t, err)

		// Next ledger fails inside PersistCurrentState, rolling back the whole
		// transaction — the current-state cursor must stay at 100.
		processor.processedLedger = 101
		err = svc.persistLedgerData(ctx, oneLedger(101, newContractDataMemo(nil, 101), indexer.NewIndexerBuffer()), nil)
		require.Error(t, err)

		currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), currentStateCursor)

		// Retrying the same ledger succeeds and advances the cursor.
		processor.failPersistCurrentStateAt = 0
		processor.processedLedger = 101
		err = svc.persistLedgerData(ctx, oneLedger(101, newContractDataMemo(nil, 101), indexer.NewIndexerBuffer()), nil)
		require.NoError(t, err)

		currentStateCursor, err = models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(101), currentStateCursor)

		stagedCount, err := models.IngestStore.Get(ctx, "test_testproto_staged_count")
		require.NoError(t, err)
		assert.Equal(t, uint32(1), stagedCount) // retry re-staged cleanly; not doubled
	})

	t.Run("K: cursor existed at snapshot but is now missing — hard error (incident)", func(t *testing.T) {
		// ING-12: casProtocolCursor only calls CompareAndSwap for a cursor
		// snapshotProtocolCursors believed existed. If the row has since
		// vanished (dropped row, bad restore — never a normal live-ingestion
		// state transition), that must abort the transaction as a genuine
		// incident rather than the operationally-normal soft skip.
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx)) // snapshot sees both cursors existing

		// Simulate the incident: the history cursor row vanishes after the snapshot.
		_, delErr := pool.Exec(ctx, `DELETE FROM ingest_store WHERE key = $1`, utils.ProtocolHistoryCursorName("testproto"))
		require.NoError(t, delErr)

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, data.ErrCASCursorMissing)

		// The transaction rolled back entirely: even the main cursor, which no
		// snapshot gates, stayed at its pre-ledger value.
		mainCursor, getErr := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
		require.NoError(t, getErr)
		assert.Equal(t, uint32(99), mainCursor)

		// The still-existing current-state cursor is untouched (whole tx rolled back).
		csCursor, getErr := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, getErr)
		assert.Equal(t, uint32(99), csCursor)
	})

	t.Run("L: reprobeProtocolCursors promotes a cursor initialized after the snapshot", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		// No protocol cursors at snapshot time: both start out known-missing.
		require.NoError(t, svc.snapshotProtocolCursors(ctx))
		assert.False(t, svc.protocolCursors.historyExists["testproto"])
		assert.False(t, svc.protocolCursors.currentStateExists["testproto"])

		// A protocol-setup/migrate run initializes both cursors afterward, without a restart.
		setupProtocolCursors(t, ctx, pool, 99, 99)
		svc.reprobeProtocolCursors(ctx)
		assert.True(t, svc.protocolCursors.historyExists["testproto"])
		assert.True(t, svc.protocolCursors.currentStateExists["testproto"])

		// Production is now enabled: a ledger processed after the re-probe actually CASes.
		processor.processedLedger = 100
		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)

		histCursor, getErr := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, getErr)
		assert.Equal(t, uint32(100), histCursor)
	})

	t.Run("H: RequiresContractData true — processor receives non-nil ContractDataChanges", func(t *testing.T) {
		processor := NewProtocolProcessorMock(t)
		processor.On("ProtocolID").Return("testproto")
		processor.On("RequiresContractData").Return(true)
		processor.On("Reset").Return()
		processor.On("ProcessLedger", mock.Anything, mock.MatchedBy(func(in ProtocolProcessorInput) bool {
			return in.ContractDataChanges != nil
		})).Return(nil)
		processor.On("PersistHistory", mock.Anything, mock.Anything).Return(nil)
		processor.On("PersistCurrentState", mock.Anything, mock.Anything).Return(nil)

		ctx, svc, _, pool := setupTest(t, []ProtocolProcessor{processor})
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)
	})

	t.Run("I: RequiresContractData false — processor receives nil ContractDataChanges", func(t *testing.T) {
		processor := NewProtocolProcessorMock(t)
		processor.On("ProtocolID").Return("testproto")
		processor.On("RequiresContractData").Return(false)
		processor.On("Reset").Return()
		processor.On("ProcessLedger", mock.Anything, mock.MatchedBy(func(in ProtocolProcessorInput) bool {
			return in.ContractDataChanges == nil
		})).Return(nil)
		processor.On("PersistHistory", mock.Anything, mock.Anything).Return(nil)
		processor.On("PersistCurrentState", mock.Anything, mock.Anything).Return(nil)

		ctx, svc, _, pool := setupTest(t, []ProtocolProcessor{processor})
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)
	})

	t.Run("J: RequiresContractData true — full committed membership reaches the processor", func(t *testing.T) {
		// A classified contract that emits NO events this ledger must still be
		// in a ContractData-requiring processor's ProtocolContracts (entries can
		// change without events, and event decoding disambiguates against the
		// full tracked set). Event-only processors keep the cheaper
		// event-derived membership, which is empty for this event-less ledger.
		contractID := []byte("contract_no_events_this_ledger33")
		wasmHash := []byte("wasm_hash_for_membership_test333")

		requiring := NewProtocolProcessorMock(t)
		requiring.On("ProtocolID").Return("testproto")
		requiring.On("RequiresContractData").Return(true)
		requiring.On("Reset").Return()
		// ContractID scans BYTEA into its hex form (the same convention
		// bufferedContracts keys use), so compare against the hex encoding.
		wantContractID := hex.EncodeToString(contractID)
		requiring.On("ProcessLedger", mock.Anything, mock.MatchedBy(func(in ProtocolProcessorInput) bool {
			for _, c := range in.ProtocolContracts {
				if string(c.ContractID) == wantContractID {
					return true
				}
			}
			return false
		})).Return(nil)
		requiring.On("PersistHistory", mock.Anything, mock.Anything).Return(nil)
		requiring.On("PersistCurrentState", mock.Anything, mock.Anything).Return(nil)

		eventOnly := NewProtocolProcessorMock(t)
		eventOnly.On("ProtocolID").Return("otherproto")
		eventOnly.On("RequiresContractData").Return(false)
		eventOnly.On("Reset").Return()
		eventOnly.On("ProcessLedger", mock.Anything, mock.MatchedBy(func(in ProtocolProcessorInput) bool {
			return len(in.ProtocolContracts) == 0
		})).Return(nil)
		eventOnly.On("PersistHistory", mock.Anything, mock.Anything).Return(nil)
		eventOnly.On("PersistCurrentState", mock.Anything, mock.Anything).Return(nil)

		ctx, svc, _, pool := setupTest(t, []ProtocolProcessor{requiring, eventOnly})
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		// setupProtocolCursors seeds "testproto" only; otherproto needs its own
		// cursors for the CAS swap to succeed.
		_, err := pool.Exec(ctx,
			`INSERT INTO ingest_store (key, value) VALUES ($1, '99'), ($2, '99')`,
			utils.ProtocolHistoryCursorName("otherproto"), utils.ProtocolCurrentStateCursorName("otherproto"))
		require.NoError(t, err)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		_, err = pool.Exec(ctx, `INSERT INTO protocols (id) VALUES ('testproto'), ('otherproto') ON CONFLICT (id) DO NOTHING`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO protocol_wasms (wasm_hash, protocol_id) VALUES ($1, 'testproto')`, wasmHash)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO protocol_contracts (contract_id, wasm_hash) VALUES ($1, $2)`, contractID, wasmHash)
		require.NoError(t, err)

		buffer := indexer.NewIndexerBuffer()
		err = svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.NoError(t, err)
	})

	t.Run("G: contract-id lookup failure fails the ledger", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// Inject a failing contract-id lookup over the otherwise-real models.
		contractsMock := data.NewProtocolContractsModelMock(t)
		contractsMock.On("BatchGetByContractIDs", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("db boom")).Once()
		models.ProtocolContracts = contractsMock

		// A contract event makes the event-contract-id set non-empty so the lookup runs.
		var contractID xdr.ContractId
		buffer := indexer.NewIndexerBuffer()
		buffer.PushContractEvents(
			indexer.ContractEventKey{TxIdx: 0, OpIdx: 0},
			[]xdr.ContractEvent{{Type: xdr.ContractEventTypeContract, ContractId: &contractID}},
		)

		err := svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), nil)
		require.ErrorContains(t, err, "resolving protocol contracts for ledger 100")

		// The transaction rolled back: the protocol history cursor stayed at 99.
		histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(99), histCursor)
	})
}

func Test_getEffectiveProtocolContracts_ReturnsCommittedWhenNoBuffered(t *testing.T) {
	t.Parallel()

	committed := []data.ProtocolContracts{{ContractID: types.HashBytea(txHash1), WasmHash: types.HashBytea(txHash2)}}

	contracts := getEffectiveProtocolContracts("testproto", committed, nil, nil)
	assert.Equal(t, committed, contracts)
}

func Test_getEffectiveProtocolContracts_MergesBufferedWithCommitted(t *testing.T) {
	t.Parallel()

	baseContract := data.ProtocolContracts{ContractID: types.HashBytea(txHash1), WasmHash: types.HashBytea(txHash2)}
	currentLedgerContract := data.ProtocolContracts{ContractID: types.HashBytea(flushTxHash3), WasmHash: types.HashBytea(flushTxHash4)}
	expectedContracts := []data.ProtocolContracts{baseContract, currentLedgerContract}

	contracts := getEffectiveProtocolContracts("testproto",
		[]data.ProtocolContracts{baseContract},
		map[string]data.ProtocolContracts{string(currentLedgerContract.ContractID): currentLedgerContract},
		map[types.HashBytea]string{currentLedgerContract.WasmHash: "testproto"},
	)
	assert.Equal(t, expectedContracts, contracts)
}

func Test_getEffectiveProtocolContracts_IncludesBufferedWhenCommittedEmpty(t *testing.T) {
	t.Parallel()

	currentLedgerContract := data.ProtocolContracts{ContractID: types.HashBytea(flushTxHash3), WasmHash: types.HashBytea(flushTxHash4)}
	expectedContracts := []data.ProtocolContracts{currentLedgerContract}

	contracts := getEffectiveProtocolContracts("testproto",
		nil,
		map[string]data.ProtocolContracts{string(currentLedgerContract.ContractID): currentLedgerContract},
		map[types.HashBytea]string{currentLedgerContract.WasmHash: "testproto"},
	)
	assert.Equal(t, expectedContracts, contracts)
}

func Test_getEffectiveProtocolContracts_RemovesContractsUpgradedAwayFromProtocol(t *testing.T) {
	t.Parallel()

	baseContract := data.ProtocolContracts{ContractID: types.HashBytea(txHash1), WasmHash: types.HashBytea(txHash2)}
	upgradedContract := data.ProtocolContracts{ContractID: types.HashBytea(txHash1), WasmHash: types.HashBytea(flushTxHash3)}

	contracts := getEffectiveProtocolContracts("testproto",
		[]data.ProtocolContracts{baseContract},
		map[string]data.ProtocolContracts{string(upgradedContract.ContractID): upgradedContract},
		map[types.HashBytea]string{upgradedContract.WasmHash: "otherproto"},
	)
	assert.Empty(t, contracts)
}

// Test_getEffectiveProtocolContracts_ReObservationKeepsMembership pins the case
// a re-observed contract must survive. Any instance change (TTL bump, restore,
// storage write) buffers a committed contract again with its binding unchanged;
// the batch's plan resolves that binding's wasm to the protocol, so the overlay
// has to re-add the contract rather than drop it as an upgrade-away.
func Test_getEffectiveProtocolContracts_ReObservationKeepsMembership(t *testing.T) {
	t.Parallel()

	trackedContract := data.ProtocolContracts{ContractID: types.HashBytea(txHash1), WasmHash: types.HashBytea(txHash2)}

	contracts := getEffectiveProtocolContracts("testproto",
		[]data.ProtocolContracts{trackedContract},
		map[string]data.ProtocolContracts{string(trackedContract.ContractID): trackedContract},
		map[types.HashBytea]string{trackedContract.WasmHash: "testproto"},
	)
	assert.Equal(t, []data.ProtocolContracts{trackedContract}, contracts)
}

func Test_distinctEventContractIDs(t *testing.T) {
	t.Parallel()

	var idA, idB xdr.ContractId
	idA[0] = 0xAA
	idB[0] = 0xBB

	events := map[indexer.ContractEventKey][]xdr.ContractEvent{
		{TxIdx: 0, OpIdx: 0}: {
			{ContractId: &idA},
			{ContractId: &idB},
			{ContractId: &idA}, // duplicate within a bucket — collapsed
			{ContractId: nil},  // missing id — skipped
		},
		{TxIdx: 1, OpIdx: 0}: {
			{ContractId: &idB}, // duplicate across buckets — collapsed
		},
	}

	got := distinctEventContractIDs(events)
	assert.ElementsMatch(t, [][]byte{idA[:], idB[:]}, got)
}

func Test_prepareClassificationPlan(t *testing.T) {
	ctx := context.Background()

	// prepareClassificationPlan only touches models.ProtocolWasms, the
	// validators/extractor pair, and the ingestion metrics, so a struct
	// literal with those fields is the whole service surface under test.
	newSvc := func(wasms data.ProtocolWasmsModelInterface, validators []ProtocolValidator, extractor WasmSpecExtractor) *ingestService {
		return &ingestService{
			models:             &data.Models{ProtocolWasms: wasms},
			appMetrics:         metrics.NewMetrics(prometheus.NewRegistry()),
			protocolValidators: validators,
			wasmSpecExtractor:  extractor,
			rpcService:         &RPCServiceMock{},
		}
	}

	var w1Raw, w2Raw, c1Raw, c2Raw [32]byte
	w1Raw[0], w2Raw[0], c1Raw[0], c2Raw[0] = 0xA1, 0xA2, 0xC1, 0xC2
	w1 := types.HashBytea(hex.EncodeToString(w1Raw[:]))
	w2 := types.HashBytea(hex.EncodeToString(w2Raw[:]))
	c1 := types.HashBytea(hex.EncodeToString(c1Raw[:]))
	c2 := types.HashBytea(hex.EncodeToString(c2Raw[:]))

	t.Run("nil plan when nothing is buffered", func(t *testing.T) {
		svc := newSvc(data.NewProtocolWasmsModelMock(t), nil, NewWasmSpecExtractorMock(t))

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, plan)
	})

	t.Run("this-batch hashes skip the known lookup; earlier-ledger hashes resolve from it", func(t *testing.T) {
		// w1 is uploaded this ledger (contract c1 points at it); w2 was uploaded
		// in an earlier ledger (contract c2 points at it). Only w2 may reach the
		// GetClassifiedByHashes read — the mock's argument matcher enforces that.
		wasmsMock := data.NewProtocolWasmsModelMock(t)
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything,
			mock.MatchedBy(func(hashes []types.HashBytea) bool {
				return len(hashes) == 1 && hashes[0] == w2
			}),
		).Return(map[types.HashBytea]string{w2: "B"}, nil).Once()

		rv := newRecordingValidator("A", w1)
		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()
		svc := newSvc(wasmsMock, []ProtocolValidator{rv}, extractor)

		plan, err := svc.prepareClassificationPlan(ctx,
			map[string]data.ProtocolWasms{string(w1): {WasmHash: w1}},
			map[string][]byte{string(w1): {1, 2, 3}},
			map[string]data.ProtocolContracts{
				string(c1): {ContractID: c1, WasmHash: w1},
				string(c2): {ContractID: c2, WasmHash: w2},
			}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, map[types.HashBytea]string{w1: "A", w2: "B"}, plan.Matches)

		// The validator saw both contracts annotated: c2 carries the known
		// verdict, c1 stays unannotated (its wasm is claimed via the matched set).
		annotations := map[types.HashBytea]string{}
		for _, ct := range rv.lastContracts {
			annotations[ct.ContractID] = ct.KnownProtocolID
		}
		assert.Equal(t, map[types.HashBytea]string{c1: "", c2: "B"}, annotations)
	})

	t.Run("contract-only ledger seeds the plan from known verdicts", func(t *testing.T) {
		wasmsMock := data.NewProtocolWasmsModelMock(t)
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
			Return(map[types.HashBytea]string{w2: "A"}, nil).Once()

		rv := newRecordingValidator("A") // claims no wasms; acts on contracts only
		// No ExtractSpec expectations: with no buffered wasms there are no
		// candidates, so the extractor must never run.
		svc := newSvc(wasmsMock, []ProtocolValidator{rv}, NewWasmSpecExtractorMock(t))

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil,
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}}, nil)
		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, map[types.HashBytea]string{w2: "A"}, plan.Matches)
		require.Equal(t, 1, rv.prefetchCalls)
		require.Len(t, rv.lastContracts, 1)
		assert.Equal(t, "A", rv.lastContracts[0].KnownProtocolID)
	})

	t.Run("no validators still returns a plan seeded from known verdicts", func(t *testing.T) {
		wasmsMock := data.NewProtocolWasmsModelMock(t)
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
			Return(map[types.HashBytea]string{w2: "A"}, nil).Once()

		svc := newSvc(wasmsMock, nil, NewWasmSpecExtractorMock(t))

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil,
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}}, nil)
		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, map[types.HashBytea]string{w2: "A"}, plan.Matches)
	})

	t.Run("transient known-read failure is retried", func(t *testing.T) {
		wasmsMock := data.NewProtocolWasmsModelMock(t)
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("transient blip")).Once()
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
			Return(map[types.HashBytea]string{w2: "A"}, nil).Once()

		svc := newSvc(wasmsMock, nil, NewWasmSpecExtractorMock(t))

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil,
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}}, nil)
		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, map[types.HashBytea]string{w2: "A"}, plan.Matches)
		assert.Equal(t, 1.0, testutil.ToFloat64(
			svc.appMetrics.Ingestion.RetriesTotal.WithLabelValues("classification_read")))
	})

	t.Run("permanent known-read failure fails fast without retrying", func(t *testing.T) {
		wasmsMock := data.NewProtocolWasmsModelMock(t)
		wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &pgconn.PgError{Code: "42703"}).Once() // undefined_column: retrying cannot succeed

		svc := newSvc(wasmsMock, nil, NewWasmSpecExtractorMock(t))

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil,
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}}, nil)
		require.Error(t, err)
		assert.Nil(t, plan)
		assert.Equal(t, 0.0, testutil.ToFloat64(
			svc.appMetrics.Ingestion.RetriesTotal.WithLabelValues("classification_read")))
	})
}

func Test_persistLedgerData_ClassificationPlan(t *testing.T) {
	setupTest := func(t *testing.T, processors []ProtocolProcessor) (context.Context, *ingestService, *data.Models, *pgxpool.Pool) {
		t.Helper()
		dbt := dbtest.Open(t)
		t.Cleanup(func() { dbt.Close() })
		ctx := context.Background()
		pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })

		m := metrics.NewMetrics(prometheus.NewRegistry())

		models, err := data.NewModels(pool, m.DB)
		require.NoError(t, err)

		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            &RPCServiceMock{},
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
			ProtocolProcessors:    processors,
		})
		require.NoError(t, err)

		return ctx, svc, models, pool
	}

	t.Run("plan matches stamp wasm rows, run Apply in-tx, and admit the buffered contract to staging", func(t *testing.T) {
		var w1Raw, c1Raw [32]byte
		w1Raw[0], c1Raw[0] = 0xA1, 0xC1
		w1 := types.HashBytea(hex.EncodeToString(w1Raw[:]))
		c1 := types.HashBytea(hex.EncodeToString(c1Raw[:]))

		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// protocol_wasms.protocol_id is an FK into protocols.
		_, err := pool.Exec(ctx, `INSERT INTO protocols (id) VALUES ('testproto')`)
		require.NoError(t, err)

		buffer := indexer.NewIndexerBuffer()
		buffer.PushProtocolWasm(data.ProtocolWasms{WasmHash: w1})
		buffer.PushProtocolContracts(data.ProtocolContracts{ContractID: c1, WasmHash: w1})

		rv := newRecordingValidator("testproto", w1)
		plan := &ClassificationPlan{
			Matches: map[types.HashBytea]string{w1: "testproto"},
			perValidator: []validatorPlan{{
				validator: rv,
				matched:   map[types.HashBytea]struct{}{w1: {}},
				contracts: []ContractCandidate{{ContractID: c1, WasmHash: w1}},
			}},
		}

		err = svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), plan)
		require.NoError(t, err)

		// The validator's Apply ran inside the transaction.
		assert.Equal(t, 1, rv.applyCalls)

		// The persisted wasm row carries the plan's verdict.
		var protocolID *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT protocol_id FROM protocol_wasms WHERE wasm_hash = $1`, w1Raw[:]).Scan(&protocolID))
		require.NotNil(t, protocolID)
		assert.Equal(t, "testproto", *protocolID)

		// The contract row was persisted bound to the classified wasm.
		var storedWasmHash []byte
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, c1Raw[:]).Scan(&storedWasmHash))
		assert.Equal(t, w1Raw[:], storedWasmHash)

		// The buffered contract, classified as this protocol, reached the
		// processor's staging input through getEffectiveProtocolContracts.
		require.Equal(t, 1, processor.processLedgerCalls)
		require.Len(t, processor.lastContracts, 1)
		assert.Equal(t, c1, processor.lastContracts[0].ContractID)
	})

	t.Run("upgrade away from the protocol drops committed membership and rebinds the contract row", func(t *testing.T) {
		var wOldRaw, w2Raw, c2Raw [32]byte
		wOldRaw[0], w2Raw[0], c2Raw[0] = 0xA0, 0xA2, 0xC2
		w2 := types.HashBytea(hex.EncodeToString(w2Raw[:]))
		c2 := types.HashBytea(hex.EncodeToString(c2Raw[:]))

		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// protocol_wasms.protocol_id is an FK into protocols.
		_, err := pool.Exec(ctx, `INSERT INTO protocols (id) VALUES ('testproto'), ('otherproto')`)
		require.NoError(t, err)

		// Committed state from earlier ledgers: c2 is a testproto contract via wOld.
		_, err = pool.Exec(ctx,
			`INSERT INTO protocol_wasms (wasm_hash, protocol_id) VALUES ($1, 'testproto')`, wOldRaw[:])
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`INSERT INTO protocol_contracts (contract_id, wasm_hash) VALUES ($1, $2)`, c2Raw[:], wOldRaw[:])
		require.NoError(t, err)

		// This ledger: c2 emits an event and rebinds to w2, which the plan
		// classifies as a different protocol.
		buffer := indexer.NewIndexerBuffer()
		buffer.PushProtocolWasm(data.ProtocolWasms{WasmHash: w2})
		buffer.PushProtocolContracts(data.ProtocolContracts{ContractID: c2, WasmHash: w2})
		eventContractID := xdr.ContractId(c2Raw)
		buffer.PushContractEvents(
			indexer.ContractEventKey{TxIdx: 0, OpIdx: 0},
			[]xdr.ContractEvent{{Type: xdr.ContractEventTypeContract, ContractId: &eventContractID}},
		)

		plan := &ClassificationPlan{Matches: map[types.HashBytea]string{w2: "otherproto"}}

		err = svc.persistLedgerData(ctx, oneLedger(100, newContractDataMemo(nil, 100), buffer), plan)
		require.NoError(t, err)

		// The processor staged the ledger but saw no testproto contracts: the
		// committed membership was dropped because c2's binding changed this
		// ledger, and its new wasm belongs to another protocol.
		require.Equal(t, 1, processor.processLedgerCalls)
		assert.Empty(t, processor.lastContracts)

		// The contract row now points at the new wasm, whose row carries the
		// plan's verdict for the other protocol.
		var storedWasmHash []byte
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, c2Raw[:]).Scan(&storedWasmHash))
		assert.Equal(t, w2Raw[:], storedWasmHash)

		var protocolID *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT protocol_id FROM protocol_wasms WHERE wasm_hash = $1`, w2Raw[:]).Scan(&protocolID))
		require.NotNil(t, protocolID)
		assert.Equal(t, "otherproto", *protocolID)
	})

	t.Run("mid-batch re-observation of a committed contract keeps its membership", func(t *testing.T) {
		var w3Raw, c3Raw [32]byte
		w3Raw[0], c3Raw[0] = 0xA3, 0xC3
		w3 := types.HashBytea(hex.EncodeToString(w3Raw[:]))
		c3 := types.HashBytea(hex.EncodeToString(c3Raw[:]))

		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		_, err := pool.Exec(ctx, `INSERT INTO protocols (id) VALUES ('testproto')`)
		require.NoError(t, err)

		// Committed state from earlier ledgers: c3 is a testproto contract via w3.
		_, err = pool.Exec(ctx,
			`INSERT INTO protocol_wasms (wasm_hash, protocol_id) VALUES ($1, 'testproto')`, w3Raw[:])
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`INSERT INTO protocol_contracts (contract_id, wasm_hash) VALUES ($1, $2)`, c3Raw[:], w3Raw[:])
		require.NoError(t, err)

		// A two-ledger batch. The mid-batch ledger (101) re-observes c3 — any
		// instance change (TTL bump, restore, storage write) buffers it again
		// with its unchanged binding — and c3 emits an event. The batch plan
		// resolves w3 from its committed verdict, so the re-observation must
		// not evict c3's membership or its events are silently dropped.
		headBuffer := indexer.NewIndexerBuffer()
		midBuffer := indexer.NewIndexerBuffer()
		midBuffer.PushProtocolContracts(data.ProtocolContracts{ContractID: c3, WasmHash: w3})
		eventContractID := xdr.ContractId(c3Raw)
		midBuffer.PushContractEvents(
			indexer.ContractEventKey{TxIdx: 0, OpIdx: 0},
			[]xdr.ContractEvent{{Type: xdr.ContractEventTypeContract, ContractId: &eventContractID}},
		)

		batch := []processedLedger{
			{seq: 100, contractData: newContractDataMemo(nil, 100), buffer: headBuffer},
			{seq: 101, contractData: newContractDataMemo(nil, 101), buffer: midBuffer},
		}
		plan, err := svc.prepareBatchClassificationPlan(ctx, batch)
		require.NoError(t, err)

		require.NoError(t, svc.persistLedgerData(ctx, batch, plan))

		// Both ledgers staged; the mid-batch ledger (the last ProcessLedger
		// call) still saw c3 as a testproto contract.
		require.Equal(t, 2, processor.processLedgerCalls)
		require.Len(t, processor.lastContracts, 1)
		assert.Equal(t, c3, processor.lastContracts[0].ContractID)
	})
}

// Test_ingestService_ingestLiveLedgers_LagReadDoesNotBlockConsumer is a regression test for a
// deadlock on the datastore backend: the lag-metric read (GetLatestLedgerSequence) contends on the
// ledger buffer's internal lock, which a download worker can hold while blocked on a full queue.
// Reading it on the consumer goroutine — the only goroutine that drains that queue — deadlocks
// ingestion. The consumer must keep advancing even while the lag read is blocked indefinitely.
func Test_ingestService_ingestLiveLedgers_LagReadDoesNotBlockConsumer(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	// The guarded latest cursor (see IngestStoreModel.UpdateGuarded) requires the current DB
	// value to be startLedger-1 or startLedger, matching the real startLiveIngestion precondition
	// (initializeCursors/the previous ledger's write always leave the cursor one behind the next
	// ledger to process).
	const startLedger = uint32(51) // not a multiple of oldestLedgerSyncInterval (100)
	setupDBCursors(t, ctx, pool, startLedger-1, startLedger-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // trustlineChangesByTrustlineKey
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessSACBalanceChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // sacBalanceChangesByKey
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // accountChangesByAccountID
		mock.Anything, // lpShareChangesByKey
		mock.Anything, // lpChangesByPoolID
	).Return(nil).Maybe()

	// GetLedger always returns an empty (tx-less) ledger, so each iteration persists only the
	// cursor. GetLatestLedgerSequence blocks until the context is cancelled, standing in for a
	// download worker that holds the buffer lock while blocked on a full queue.
	mockBackend := &LedgerBackendMock{}
	mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(dummyLedgerMeta(1), nil).Maybe()
	mockBackend.On("GetLatestLedgerSequence", mock.Anything).
		Run(func(mock.Arguments) { <-ctx.Done() }).
		Return(uint32(0), context.Canceled).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:         IngestionModeLive,
		Models:                models,
		RPCService:            &RPCServiceMock{},
		LedgerBackend:         mockBackend,
		TokenIngestionService: mockTokenIngestionService,
		Metrics:               m,
		NetworkPassphrase:     network.TestNetworkPassphrase,
		Archive:               &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	// The lock probe is the shutdown path under test: a cancelled context fails
	// the real probe too (the connection refuses the query), so the probe here
	// cancels the pipeline once the consumer has advanced past the blocked lag
	// read and returns that failure, exactly as the real probe would.
	var probeCalls int
	cancellingCheckLockSession := func(probeCtx context.Context) error {
		probeCalls++
		if probeCalls == 3 {
			cancel()
		}
		return probeCtx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- svc.ingestLiveLedgers(ctx, startLedger, cancellingCheckLockSession) }()

	select {
	case runErr := <-done:
		// The consumer must have kept draining and advancing the cursor despite
		// the blocked lag read.
		var s string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&s))
		v, parseErr := strconv.ParseUint(s, 10, 32)
		require.NoError(t, parseErr)
		assert.GreaterOrEqual(t, uint32(v), startLedger+1, "consumer cursor should advance past the blocked lag read")

		// A shutdown must surface as cancellation, not as a lost advisory lock,
		// and must not count as an ingestion error.
		require.ErrorIs(t, runErr, context.Canceled)
		assert.NotContains(t, runErr.Error(), "advisory lock")
		assert.Equal(t, 0.0, testutil.ToFloat64(
			svc.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live")))
	case <-time.After(5 * time.Second):
		t.Fatal("ingestLiveLedgers did not return after context cancellation")
	}
}

// Test_ingestService_ingestLiveLedgers_DeadLockSessionExitsFatally is a regression test for
// ING-05: a CNPG failover can kill the Postgres session holding the advisory lock without this
// process observing the disconnect, silently releasing the lock while pgxpool never destroys the
// (now server-dead) pooled connection. checkLockSession is probed before every persist and, on
// failure, ingestLiveLedgers must return immediately — not after exhausting the ledger-fetch
// retry ladder (maxLedgerFetchRetries attempts, up to ~2.5 minutes) — and without having advanced
// the cursor, so the process can exit and re-acquire the lock cleanly on restart.
func Test_ingestService_ingestLiveLedgers_DeadLockSessionExitsFatally(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const startLedger = uint32(51)
	setupDBCursors(t, ctx, pool, startLedger-1, startLedger-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	// The fetch stage legitimately runs ahead of the persist-stage probe, so ledgers may be
	// fetched and processed; the probe must still stop the pipeline before anything is written.
	mockBackend := &LedgerBackendMock{}
	mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(dummyLedgerMeta(1), nil).Maybe()
	mockBackend.On("GetLatestLedgerSequence", mock.Anything).Return(uint32(0), context.Canceled).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:     IngestionModeLive,
		Models:            models,
		RPCService:        &RPCServiceMock{},
		LedgerBackend:     mockBackend,
		Metrics:           m,
		NetworkPassphrase: network.TestNetworkPassphrase,
		Archive:           &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	sessionDeadErr := fmt.Errorf("driver: bad connection")
	deadLockSession := func(context.Context) error { return sessionDeadErr }

	start := time.Now()
	runErr := svc.ingestLiveLedgers(ctx, startLedger, deadLockSession)
	elapsed := time.Since(start)

	require.Error(t, runErr)
	assert.ErrorIs(t, runErr, sessionDeadErr)
	assert.Less(t, elapsed, time.Second, "a dead lock session must fail fast, not after the ledger-fetch retry ladder")

	var cursor string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
	assert.Equal(t, strconv.FormatUint(uint64(startLedger-1), 10), cursor,
		"a failed lock probe must not let any ledger persist")
}

// Test_ingestService_ingestLiveLedgers_StageErrorStopsPipeline pins the pipeline's fail-fast
// contract: a permanent fetch error cancels every stage and ingestLiveLedgers returns it, with
// the cursor resting on the last fully persisted ledger so a restart resumes exactly there.
func Test_ingestService_ingestLiveLedgers_StageErrorStopsPipeline(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const startLedger = uint32(51)
	setupDBCursors(t, ctx, pool, startLedger-1, startLedger-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessSACBalanceChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()

	cursorReached := func(target uint32) bool {
		var s string
		if qErr := pool.QueryRow(context.Background(),
			`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&s); qErr != nil {
			return false
		}
		v, parseErr := strconv.ParseUint(s, 10, 32)
		return parseErr == nil && uint32(v) >= target
	}

	permanentFetchErr := fmt.Errorf("ledger source is permanently gone")
	mockBackend := &LedgerBackendMock{}
	mockBackend.On("GetLedger", mock.Anything, startLedger).Return(dummyLedgerMeta(1), nil).Once()
	mockBackend.On("GetLedger", mock.Anything, startLedger+1).Return(dummyLedgerMeta(1), nil).Once()
	// The failing fetch waits for ledger startLedger+1 to be fully persisted first, so the
	// pipeline's final state is deterministic: without this, the cancellation could beat the
	// in-flight persists and the cursor could rest on any of the earlier ledgers.
	mockBackend.On("GetLedger", mock.Anything, startLedger+2).Run(func(mock.Arguments) {
		for i := 0; i < 500 && !cursorReached(startLedger+1); i++ {
			time.Sleep(10 * time.Millisecond)
		}
	}).Return(xdr.LedgerCloseMeta{}, permanentFetchErr).Once()
	mockBackend.On("GetLatestLedgerSequence", mock.Anything).Return(uint32(0), context.Canceled).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:         IngestionModeLive,
		Models:                models,
		RPCService:            &RPCServiceMock{},
		LedgerBackend:         mockBackend,
		TokenIngestionService: mockTokenIngestionService,
		Metrics:               m,
		NetworkPassphrase:     network.TestNetworkPassphrase,
		Archive:               &HistoryArchiveMock{},
		IsPermanentFetchError: func(err error) bool { return errors.Is(err, permanentFetchErr) },
	})
	require.NoError(t, err)

	noopCheckLockSession := func(context.Context) error { return nil }
	runErr := svc.ingestLiveLedgers(ctx, startLedger, noopCheckLockSession)

	require.Error(t, runErr)
	assert.ErrorIs(t, runErr, permanentFetchErr)

	var cursor string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
	assert.Equal(t, strconv.FormatUint(uint64(startLedger+1), 10), cursor,
		"the cursor must rest on the last fully persisted ledger")
}

// Test_persistLedgerData_SiblingFailureRollsBackEverything pins the commit barrier's pre-commit
// contract: when one sibling COPY fails (here: a primary-key collision on transactions), no
// sibling and no coordinated write may be durable — commits are held until every stream
// succeeds, so a pre-commit failure leaves the database exactly as it was and the ledger fully
// retryable.
func Test_persistLedgerData_Batch(t *testing.T) {
	setupTest := func(t *testing.T, processors ...ProtocolProcessor) (context.Context, *ingestService, *pgxpool.Pool) {
		t.Helper()
		dbt := dbtest.Open(t)
		t.Cleanup(func() { dbt.Close() })
		ctx := context.Background()
		pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })

		m := metrics.NewMetrics(prometheus.NewRegistry())
		models, err := data.NewModels(pool, m.DB)
		require.NoError(t, err)

		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges",
			mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessSACBalanceChanges",
			mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:         IngestionModeLive,
			Models:                models,
			RPCService:            &RPCServiceMock{},
			LedgerBackend:         &LedgerBackendMock{},
			TokenIngestionService: mockTokenIngestionService,
			Metrics:               m,
			NetworkPassphrase:     network.TestNetworkPassphrase,
			Archive:               &HistoryArchiveMock{},
			ProtocolProcessors:    processors,
		})
		require.NoError(t, err)
		return ctx, svc, pool
	}

	// batchItem builds one ledger's persist payload carrying a transaction
	// and an operation with ledger-derived TOIDs, so PKs never collide
	// across the batch.
	batchItem := func(seq uint32) processedLedger {
		toID := int64(seq) << 32
		tx := createTestTransaction(fmt.Sprintf("%064x", seq), toID)
		op := createTestOperation(toID + 1)
		buffer := indexer.NewIndexerBuffer()
		buffer.PushTransaction(testAddr1, &tx)
		buffer.PushOperation(testAddr1, &op, &tx)
		return processedLedger{seq: seq, contractData: newContractDataMemo(nil, seq), buffer: buffer}
	}

	t.Run("one commit set persists every ledger and the cursor lands on the last", func(t *testing.T) {
		ctx, svc, pool := setupTest(t)
		setupDBCursors(t, ctx, pool, 99, 99)

		err := svc.persistLedgerData(ctx, []processedLedger{batchItem(100), batchItem(101), batchItem(102)}, nil)
		require.NoError(t, err)

		var txCount, opCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&txCount))
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM operations`).Scan(&opCount))
		assert.Equal(t, 3, txCount)
		assert.Equal(t, 3, opCount)

		var cursor string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
		assert.Equal(t, "102", cursor, "the cursor must land on the batch's last ledger")
	})

	t.Run("a failure in any ledger rolls back the whole batch", func(t *testing.T) {
		ctx, svc, pool := setupTest(t)
		setupDBCursors(t, ctx, pool, 99, 99)

		items := []processedLedger{batchItem(100), batchItem(101), batchItem(102)}

		// Pre-insert a row with ledger 102's transaction PK so the
		// transactions sibling fails only on the batch's last ledger, after
		// the earlier ledgers streamed successfully.
		last := items[2].buffer.GetTransactions()[0]
		_, err := pool.Exec(ctx,
			`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
			 VALUES ($1, $2, 1, 'TransactionResultCodeTxSuccess', $3, $4)`,
			[]byte("preexisting-hash"), last.ToID, 102, last.LedgerCreatedAt)
		require.NoError(t, err)

		err = svc.persistLedgerData(ctx, items, nil)
		require.Error(t, err)

		var txCount, opCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&txCount))
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM operations`).Scan(&opCount))
		assert.Equal(t, 1, txCount, "only the pre-existing row may remain — no ledger of the batch may be durable")
		assert.Equal(t, 0, opCount, "the operations sibling streamed successfully but must not have committed")

		var cursor string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
		assert.Equal(t, "99", cursor, "the cursor must stay below the whole batch")
	})

	t.Run("a mid-batch ledger's membership sees contracts classified at the batch head", func(t *testing.T) {
		var wRaw, cRaw [32]byte
		wRaw[0], cRaw[0] = 0xA7, 0xC7
		wasmHash := types.HashBytea(hex.EncodeToString(wRaw[:]))
		contractHex := types.HashBytea(hex.EncodeToString(cRaw[:]))

		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, pool := setupTest(t, processor)
		processor.ingestStore = svc.models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// protocol_wasms.protocol_id is an FK into protocols.
		_, err := pool.Exec(ctx, `INSERT INTO protocols (id) VALUES ('testproto')`)
		require.NoError(t, err)

		// The batch head uploads the wasm and deploys contract C, classified to
		// testproto by the batch's plan.
		head := batchItem(100)
		head.buffer.PushProtocolWasm(data.ProtocolWasms{WasmHash: wasmHash})
		head.buffer.PushProtocolContracts(data.ProtocolContracts{ContractID: contractHex, WasmHash: wasmHash})
		plan := &ClassificationPlan{Matches: map[types.HashBytea]string{wasmHash: "testproto"}}

		// The mid-batch ledger carries only an event from C: nothing re-buffers
		// the contract, so its membership can come only from the rows the head
		// staged on the still-uncommitted coordinating transaction.
		mid := batchItem(101)
		contractID := xdr.ContractId(cRaw)
		mid.buffer.PushContractEvents(
			indexer.ContractEventKey{TxIdx: 0, OpIdx: 0},
			[]xdr.ContractEvent{{Type: xdr.ContractEventTypeContract, ContractId: &contractID}},
		)

		require.NoError(t, svc.persistLedgerData(ctx, []processedLedger{head, mid}, plan))

		// ProcessLedger ran last for the mid-batch ledger; its membership must
		// include the head-classified contract.
		assert.Equal(t, uint32(101), processor.processedLedger)
		require.Len(t, processor.lastContracts, 1)
		assert.Equal(t, contractHex, processor.lastContracts[0].ContractID)
	})
}

func Test_persistLedgerData_SiblingFailureRollsBackEverything(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const ledgerSeq = uint32(100)
	setupDBCursors(t, ctx, pool, ledgerSeq-1, ledgerSeq-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessSACBalanceChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:         IngestionModeLive,
		Models:                models,
		RPCService:            &RPCServiceMock{},
		LedgerBackend:         &LedgerBackendMock{},
		TokenIngestionService: mockTokenIngestionService,
		Metrics:               m,
		NetworkPassphrase:     network.TestNetworkPassphrase,
		Archive:               &HistoryArchiveMock{},
	})
	require.NoError(t, err)
	ingestSvc := svc

	tx := createTestTransaction("collision-hash", 100)
	op := createTestOperation(101)
	buffer := indexer.NewIndexerBuffer()
	buffer.PushTransaction(testAddr1, &tx)
	buffer.PushOperation(testAddr1, &op, &tx)

	// Pre-insert a row with the transaction's exact primary key (to_id, ledger_created_at) so
	// the transactions sibling's COPY fails after the other siblings have streamed their rows.
	_, err = pool.Exec(ctx,
		`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
		 VALUES ($1, $2, 1, 'TransactionResultCodeTxSuccess', $3, $4)`,
		[]byte("preexisting-hash"), tx.ToID, ledgerSeq, tx.LedgerCreatedAt)
	require.NoError(t, err)

	err = ingestSvc.persistLedgerData(ctx, oneLedger(ledgerSeq, newContractDataMemo(nil, ledgerSeq), buffer), nil)
	require.Error(t, err)

	var txCount, opCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&txCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM operations`).Scan(&opCount))
	assert.Equal(t, 1, txCount, "only the pre-existing row may remain — the failed COPY must not be durable")
	assert.Equal(t, 0, opCount, "the operations sibling streamed successfully but must not have committed")

	var cursor string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
	assert.Equal(t, strconv.FormatUint(uint64(ledgerSeq-1), 10), cursor,
		"the coordinating transaction (and its cursor update) must roll back with the siblings")
}

// Test_persistLedgerData_FKParentsCommitWithTheirChildren drives the real
// token ingestion service through persistLedgerData and checks that each
// balance table and its foreign-key parent land together with the cursor.
// It proves the coordinating transaction writes valid parent/child pairs;
// keeping those tables off the siblings is pinned by Test_persistSiblings_Set.
func Test_persistLedgerData_FKParentsCommitWithTheirChildren(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const ledgerSeq = uint32(100)
	setupDBCursors(t, ctx, pool, ledgerSeq-1, ledgerSeq-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode: IngestionModeLive,
		Models:        models,
		RPCService:    &RPCServiceMock{},
		LedgerBackend: &LedgerBackendMock{},
		TokenIngestionService: NewTokenIngestionService(TokenIngestionServiceConfig{
			TrustlineBalanceModel:     models.TrustlineBalance,
			NativeBalanceModel:        models.NativeBalance,
			SACBalanceModel:           models.SACBalance,
			LiquidityPoolModel:        models.LiquidityPool,
			LiquidityPoolBalanceModel: models.LiquidityPoolBalance,
			NetworkPassphrase:         network.TestNetworkPassphrase,
		}),
		Metrics:           m,
		NetworkPassphrase: network.TestNetworkPassphrase,
		Archive:           &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	tx := createTestTransaction(strings.Repeat("cd", 32), 100)
	op := createTestOperation(101)
	buffer := indexer.NewIndexerBuffer()
	buffer.PushTransaction(testAddr1, &tx)
	buffer.PushOperation(testAddr1, &op, &tx)

	// Both parents are new to this ledger: neither trustline_assets nor
	// liquidity_pools holds a row before the persist.
	poolID := strings.Repeat("ab", 32)
	buffer.PushTrustlineChange(types.TrustlineChange{
		AccountID: testAddr1, Asset: "USDC:" + testAddr2, OperationID: op.ID,
		LedgerNumber: ledgerSeq, Operation: types.TrustlineOpAdd, Balance: 10, Limit: 100,
	})
	buffer.PushLiquidityPoolChange(types.LiquidityPoolChange{
		PoolID: poolID, OperationID: op.ID, LedgerNumber: ledgerSeq, Operation: types.LiquidityPoolOpAdd,
		AssetA: "native", ReserveA: 5, AssetB: "USDC:" + testAddr2, ReserveB: 7,
	})
	buffer.PushLiquidityPoolShareChange(types.LiquidityPoolShareChange{
		AccountID: testAddr1, PoolID: poolID, OperationID: op.ID,
		LedgerNumber: ledgerSeq, Operation: types.LiquidityPoolShareOpAdd, Shares: 3,
	})

	err = svc.persistLedgerData(ctx, oneLedger(ledgerSeq, newContractDataMemo(nil, ledgerSeq), buffer), nil)
	require.NoError(t, err)

	count := func(query string, args ...any) int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, query, args...).Scan(&n))
		return n
	}
	assetID := data.DeterministicAssetID("USDC", testAddr2)
	assert.Equal(t, 1, count(`SELECT count(*) FROM trustline_assets WHERE id = $1`, assetID))
	assert.Equal(t, 1, count(`SELECT count(*) FROM trustline_balances WHERE asset_id = $1`, assetID))
	assert.Equal(t, 1, count(`SELECT count(*) FROM liquidity_pools WHERE pool_id = $1`, poolID))
	assert.Equal(t, 1, count(`SELECT count(*) FROM liquidity_pool_balances WHERE pool_id = $1`, poolID))

	var cursor string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&cursor))
	assert.Equal(t, strconv.FormatUint(uint64(ledgerSeq), 10), cursor)
}

// Test_persistLedgerData_FKViolationCommitsNothing pins that a deferred
// foreign-key violation is caught before the commit barrier: the batch rolls
// back whole with a permanent 23503, not ErrPartialPersist after the siblings
// have committed.
func Test_persistLedgerData_FKViolationCommitsNothing(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	const ledgerSeq = uint32(100)
	setupDBCursors(t, ctx, pool, ledgerSeq-1, ledgerSeq-1)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	// A trustline balance whose trustline_assets parent was never written.
	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			upsertErr := models.TrustlineBalance.BatchUpsert(args.Get(0).(context.Context), args.Get(1).(pgx.Tx), []data.TrustlineBalance{{
				AccountID: types.AddressBytea(testAddr1), AssetID: uuid.New(), Balance: 1, Limit: 10, LedgerNumber: ledgerSeq,
			}}, nil)
			require.NoError(t, upsertErr, "the deferred FK must not fire at insert time")
		}).Return(nil)
	mockTokenIngestionService.On("ProcessSACBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:         IngestionModeLive,
		Models:                models,
		RPCService:            &RPCServiceMock{},
		LedgerBackend:         &LedgerBackendMock{},
		TokenIngestionService: mockTokenIngestionService,
		Metrics:               m,
		NetworkPassphrase:     network.TestNetworkPassphrase,
		Archive:               &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	tx := createTestTransaction(strings.Repeat("ef", 32), 100)
	buffer := indexer.NewIndexerBuffer()
	buffer.PushTransaction(testAddr1, &tx)

	err = svc.persistLedgerData(ctx, oneLedger(ledgerSeq, newContractDataMemo(nil, ledgerSeq), buffer), nil)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPartialPersist)
	assert.True(t, isPermanentPersistError(err), "a foreign-key violation is permanent: %v", err)

	var txCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&txCount))
	assert.Zero(t, txCount, "no sibling may commit when the coordinator's foreign keys fail")
}

// Test_prepareBatchClassificationPlan_MergesBatchInputs pins the property that
// removed the persist batch cut: a contract bound to a wasm uploaded by an
// EARLIER ledger of the same batch is classified from that wasm's buffered
// bytecode, never from protocol_wasms.
//
// Per ledger, ledger N+1's plan would have to resolve the wasm through
// GetClassifiedByHashes — a pool read that cannot see the row ledger N staged
// on the uncommitted batch — and would classify the contract against a miss.
// Merging the batch's inputs into one plan puts the wasm in thisBatch instead.
// The mock's argument matcher is the assertion: the wasm must never reach the
// database read.
func Test_prepareBatchClassificationPlan_MergesBatchInputs(t *testing.T) {
	ctx := context.Background()

	var wRaw, cRaw [32]byte
	wRaw[0], cRaw[0] = 0xB1, 0xD1
	wasmHash := types.HashBytea(hex.EncodeToString(wRaw[:]))
	contractID := types.HashBytea(hex.EncodeToString(cRaw[:]))

	// Ledger 100 uploads the wasm; ledger 101 binds a contract to it.
	upload := indexer.NewIndexerBuffer()
	upload.PushProtocolWasm(data.ProtocolWasms{WasmHash: wasmHash})
	upload.PushProtocolWasmBytecode(string(wasmHash), []byte{1, 2, 3})

	deploy := indexer.NewIndexerBuffer()
	deploy.PushProtocolContracts(data.ProtocolContracts{ContractID: contractID, WasmHash: wasmHash})

	wasmsMock := data.NewProtocolWasmsModelMock(t)
	wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything,
		mock.MatchedBy(func(hashes []types.HashBytea) bool {
			// The batch's own upload must not be looked up in the database.
			return len(hashes) == 0
		}),
	).Return(map[types.HashBytea]string{}, nil).Once()

	rv := newRecordingValidator("A", wasmHash)
	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

	svc := &ingestService{
		models:             &data.Models{ProtocolWasms: wasmsMock},
		appMetrics:         metrics.NewMetrics(prometheus.NewRegistry()),
		protocolValidators: []ProtocolValidator{rv},
		wasmSpecExtractor:  extractor,
		rpcService:         &RPCServiceMock{},
	}

	plan, err := svc.prepareBatchClassificationPlan(ctx, []processedLedger{
		{seq: 100, buffer: upload},
		{seq: 101, buffer: deploy},
	})
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "A", plan.Matches[wasmHash],
		"the batch's own upload must be classified from its buffered bytecode")
}

// Test_prepareBatchClassificationPlan_LastBindingWins pins the merge rule for a
// contract rebound inside one batch: the contract itself ends bound to its
// final wasm, but the plan resolves BOTH hashes, because the earlier ledger
// stages its protocol events against the binding it saw.
func Test_prepareBatchClassificationPlan_LastBindingWins(t *testing.T) {
	ctx := context.Background()

	var w1Raw, w2Raw, cRaw [32]byte
	w1Raw[0], w2Raw[0], cRaw[0] = 0xB2, 0xB3, 0xD2
	w1 := types.HashBytea(hex.EncodeToString(w1Raw[:]))
	w2 := types.HashBytea(hex.EncodeToString(w2Raw[:]))
	contractID := types.HashBytea(hex.EncodeToString(cRaw[:]))

	first := indexer.NewIndexerBuffer()
	first.PushProtocolContracts(data.ProtocolContracts{ContractID: contractID, WasmHash: w1})
	second := indexer.NewIndexerBuffer()
	second.PushProtocolContracts(data.ProtocolContracts{ContractID: contractID, WasmHash: w2})

	// The surviving binding's wasm and the superseded one are both resolved.
	wasmsMock := data.NewProtocolWasmsModelMock(t)
	wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything,
		mock.MatchedBy(func(hashes []types.HashBytea) bool {
			return len(hashes) == 2 && hashes[0] == w2 && hashes[1] == w1
		}),
	).Return(map[types.HashBytea]string{w1: "A", w2: "B"}, nil).Once()

	svc := &ingestService{
		models:     &data.Models{ProtocolWasms: wasmsMock},
		appMetrics: metrics.NewMetrics(prometheus.NewRegistry()),
	}

	plan, err := svc.prepareBatchClassificationPlan(ctx, []processedLedger{
		{seq: 100, buffer: first},
		{seq: 101, buffer: second},
	})
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, map[types.HashBytea]string{w1: "A", w2: "B"}, plan.Matches,
		"the superseded binding must stay classified for the ledger that saw it")
}

// Test_prepareBatchClassificationPlan_SupersededBindingIsClaimed pins the
// validator-side half of the rebind rule: the earlier ledger of the batch
// stages its protocol events against the superseded binding, so that binding
// must reach the validators as a contract candidate too, or Apply never writes
// the contract_tokens row those events' current-state rows reference.
func Test_prepareBatchClassificationPlan_SupersededBindingIsClaimed(t *testing.T) {
	ctx := context.Background()

	var w1Raw, w2Raw, cRaw [32]byte
	w1Raw[0], w2Raw[0], cRaw[0] = 0xB4, 0xB5, 0xD3
	w1 := types.HashBytea(hex.EncodeToString(w1Raw[:]))
	w2 := types.HashBytea(hex.EncodeToString(w2Raw[:]))
	contractID := types.HashBytea(hex.EncodeToString(cRaw[:]))

	first := indexer.NewIndexerBuffer()
	first.PushProtocolContracts(data.ProtocolContracts{ContractID: contractID, WasmHash: w1})
	second := indexer.NewIndexerBuffer()
	second.PushProtocolContracts(data.ProtocolContracts{ContractID: contractID, WasmHash: w2})

	wasmsMock := data.NewProtocolWasmsModelMock(t)
	wasmsMock.On("GetClassifiedByHashes", mock.Anything, mock.Anything, mock.Anything).
		Return(map[types.HashBytea]string{w1: "A"}, nil).Once()

	rv := newRecordingValidator("A")
	svc := &ingestService{
		models:             &data.Models{ProtocolWasms: wasmsMock},
		appMetrics:         metrics.NewMetrics(prometheus.NewRegistry()),
		protocolValidators: []ProtocolValidator{rv},
		wasmSpecExtractor:  NewWasmSpecExtractorMock(t),
		rpcService:         &RPCServiceMock{},
	}

	plan, err := svc.prepareBatchClassificationPlan(ctx, []processedLedger{
		{seq: 100, buffer: first},
		{seq: 101, buffer: second},
	})
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.ElementsMatch(t, []ContractCandidate{
		{ContractID: contractID, WasmHash: w1, KnownProtocolID: "A"},
		{ContractID: contractID, WasmHash: w2},
	}, rv.lastContracts, "the validator must see the superseded binding as a candidate")
}

// Test_persistSiblings_Set pins the sibling set to exactly the bulk-COPY
// tables, the list startup reconciliation deletes orphans from: a table
// streamed by a sibling but absent from it keeps rows above the cursor after
// a crash, and re-ingesting collides on primary keys COPY cannot resolve.
// Current state never rides a sibling; it commits with the cursor.
func Test_persistSiblings_Set(t *testing.T) {
	var mu sync.Mutex
	siblings := (&ingestService{}).persistSiblings(&mu)
	names := make([]string, len(siblings))
	for i, s := range siblings {
		names[i] = s.name
	}
	require.ElementsMatch(t, data.BulkCopyTableNames(), names)
	// The pool floor is the barrier's held connections (every sibling plus the
	// coordinator) plus the advisory-lock session. Tie it to the sibling list so
	// adding a sibling cannot leave the floor stale, which wedges silently.
	require.EqualValues(t, len(siblings)+2, db.MinIngestMaxConns)
}
