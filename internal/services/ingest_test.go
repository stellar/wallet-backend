package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	defaultGetLedgersLimit = 50

	// Test hash constants for ingest tests (64-char hex strings for BYTEA storage)
	flushTxHash1 = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101"
	flushTxHash2 = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f202"
	flushTxHash3 = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f303"
	flushTxHash4 = "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f404"
	txHash1      = "1111111111111111111111111111111111111111111111111111111111111111"
	txHash2      = "2222222222222222222222222222222222222222222222222222222222222222"
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

func Test_ingestService_calculateBackfillGaps(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name         string
		startLedger  uint32
		endLedger    uint32
		setupDB      func(t *testing.T)
		expectedGaps []data.LedgerRange
	}{
		{
			name:        "entirely_before_oldest_ingested_ledger",
			startLedger: 50,
			endLedger:   80,
			setupDB: func(t *testing.T) {
				// Actual oldest ledger in transactions is 100
				_, err := dbConnectionPool.Exec(ctx,
					`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
					VALUES ('anchor_hash', 1, 100, 'TransactionResultCodeTxSuccess', 100, NOW())`)
				require.NoError(t, err)
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 80},
			},
		},
		{
			name:        "overlaps_with_oldest_ingested_ledger",
			startLedger: 50,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				// Insert transactions for 100-200 (no gaps); oldest is 100
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 99},
			},
		},
		{
			name:        "entirely_within_ingested_range_no_gaps",
			startLedger: 110,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				// Insert transactions for 100-200 (no gaps); oldest is 100
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{},
		},
		{
			name:        "entirely_within_ingested_range_with_gaps",
			startLedger: 110,
			endLedger:   180,
			setupDB: func(t *testing.T) {
				// Insert transactions with gaps: 100-120, 150-200 (gap at 121-149); oldest is 100
				for ledger := uint32(100); ledger <= 120; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
				for ledger := uint32(150); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 121, GapEnd: 149},
			},
		},
		{
			// After TimescaleDB retention drops old chunks, the stored oldest_ledger_cursor
			// may point to a ledger that no longer exists in the transactions table.
			// calculateBackfillGaps should ignore the cursor and use the actual oldest
			// ledger from the transactions table.
			name:        "stale_cursor_is_ignored_uses_actual_oldest_from_transactions",
			startLedger: 50,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				// Cursor claims oldest is 50 (stale — retention dropped ledgers 50-99)
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 50)`)
				require.NoError(t, err)
				// Actual transactions only exist from 100 onwards
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			// Oldest in transactions is 100, so gap [50,99] should be backfilled
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 99},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &mockAppTracker,
				RPCService:             &mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
			})
			require.NoError(t, err)

			gaps, err := svc.calculateBackfillGaps(ctx, tc.startLedger, tc.endLedger)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
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

func Test_startBackfilling_Validation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		startLedger    uint32
		endLedger      uint32
		latestIngested uint32
		errorContains  string
	}{
		{
			name:           "valid_range_fails_fast_on_backend_error",
			startLedger:    50,
			endLedger:      80,
			latestIngested: 100,
			errorContains:  "creating ledger backend",
		},
		{
			name:           "end_exceeds_latest",
			startLedger:    50,
			endLedger:      150,
			latestIngested: 100,
			errorContains:  "end ledger 150 cannot be greater than latest ingested ledger 100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			// Set up latest ingested ledger cursor
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
				data.LatestLedgerCursorName, fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)

			// Required for the valid-range/fail-fast subtest: anchoring the oldest ingested
			// ledger makes GetOldestLedger return latestIngested so a real gap forms and the
			// backend factory is reached.
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
				VALUES ('anchor_hash', 1, 100, 'TransactionResultCodeTxSuccess', $1, NOW())`,
				tc.latestIngested)
			require.NoError(t, err)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}

			// Create a mock ledger backend factory that returns an error immediately.
			// This lets validation + gap calculation pass, then the pipeline fails fast on backend creation.
			mockBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("mock backend factory error")
			}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &mockAppTracker,
				RPCService:             &mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				LedgerBackendFactory:   mockBackendFactory,
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
			})
			require.NoError(t, err)

			err = svc.startBackfilling(ctx, tc.startLedger, tc.endLedger)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errorContains)
		})
	}
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
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`, fmt.Sprintf("%d", oldestLedger))
		require.NoError(t, err)
	}
}

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
				IngestionMode:          IngestionModeLive,
				Models:                 models,
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
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

			oldestCursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
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
				IngestionMode:          tc.mode,
				Models:                 models,
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			err = svc.Run(ctx, 100, 200)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrContains)
		})
	}
}

// Test_ingestProcessedDataWithRetry tests the ingestProcessedDataWithRetry function covering success, failure, and retry scenarios.
func Test_ingestProcessedDataWithRetry(t *testing.T) {
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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
			mock.Anything, // contractChanges
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
		).Return(nil)

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			TokenIngestionService:  mockTokenIngestionService,
			Metrics:                m,
			GetLedgersLimit:        defaultGetLedgersLimit,
			Network:                network.TestNetworkPassphrase,
			NetworkPassphrase:      network.TestNetworkPassphrase,
			Archive:                &HistoryArchiveMock{},
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

		// Call ingestProcessedDataWithRetry - should succeed
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, buffer)

		// Verify success
		require.NoError(t, err)
		assert.Equal(t, 0, numTx) // No transactions in buffer
		assert.Equal(t, 0, numOps)

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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
			mock.Anything, // contractChanges
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
		).Return(fmt.Errorf("db connection failed"))

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			TokenIngestionService:  mockTokenIngestionService,
			Metrics:                m,
			GetLedgersLimit:        defaultGetLedgersLimit,
			Network:                network.TestNetworkPassphrase,
			NetworkPassphrase:      network.TestNetworkPassphrase,
			Archive:                &HistoryArchiveMock{},
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

		// Call ingestProcessedDataWithRetry - should fail after retries due to DB error
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		_, _, err = svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, buffer)

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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(fmt.Errorf("transient error")).Once()
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			TokenIngestionService:  mockTokenIngestionService,
			Metrics:                m,
			GetLedgersLimit:        defaultGetLedgersLimit,
			Network:                network.TestNetworkPassphrase,
			NetworkPassphrase:      network.TestNetworkPassphrase,
			Archive:                &HistoryArchiveMock{},
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

		// Call ingestProcessedDataWithRetry - should succeed after retry
		// Note: assetIDMap and contractIDMap are no longer passed - operations use direct DB queries
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, buffer)

		// Verify success after retry
		require.NoError(t, err)
		assert.Equal(t, 0, numTx)
		assert.Equal(t, 0, numOps)

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
}

func (p *testProtocolProcessor) ProtocolID() string { return p.id }

func (p *testProtocolProcessor) Reset() { p.stagedLedgerCount = 0 }

func (p *testProtocolProcessor) ProcessLedger(_ context.Context, input ProtocolProcessorInput) error {
	p.processLedgerCalls++
	p.stagedLedgerCount++
	p.processedLedger = input.LedgerSequence
	return nil
}

func (p *testProtocolProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_written", p.id), p.processedLedger)
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

func Test_PersistLedgerData_ProtocolCASGating(t *testing.T) {
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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             &RPCServiceMock{},
			LedgerBackend:          &LedgerBackendMock{},
			TokenIngestionService:  mockTokenIngestionService,
			Metrics:                m,
			GetLedgersLimit:        defaultGetLedgersLimit,
			Network:                network.TestNetworkPassphrase,
			NetworkPassphrase:      network.TestNetworkPassphrase,
			Archive:                &HistoryArchiveMock{},
			ProtocolProcessors:     processors,
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

		buffer := indexer.NewIndexerBuffer()
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
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

		buffer := indexer.NewIndexerBuffer()
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
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

		buffer := indexer.NewIndexerBuffer()
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
		require.NoError(t, err)

		// Main cursor advances; protocol persist methods were not called and
		// protocol cursor rows remain absent.
		mainCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
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

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
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

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
		require.NoError(t, err)

		// Main cursor should advance
		mainCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), mainCursor)
	})

	t.Run("F: PersistCurrentState failure rolls back the cursor and a retry succeeds", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto", failPersistCurrentStateAt: 101}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)

		// First ledger succeeds and advances the current-state cursor to 100.
		processor.processedLedger = 100
		meta100 := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta100, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
		require.NoError(t, err)

		// Next ledger fails inside PersistCurrentState, rolling back the whole
		// transaction — the current-state cursor must stay at 100.
		processor.processedLedger = 101
		meta101 := dummyLedgerMeta(101)
		_, _, err = svc.persistLedgerData(ctx, 101, &meta101, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
		require.Error(t, err)

		currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), currentStateCursor)

		// Retrying the same ledger succeeds and advances the cursor.
		processor.failPersistCurrentStateAt = 0
		processor.processedLedger = 101
		_, _, err = svc.persistLedgerData(ctx, 101, &meta101, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
		require.NoError(t, err)

		currentStateCursor, err = models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(101), currentStateCursor)

		stagedCount, err := models.IngestStore.Get(ctx, "test_testproto_staged_count")
		require.NoError(t, err)
		assert.Equal(t, uint32(1), stagedCount) // retry re-staged cleanly; not doubled
	})

	t.Run("G: contract-id lookup failure fails the ledger", func(t *testing.T) {
		processor := &testProtocolProcessor{id: "testproto"}
		ctx, svc, models, pool := setupTest(t, []ProtocolProcessor{processor})
		processor.ingestStore = models.IngestStore
		processor.processedLedger = 100
		setupDBCursors(t, ctx, pool, 99, 99)
		setupProtocolCursors(t, ctx, pool, 99, 99)

		// Inject a failing contract-id lookup over the otherwise-real models.
		contractsMock := data.NewProtocolContractsModelMock(t)
		contractsMock.On("BatchGetByContractIDs", mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("db boom")).Once()
		models.ProtocolContracts = contractsMock

		// A contract event makes the event-contract-id set non-empty so the lookup runs.
		var contractID xdr.ContractId
		buffer := indexer.NewIndexerBuffer()
		buffer.PushContractEvents(
			indexer.ContractEventKey{TxIdx: 0, OpIdx: 0},
			[]xdr.ContractEvent{{Type: xdr.ContractEventTypeContract, ContractId: &contractID}},
		)

		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, buffer, "latest_ledger_cursor")
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
		nil,
	)
	assert.Empty(t, contracts)
}

func Test_distinctEventContractIDs(t *testing.T) {
	t.Parallel()

	var idA, idB xdr.ContractId
	idA[0] = 0xAA
	idB[0] = 0xBB
	hexA := types.HashBytea(hex.EncodeToString(idA[:]))
	hexB := types.HashBytea(hex.EncodeToString(idB[:]))

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
	assert.ElementsMatch(t, []types.HashBytea{hexA, hexB}, got)
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

	setupDBCursors(t, ctx, pool, 0, 0)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessTokenChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
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
		IngestionMode:          IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockBackend,
		TokenIngestionService:  mockTokenIngestionService,
		Metrics:                m,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	const startLedger = uint32(51) // not a multiple of oldestLedgerSyncInterval (100)
	done := make(chan error, 1)
	go func() { done <- svc.ingestLiveLedgers(ctx, startLedger) }()

	// The consumer must keep draining and advancing the cursor despite the blocked lag read.
	require.Eventually(t, func() bool {
		var s string
		if qErr := pool.QueryRow(context.Background(),
			`SELECT value FROM ingest_store WHERE key = $1`, data.LatestLedgerCursorName).Scan(&s); qErr != nil {
			return false
		}
		v, err := strconv.ParseUint(s, 10, 32)
		require.NoError(t, err)
		return uint32(v) >= startLedger+2
	}, 5*time.Second, 20*time.Millisecond, "consumer cursor should advance past the blocked lag read")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingestLiveLedgers did not return after context cancellation")
	}
}
