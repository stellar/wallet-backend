package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
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
	"github.com/stellar/wallet-backend/internal/signing/store"
)

var (
	testKP1   = keypair.MustRandom()
	testKP2   = keypair.MustRandom()
	testAddr1 = testKP1.Address()
	testAddr2 = testKP2.Address()
)

const (
	defaultGetLedgersLimit = 50

	// Test hash constants for ingest tests (64-char hex strings for BYTEA storage)
	flushTxHash1 = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101"
	flushTxHash2 = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f202"
	flushTxHash3 = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f303"
	flushTxHash4 = "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f404"

	// Test fixtures for ledger metadata
	ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	ledgerMetadataWith1Tx = "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"
)

func Test_NewIngestService_poolValidation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	// Open a pool with a very small max connections (3)
	dsn := dbt.DSN + "&pool_max_conns=3"
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dsn)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbConnectionPool, m.DB)
	require.NoError(t, err)

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	// 4 flush workers × 5 COPYs + 5 = 25 required, but pool only has 3
	_, err = NewIngestService(IngestServiceConfig{
		IngestionMode:        IngestionModeBackfill,
		Models:               models,
		AppTracker:           &apptracker.MockAppTracker{},
		RPCService:           mockRPCService,
		LedgerBackend:        &LedgerBackendMock{},
		Metrics:              m,
		Network:              network.TestNetworkPassphrase,
		NetworkPassphrase:    network.TestNetworkPassphrase,
		Archive:              &HistoryArchiveMock{},
		BackfillFlushWorkers: 4,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgxpool max connections")

	// Same config in live mode should NOT error (validation only applies to backfill)
	m2 := metrics.NewMetrics(prometheus.NewRegistry())
	models2, err := data.NewModels(dbConnectionPool, m2.DB)
	require.NoError(t, err)

	_, err = NewIngestService(IngestServiceConfig{
		IngestionMode:        IngestionModeLive,
		Models:               models2,
		AppTracker:           &apptracker.MockAppTracker{},
		RPCService:           mockRPCService,
		LedgerBackend:        &LedgerBackendMock{},
		Metrics:              m2,
		Network:              network.TestNetworkPassphrase,
		NetworkPassphrase:    network.TestNetworkPassphrase,
		Archive:              &HistoryArchiveMock{},
		BackfillFlushWorkers: 4,
	})
	require.NoError(t, err)
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
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &mockAppTracker,
				RPCService:             &mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				ChannelAccountStore:    mockChAccStore,
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
				BackfillFlushWorkers:   1,
			})
			require.NoError(t, err)

			gaps, err := svc.calculateBackfillGaps(ctx, tc.startLedger, tc.endLedger)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func Test_Backfill_Validation(t *testing.T) {
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
		expectError    bool
		errorContains  string
	}{
		{
			name:           "valid_range",
			startLedger:    50,
			endLedger:      80,
			latestIngested: 100,
			expectError:    false,
		},
		{
			name:           "end_exceeds_latest",
			startLedger:    50,
			endLedger:      150,
			latestIngested: 100,
			expectError:    true,
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
				`INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}

			// Create a mock ledger backend factory that returns an error immediately
			// This allows validation to pass but stops batch processing early
			mockBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("mock backend factory error")
			}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &mockAppTracker,
				RPCService:             &mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				LedgerBackendFactory:   mockBackendFactory,
				ChannelAccountStore:    mockChAccStore,
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
				BackfillFlushWorkers:   1,
			})
			require.NoError(t, err)

			err = svc.startBackfilling(ctx, tc.startLedger, tc.endLedger)
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				// For valid cases, validation passes but gap processing fails
				// (mock factory returns error). Pipeline logs failures and continues.
				require.NoError(t, err)
			}
		})
	}
}

// ==================== Test Helper Functions ====================

// setupDBCursors sets up ingest_store cursors for testing
func setupDBCursors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, latestLedger, oldestLedger uint32) {
	_, err := pool.Exec(ctx, `DELETE FROM ingest_store`)
	require.NoError(t, err)
	if latestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`, fmt.Sprintf("%d", latestLedger))
		require.NoError(t, err)
	}
	if oldestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`, fmt.Sprintf("%d", oldestLedger))
		require.NoError(t, err)
	}
}

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
func createTestStateChange(toID int64, accountID string, opID int64) types.StateChange {
	now := time.Now()
	reason := types.StateChangeReasonCredit
	return types.StateChange{
		ToID:                toID,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   reason,
		AccountID:           types.AddressBytea(accountID),
		OperationID:         opID,
		LedgerNumber:        1000,
		LedgerCreatedAt:     now,
		IngestedAt:          now,
	}
}

func Test_ingestService_getLedgerWithRetry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name            string
		setupBackend    func(*LedgerBackendMock)
		ctxFunc         func() (context.Context, context.CancelFunc)
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "success_on_first_try",
			setupBackend: func(lb *LedgerBackendMock) {
				var meta xdr.LedgerCloseMeta
				err := xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &meta)
				require.NoError(t, err)
				lb.On("GetLedger", mock.Anything, uint32(100)).Return(meta, nil).Once()
			},
			ctxFunc: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(ctx)
			},
			wantErr: false,
		},
		{
			name: "success_after_retries",
			setupBackend: func(lb *LedgerBackendMock) {
				var meta xdr.LedgerCloseMeta
				err := xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &meta)
				require.NoError(t, err)
				// Fail twice, then succeed
				lb.On("GetLedger", mock.Anything, uint32(100)).Return(xdr.LedgerCloseMeta{}, fmt.Errorf("temporary error")).Twice()
				lb.On("GetLedger", mock.Anything, uint32(100)).Return(meta, nil).Once()
			},
			ctxFunc: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(ctx)
			},
			wantErr: false,
		},
		{
			name: "context_cancelled_immediately",
			setupBackend: func(lb *LedgerBackendMock) {
				// May or may not be called depending on timing
				lb.On("GetLedger", mock.Anything, uint32(100)).Return(xdr.LedgerCloseMeta{}, fmt.Errorf("error")).Maybe()
			},
			ctxFunc: func() (context.Context, context.CancelFunc) {
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel() // Cancel immediately
				return cancelledCtx, cancel
			},
			wantErr:         true,
			wantErrContains: "context cancelled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockLedgerBackend := &LedgerBackendMock{}
			tc.setupBackend(mockLedgerBackend)
			defer mockLedgerBackend.AssertExpectations(t)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillFlushWorkers:   1,
			})
			require.NoError(t, err)

			testCtx, cancel := tc.ctxFunc()
			defer cancel()

			ledger, err := svc.getLedgerWithRetry(testCtx, mockLedgerBackend, 100)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)

				var meta xdr.LedgerCloseMeta
				err := xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &meta)
				require.NoError(t, err)
				assert.Equal(t, meta, ledger)
			}
		})
	}
}

func Test_ingestService_updateOldestCursor(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name          string
		initialCursor uint32
		updateTo      uint32
		wantCursor    uint32 // Expected cursor after update (UpdateMin keeps lower)
	}{
		{
			name:          "updates_to_lower_value",
			initialCursor: 100,
			updateTo:      50,
			wantCursor:    50,
		},
		{
			name:          "keeps_existing_lower_value",
			initialCursor: 50,
			updateTo:      100,
			wantCursor:    50, // UpdateMin keeps the lower value
		},
		{
			name:          "same_value_no_change",
			initialCursor: 100,
			updateTo:      100,
			wantCursor:    100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up and set initial cursor
			setupDBCursors(t, ctx, dbConnectionPool, 200, tc.initialCursor)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillFlushWorkers:   1,
			})
			require.NoError(t, err)

			err = svc.updateOldestCursor(ctx, tc.updateTo)
			require.NoError(t, err)

			// Verify the cursor value
			cursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor)
		})
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
				LatestLedgerCursorName: "latest_ledger_cursor",
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
			latestCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
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
				LatestLedgerCursorName: "latest_ledger_cursor",
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

func Test_ingestService_flushBufferWithRetry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name        string
		setupBuffer func() *indexer.IndexerBuffer
		wantTxCount int
		wantOpCount int
		txHashes    []string
	}{
		{
			name:        "flush_empty_buffer",
			setupBuffer: func() *indexer.IndexerBuffer { return indexer.NewIndexerBuffer() },
			wantTxCount: 0,
			wantOpCount: 0,
			txHashes:    []string{},
		},
		{
			name: "flush_with_data_inserts_to_database",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash1, 1)
				tx2 := createTestTransaction(flushTxHash2, 2)
				op1 := createTestOperation(200)
				op2 := createTestOperation(201)
				sc1 := createTestStateChange(1, testAddr1, 200)
				sc2 := createTestStateChange(2, testAddr2, 201)

				buf.PushTransaction(testAddr1, &tx1)
				buf.PushTransaction(testAddr2, &tx2)
				buf.PushOperation(testAddr1, &op1, &tx1)
				buf.PushOperation(testAddr2, &op2, &tx2)
				buf.PushStateChange(&tx1, &op1, sc1)
				buf.PushStateChange(&tx2, &op2, sc2)
				return buf
			},
			wantTxCount: 2,
			wantOpCount: 2,
			txHashes:    []string{flushTxHash1, flushTxHash2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up test data
			for _, hash := range []string{flushTxHash1, flushTxHash2, flushTxHash3, flushTxHash4} {
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			_, err = dbConnectionPool.Exec(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)

			setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				Metrics:                m,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillFlushWorkers:   1,
			})
			require.NoError(t, err)

			buffer := tc.setupBuffer()
			err = svc.flushBufferWithRetry(ctx, buffer)
			require.NoError(t, err)

			// Verify transaction count
			if len(tc.txHashes) > 0 {
				hashBytes := make([][]byte, len(tc.txHashes))
				for i, h := range tc.txHashes {
					val, err := types.HashBytea(h).Value()
					require.NoError(t, err)
					hashBytes[i] = val.([]byte)
				}
				var txCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM transactions WHERE hash = ANY($1)`,
					hashBytes).Scan(&txCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantTxCount, txCount, "transaction count mismatch")
			}

			// Verify operation count
			if tc.wantOpCount > 0 {
				var opCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM operations`).Scan(&opCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantOpCount, opCount, "operation count mismatch")
			}
		})
	}
}

// Old batch-based backfill tests removed — the pipeline now processes gaps
// sequentially with 3-stage pipeline (dispatcher → process → flush).

func Test_ingestService_startBackfilling_AllGapsFail_CursorUnchanged(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Clean up
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)

	initialOldest := uint32(100)
	initialLatest := uint32(100)
	setupDBCursors(t, ctx, dbConnectionPool, initialLatest, initialOldest)

	m := metrics.NewMetrics(prometheus.NewRegistry())

	models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
	require.NoError(t, modelsErr)

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	// Factory that always fails — every gap will fail at backend creation
	factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		return nil, fmt.Errorf("all backends unavailable")
	}

	svc, svcErr := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeBackfill,
		Models:                 models,
		LatestLedgerCursorName: "latest_ledger_cursor",
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             mockRPCService,
		LedgerBackend:          &LedgerBackendMock{},
		LedgerBackendFactory:   factory,
		Metrics:                m,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
		BackfillFlushWorkers:   1,
	})
	require.NoError(t, svcErr)

	// Run backfilling with all gaps failing
	backfillErr := svc.startBackfilling(ctx, 50, 99)

	// Pipeline logs failures but returns nil
	require.NoError(t, backfillErr)

	// Verify cursor remains unchanged
	finalOldest, getErr := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, initialOldest, finalOldest,
		"oldest cursor should remain unchanged when all gaps fail")

	finalLatest, getErr := models.IngestStore.Get(ctx, "latest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, initialLatest, finalLatest,
		"latest cursor should remain unchanged when all gaps fail")
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

		mockChAccStore := &store.ChannelAccountStoreMock{}

		// Mock token ingestion methods (called in parallel by insertAndUpsertParallel)
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockTokenIngestionService.On("ProcessNativeBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockTokenIngestionService.On("ProcessSACBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockTokenIngestionService.On("ProcessContractTokenChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
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
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, buffer)

		// Verify success
		require.NoError(t, err)
		assert.Equal(t, 0, numTx) // No transactions in buffer
		assert.Equal(t, 0, numOps)

		// Verify DB cursor was updated
		finalCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
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

		mockChAccStore := &store.ChannelAccountStoreMock{}

		// Mock token ingestion methods - one fails to simulate DB failure
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("db connection failed")).Maybe()
		mockTokenIngestionService.On("ProcessNativeBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessSACBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessContractTokenChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
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
		_, _, err = svc.ingestProcessedDataWithRetry(ctx, 100, buffer)

		// Verify error propagates with retry failure message
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed after")
		assert.Contains(t, err.Error(), "db connection failed")

		// Verify DB cursor was NOT updated (transaction rolled back)
		finalCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
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

		mockChAccStore := &store.ChannelAccountStoreMock{}

		// Mock token ingestion methods - trustline fails once then succeeds on retry
		mockTokenIngestionService := NewTokenIngestionServiceMock(t)
		mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("transient error")).Once()
		mockTokenIngestionService.On("ProcessTrustlineChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessNativeBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessSACBalanceChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockTokenIngestionService.On("ProcessContractTokenChanges", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		svc, err := NewIngestService(IngestServiceConfig{
			IngestionMode:          IngestionModeLive,
			Models:                 models,
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
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
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, buffer)

		// Verify success after retry
		require.NoError(t, err)
		assert.Equal(t, 0, numTx)
		assert.Equal(t, 0, numOps)

		// Verify DB cursor was updated
		finalCursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), finalCursor, "cursor should be updated after successful retry")

		mockTokenIngestionService.AssertExpectations(t)
	})
}
