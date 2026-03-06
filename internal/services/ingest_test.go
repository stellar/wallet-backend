package services

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
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
	"github.com/stellar/wallet-backend/internal/signing/store"
)

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

func Test_ingestService_splitGapsIntoBatches(t *testing.T) {
	svc := &ingestService{}

	testCases := []struct {
		name      string
		gaps      []data.LedgerRange
		batchSize uint32
		expected  []BackfillBatch
	}{
		{
			name:      "empty_gaps",
			gaps:      []data.LedgerRange{},
			batchSize: 100,
			expected:  nil,
		},
		{
			name: "single_gap_smaller_than_batch",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 150},
			},
			batchSize: 200,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 150},
			},
		},
		{
			name: "single_gap_larger_than_batch",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 399},
			},
			batchSize: 100,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 199},
				{StartLedger: 200, EndLedger: 299},
				{StartLedger: 300, EndLedger: 399},
			},
		},
		{
			name: "single_gap_exact_batch_size",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 199},
			},
			batchSize: 100,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 199},
			},
		},
		{
			name: "multiple_gaps",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 149},
				{GapStart: 300, GapEnd: 349},
			},
			batchSize: 100,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 149},
				{StartLedger: 300, EndLedger: 349},
			},
		},
		{
			name: "multiple_gaps_with_splits",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 249},
				{GapStart: 500, GapEnd: 599},
			},
			batchSize: 100,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 199},
				{StartLedger: 200, EndLedger: 249},
				{StartLedger: 500, EndLedger: 599},
			},
		},
		{
			name: "single_ledger_gap",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 100},
			},
			batchSize: 100,
			expected: []BackfillBatch{
				{StartLedger: 100, EndLedger: 100},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc.backfillBatchSize = tc.batchSize
			result := svc.splitGapsIntoBatches(tc.gaps)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func Test_analyzeBatchResults(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		results      []BackfillResult
		wantFailures int
	}{
		{
			name:         "empty_results",
			results:      []BackfillResult{},
			wantFailures: 0,
		},
		{
			name: "all_success",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 100, EndLedger: 199}, LedgersCount: 100},
				{Batch: BackfillBatch{StartLedger: 200, EndLedger: 299}, LedgersCount: 100},
			},
			wantFailures: 0,
		},
		{
			name: "some_failures",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 100, EndLedger: 199}, LedgersCount: 100},
				{Batch: BackfillBatch{StartLedger: 200, EndLedger: 299}, Error: fmt.Errorf("failed")},
				{Batch: BackfillBatch{StartLedger: 300, EndLedger: 399}, LedgersCount: 100},
			},
			wantFailures: 1,
		},
		{
			name: "all_failures",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 100, EndLedger: 199}, Error: fmt.Errorf("failed1")},
				{Batch: BackfillBatch{StartLedger: 200, EndLedger: 299}, Error: fmt.Errorf("failed2")},
			},
			wantFailures: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numFailed := analyzeBatchResults(ctx, tc.results)
			assert.Equal(t, tc.wantFailures, numFailed)
		})
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
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
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

func Test_ingestService_setupBatchBackend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name            string
		batch           BackfillBatch
		setupFactory    func() LedgerBackendFactory
		wantErr         bool
		wantErrContains string
	}{
		{
			name:  "success",
			batch: BackfillBatch{StartLedger: 100, EndLedger: 199},
			setupFactory: func() LedgerBackendFactory {
				return func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, ledgerbackend.BoundedRange(100, 199)).Return(nil)
					return mockBackend, nil
				}
			},
			wantErr: false,
		},
		{
			name:  "factory_error",
			batch: BackfillBatch{StartLedger: 100, EndLedger: 199},
			setupFactory: func() LedgerBackendFactory {
				return func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
					return nil, fmt.Errorf("factory failed")
				}
			},
			wantErr:         true,
			wantErrContains: "creating ledger backend",
		},
		{
			name:  "prepare_range_error",
			batch: BackfillBatch{StartLedger: 100, EndLedger: 199},
			setupFactory: func() LedgerBackendFactory {
				return func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(fmt.Errorf("prepare failed"))
					return mockBackend, nil
				}
			},
			wantErr:         true,
			wantErrContains: "preparing backend range",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				LedgerBackendFactory:   tc.setupFactory(),
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			backend, err := svc.setupBatchBackend(ctx, tc.batch)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, backend)
			}
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

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				MetricsService:         mockMetricsService,
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

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				MetricsService:         mockMetricsService,
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

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
		mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		mockChAccStore := &store.ChannelAccountStoreMock{}

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
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
			TokenIngestionService:  mockTokenIngestionService,
			MetricsService:         mockMetricsService,
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

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
		mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		mockChAccStore := &store.ChannelAccountStoreMock{}

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
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
			TokenIngestionService:  mockTokenIngestionService,
			MetricsService:         mockMetricsService,
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

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
		mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		// Create mock services
		mockRPCService := &RPCServiceMock{}
		mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

		mockChAccStore := &store.ChannelAccountStoreMock{}

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
			LatestLedgerCursorName: "latest_ledger_cursor",
			OldestLedgerCursorName: "oldest_ledger_cursor",
			AppTracker:             &apptracker.MockAppTracker{},
			RPCService:             mockRPCService,
			LedgerBackend:          &LedgerBackendMock{},
			ChannelAccountStore:    mockChAccStore,
			TokenIngestionService:  mockTokenIngestionService,
			MetricsService:         mockMetricsService,
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
