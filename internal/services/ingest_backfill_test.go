package services

import (
	"context"
	"fmt"
	"testing"

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
					`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
					VALUES ('anchor_hash', 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 100, NOW())`)
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
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
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
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
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
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
				for ledger := uint32(150); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
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
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
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

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
				SkipTxMeta:             false,
				SkipTxEnvelope:         false,
			})
			require.NoError(t, err)

			gaps, err := svc.calculateBackfillGaps(ctx, tc.startLedger, tc.endLedger)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func Test_HistoricalBackfill_Validation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                  string
		startLedger           uint32
		endLedger             uint32
		latestIngested        uint32
		expectValidationError bool
		errorContains         string
	}{
		{
			name:                  "valid_range",
			startLedger:           50,
			endLedger:             80,
			latestIngested:        100,
			expectValidationError: false,
		},
		{
			name:                  "end_exceeds_latest",
			startLedger:           50,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: true,
			errorContains:         "end ledger 150 cannot be greater than latest ingested ledger 100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("mock backend factory error")
			}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             &RPCServiceMock{},
				LedgerBackend:          &LedgerBackendMock{},
				LedgerBackendFactory:   mockBackendFactory,
				ChannelAccountStore:    &store.ChannelAccountStoreMock{},
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				SkipTxMeta:             false,
				SkipTxEnvelope:         false,
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startHistoricalBackfill(ctx, tc.startLedger, tc.endLedger)
			if tc.expectValidationError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				// Historical mode does not return error on batch failures, just logs them
				require.NoError(t, err)
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
				IngestionMode:          IngestionModeBackfill,
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

			err = svc.updateOldestCursor(ctx, tc.updateTo)
			require.NoError(t, err)

			// Verify the cursor value
			cursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor)
		})
	}
}

func Test_ingestService_flushHistoricalBatch(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                 string
		setupBuffer          func() *indexer.IndexerBuffer
		initialCursor        uint32
		wantTxCount          int
		wantOpCount          int
		wantStateChangeCount int
		txHashes             []string // For verification queries
	}{
		{
			name:                 "flush_empty_buffer_no_cursor_update",
			setupBuffer:          func() *indexer.IndexerBuffer { return indexer.NewIndexerBuffer() },
			initialCursor:        100,
			wantTxCount:          0,
			wantOpCount:          0,
			wantStateChangeCount: 0,
			txHashes:             []string{},
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

				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddr2, tx2)
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushOperation(testAddr2, op2, tx2)
				buf.PushStateChange(tx1, op1, sc1)
				buf.PushStateChange(tx2, op2, sc2)
				return buf
			},
			initialCursor:        100,
			wantTxCount:          2,
			wantOpCount:          2,
			wantStateChangeCount: 2,
			txHashes:             []string{flushTxHash1, flushTxHash2},
		},
		{
			name: "flush_with_cursor_update_to_lower_value",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash3, 3)
				buf.PushTransaction(testAddr1, tx1)
				return buf
			},
			initialCursor:        100,
			wantTxCount:          1,
			wantOpCount:          0,
			wantStateChangeCount: 0,
			txHashes:             []string{flushTxHash3},
		},
		{
			name: "flush_with_cursor_update_to_higher_value_keeps_existing",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash4, 4)
				buf.PushTransaction(testAddr1, tx1)
				return buf
			},
			initialCursor:        100,
			wantTxCount:          1,
			wantOpCount:          0,
			wantStateChangeCount: 0,
			txHashes:             []string{flushTxHash4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up test data from previous runs (using HashBytea for BYTEA column)
			for _, hash := range []string{flushTxHash1, flushTxHash2, flushTxHash3, flushTxHash4} {
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			// Also clean up any orphan operations
			_, err = dbConnectionPool.Exec(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)

			// Set up initial cursor
			setupDBCursors(t, ctx, dbConnectionPool, 200, tc.initialCursor)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			mockChAccStore := &store.ChannelAccountStoreMock{}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				ChannelAccountStore:    mockChAccStore,
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			buffer := tc.setupBuffer()

			// Call flushHistoricalBatch with a single-buffer slice
			err = svc.flushHistoricalBatch(ctx, []*indexer.IndexerBuffer{buffer})
			require.NoError(t, err)

			// Verify transaction count in database
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

			// Verify operation count in database
			if tc.wantOpCount > 0 {
				var opCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM operations`).Scan(&opCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantOpCount, opCount, "operation count mismatch")
			}

			// Verify state change count in database
			if tc.wantStateChangeCount > 0 {
				scHashBytes := make([][]byte, len(tc.txHashes))
				for i, h := range tc.txHashes {
					val, err := types.HashBytea(h).Value()
					require.NoError(t, err)
					scHashBytes[i] = val.([]byte)
				}
				var scCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = ANY($1))`,
					scHashBytes).Scan(&scCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantStateChangeCount, scCount, "state change count mismatch")
			}

			// Historical backfill should never unlock channel accounts
			mockChAccStore.AssertNotCalled(t, "UnassignTxAndUnlockChannelAccounts")
		})
	}
}

// ==================== Backfill Failure Scenario Tests ====================

// Test_ingestService_processBackfillBatchesParallel_PartialFailure verifies that when one
// batch fails during parallel processing, other batches still complete successfully.
func Test_ingestService_processBackfillBatchesParallel_PartialFailure(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name            string
		batches         []BackfillBatch
		failBatchIndex  int // which batch index should fail (0-based)
		wantFailedCount int
	}{
		{
			name: "middle_batch_fails",
			batches: []BackfillBatch{
				{StartLedger: 100, EndLedger: 109},
				{StartLedger: 110, EndLedger: 119}, // This one fails
				{StartLedger: 120, EndLedger: 129},
			},
			failBatchIndex:  1,
			wantFailedCount: 1,
		},
		{
			name: "first_batch_fails",
			batches: []BackfillBatch{
				{StartLedger: 100, EndLedger: 109}, // This one fails
				{StartLedger: 110, EndLedger: 119},
				{StartLedger: 120, EndLedger: 129},
			},
			failBatchIndex:  0,
			wantFailedCount: 1,
		},
		{
			name: "last_batch_fails",
			batches: []BackfillBatch{
				{StartLedger: 100, EndLedger: 109},
				{StartLedger: 110, EndLedger: 119},
				{StartLedger: 120, EndLedger: 129}, // This one fails
			},
			failBatchIndex:  2,
			wantFailedCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("SetOldestLedgerIngested", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, modelsErr)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			failBatch := tc.batches[tc.failBatchIndex]

			// Factory that returns a backend that fails PrepareRange for the specified batch
			factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				mockBackend := &LedgerBackendMock{}
				// Use MatchedBy to check if this is the failing batch
				mockBackend.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
					return r.From() == failBatch.StartLedger
				})).Return(fmt.Errorf("simulated failure for batch starting at %d", failBatch.StartLedger))
				// All other batches succeed - return error to prevent processing
				// This avoids nil pointer issues with empty LedgerCloseMeta
				mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
				// Return proper empty ledger meta to avoid nil pointer issues
				mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{
					V: 0,
					V0: &xdr.LedgerCloseMetaV0{
						LedgerHeader: xdr.LedgerHeaderHistoryEntry{
							Header: xdr.LedgerHeader{
								LedgerSeq: xdr.Uint32(100),
							},
						},
					},
				}, nil).Maybe()
				mockBackend.On("Close").Return(nil).Maybe()
				return mockBackend, nil
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
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillBatchSize:      10,
			})
			require.NoError(t, svcErr)

			results := svc.processBackfillBatchesParallel(ctx, tc.batches, func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
				return svc.processLedgersInBatch(ctx, backend, batch, svc.flushHistoricalBatch)
			}, nil)

			// Verify results
			require.Len(t, results, len(tc.batches))

			// Count actual failures
			actualFailed := 0
			for i, result := range results {
				if result.Error != nil {
					actualFailed++
					assert.Equal(t, tc.failBatchIndex, i, "expected batch at index %d to fail", tc.failBatchIndex)
					assert.Contains(t, result.Error.Error(), "preparing backend range")
				}
			}
			assert.Equal(t, tc.wantFailedCount, actualFailed)

			// Verify analyzeBatchResults returns correct count
			numFailed := analyzeBatchResults(ctx, results)
			assert.Equal(t, tc.wantFailedCount, numFailed)
		})
	}
}

// Test_ingestService_startBackfilling_HistoricalMode_PartialFailure_CursorUpdate verifies
// that the oldest_ingest_ledger cursor is updated to the minimum of successful batches
// when some batches fail during historical backfill.
func Test_ingestService_startBackfilling_HistoricalMode_PartialFailure_CursorUpdate(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name            string
		initialOldest   uint32
		initialLatest   uint32
		startLedger     uint32
		endLedger       uint32
		batchSize       uint32
		failBatchStart  uint32 // start ledger of batch to fail
		wantFinalOldest uint32
		wantError       bool
		wantErrContains string
	}{
		{
			name:            "middle_batch_fails_cursor_updates_to_minimum",
			initialOldest:   100,
			initialLatest:   100,
			startLedger:     50,
			endLedger:       99,
			batchSize:       10,
			failBatchStart:  70, // Batch [70-79] fails
			wantFinalOldest: 50, // Minimum of successful batches (50, 60, 80, 90)
			wantError:       false,
		},
		{
			name:            "first_batch_fails_cursor_updates_to_second_batch",
			initialOldest:   100,
			initialLatest:   100,
			startLedger:     50,
			endLedger:       79,
			batchSize:       10,
			failBatchStart:  50, // Batch [50-59] fails
			wantFinalOldest: 60, // Next successful batch
			wantError:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up database
			_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, `DELETE FROM operations`)
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes`)
			require.NoError(t, err)

			// Set up initial cursors
			setupDBCursors(t, ctx, dbConnectionPool, tc.initialLatest, tc.initialOldest)

			// Insert anchor transaction so GetOldestLedger() returns initialOldest
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
				VALUES ('anchor_hash', 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $1, NOW())`,
				tc.initialOldest)
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("SetOldestLedgerIngested", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, modelsErr)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			// Factory that fails for the specified batch
			factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				mockBackend := &LedgerBackendMock{}
				mockBackend.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
					return r.From() == tc.failBatchStart
				})).Return(fmt.Errorf("simulated network failure"))
				mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
				// For successful batches, return empty ledgers (no transactions)
				mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{
					V: 0,
					V0: &xdr.LedgerCloseMetaV0{
						LedgerHeader: xdr.LedgerHeaderHistoryEntry{
							Header: xdr.LedgerHeader{
								LedgerSeq: xdr.Uint32(50),
							},
						},
					},
				}, nil).Maybe()
				mockBackend.On("Close").Return(nil).Maybe()
				return mockBackend, nil
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
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillBatchSize:      int(tc.batchSize),
			})
			require.NoError(t, svcErr)

			// Run backfilling
			backfillErr := svc.startHistoricalBackfill(ctx, tc.startLedger, tc.endLedger)

			if tc.wantError {
				require.Error(t, backfillErr)
				if tc.wantErrContains != "" {
					assert.Contains(t, backfillErr.Error(), tc.wantErrContains)
				}
			} else {
				// Historical mode should not return error even with partial failures
				require.NoError(t, backfillErr)
			}

			// Verify cursor was updated correctly using IngestStore.Get
			finalOldest, getErr := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, getErr)
			assert.Equal(t, tc.wantFinalOldest, finalOldest,
				"oldest cursor should be updated to minimum of successful batches")
		})
	}
}

// Test_ingestService_processBackfillBatches_PartialFailure_OnlySuccessfulBatchPersisted verifies
// that when one batch fails and another succeeds:
// - The failed batch does not persist any data
// - The successful batch persists its transactions
// - Proper error handling for both cases
func Test_ingestService_processBackfillBatches_PartialFailure_OnlySuccessfulBatchPersisted(t *testing.T) {
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
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes`)
	require.NoError(t, err)

	// Set up initial cursors
	setupDBCursors(t, ctx, dbConnectionPool, 200, 200)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
	mockMetricsService.On("SetOldestLedgerIngested", mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
	mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveStateChangeProcessingDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveIngestionDuration", mock.Anything, mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, modelsErr)

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	// Parse ledger metadata with a transaction for the successful batch
	var metaWithTx xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(ledgerMetadataWith1Tx, &metaWithTx)
	require.NoError(t, err)

	// Two batches: first fails, second succeeds
	// Use single-ledger batches to avoid duplicate key issues with test fixtures
	batches := []BackfillBatch{
		{StartLedger: 100, EndLedger: 100}, // Will fail
		{StartLedger: 110, EndLedger: 110}, // Will succeed - single ledger to avoid duplicate tx hash
	}

	// Factory that returns a mock backend configured based on the batch range
	factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		mockBackend := &LedgerBackendMock{}
		// Fail batch 1 (100-109) at PrepareRange
		mockBackend.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
			return r.From() == 100
		})).Return(fmt.Errorf("ledger range unavailable"))
		// Succeed batch 2 (110-119)
		mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(metaWithTx, nil).Maybe()
		mockBackend.On("Close").Return(nil).Maybe()
		return mockBackend, nil
	}

	svc, svcErr := NewIngestService(IngestServiceConfig{
		IngestionMode:             IngestionModeBackfill,
		Models:                    models,
		LatestLedgerCursorName:    "latest_ledger_cursor",
		OldestLedgerCursorName:    "oldest_ledger_cursor",
		AppTracker:                &apptracker.MockAppTracker{},
		RPCService:                mockRPCService,
		LedgerBackend:             &LedgerBackendMock{},
		LedgerBackendFactory:      factory,
		MetricsService:            mockMetricsService,
		GetLedgersLimit:           defaultGetLedgersLimit,
		Network:                   network.TestNetworkPassphrase,
		NetworkPassphrase:         network.TestNetworkPassphrase,
		Archive:                   &HistoryArchiveMock{},
		BackfillBatchSize:         10,
		BackfillDBInsertBatchSize: 50,
	})
	require.NoError(t, svcErr)

	// Process both batches in parallel
	results := svc.processBackfillBatchesParallel(ctx, batches, func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
		return svc.processLedgersInBatch(ctx, backend, batch, svc.flushHistoricalBatch)
	}, nil)

	// Verify we got results for both batches
	require.Len(t, results, 2)

	// Verify batch 1 (100-109) failed
	require.Error(t, results[0].Error, "batch 1 should have failed")
	assert.Contains(t, results[0].Error.Error(), "preparing backend range")
	assert.Contains(t, results[0].Error.Error(), "ledger range unavailable")

	// Verify batch 2 (110-119) succeeded
	require.NoError(t, results[1].Error, "batch 2 should have succeeded")

	// Verify no transactions were persisted for failed batch (ledger 100)
	var failedBatchTxCount int
	err = dbConnectionPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM transactions WHERE ledger_number BETWEEN $1 AND $2`,
		100, 109).Scan(&failedBatchTxCount)
	require.NoError(t, err)
	assert.Equal(t, 0, failedBatchTxCount, "no transactions should be persisted for failed batch")

	// Verify transactions were persisted for successful batch
	// The ledgerMetadataWith1Tx fixture has a fixed ledger sequence of 4478
	// so we query for that ledger number (the XDR metadata determines the stored ledger)
	var successBatchTxCount int
	err = dbConnectionPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM transactions WHERE ledger_number = $1`,
		4478).Scan(&successBatchTxCount)
	require.NoError(t, err)
	assert.Equal(t, 1, successBatchTxCount, "1 transaction should be persisted for successful batch")
}

// Test_ingestService_startBackfilling_HistoricalMode_AllBatchesFail_CursorUnchanged
// verifies that the cursor remains unchanged when all batches fail during historical backfill.
func Test_ingestService_startBackfilling_HistoricalMode_AllBatchesFail_CursorUnchanged(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Clean up database
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)

	// Set up initial cursors
	initialOldest := uint32(100)
	initialLatest := uint32(100)
	setupDBCursors(t, ctx, dbConnectionPool, initialLatest, initialOldest)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
	mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, modelsErr)

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	// Factory that always fails
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
		MetricsService:         mockMetricsService,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
		BackfillBatchSize:      10,
	})
	require.NoError(t, svcErr)

	// Run backfilling with all batches failing
	backfillErr := svc.startHistoricalBackfill(ctx, 50, 99)

	// Historical mode should NOT return error even when all batches fail
	require.NoError(t, backfillErr, "historical mode should not return error even when all batches fail")

	// Verify cursor remains unchanged
	finalOldest, getErr := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, initialOldest, finalOldest,
		"oldest cursor should remain unchanged when all batches fail")

	finalLatest, getErr := models.IngestStore.Get(ctx, "latest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, initialLatest, finalLatest,
		"latest cursor should remain unchanged when all batches fail")
}
