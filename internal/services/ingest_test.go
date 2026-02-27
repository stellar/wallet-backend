package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
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
	testKP1       = keypair.MustRandom()
	testKP2       = keypair.MustRandom()
	testKP3       = keypair.MustRandom()
	testAddr1     = testKP1.Address()
	testAddr2     = testKP2.Address()
	testAddrUnreg = testKP3.Address()
)

const (
	defaultGetLedgersLimit = 50

	// Test hash constants for ingest tests (64-char hex strings for BYTEA storage)
	flushTxHash1   = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101"
	flushTxHash2   = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f202"
	flushTxHash3   = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f303"
	flushTxHash4   = "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f404"
	flushTxHash5   = "f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f505"
	flushTxHash6   = "f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f606"
	catchupTxHash1 = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c101"
	catchupTxHash2 = "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202"
	catchupTxHash3 = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c303"
	catchupTxHash4 = "c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c404"
	catchupTxHash5 = "c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c505"
	catchupTxHash6 = "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c606"
	prevTxHash     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	txHash1        = "1111111111111111111111111111111111111111111111111111111111111111"
	txHash2        = "2222222222222222222222222222222222222222222222222222222222222222"

	// Test fixtures for ledger metadata
	ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	ledgerMetadataWith1Tx = "AAAAAQAAAAD8G2qemHnBKFkbq90RTagxAypNnA7DXDc63Giipq9mNwAAABYLEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VPJvwbisrc9A0yzFxxCdkICgB3Gv7qHOi8ZdsK2CNks2AAAAAGhTTAsAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAunZtorOSbnRpgnykoDe4kzAvLwNXefncy1R/1ynBWyDv0DfdnqJ6Hcy/0AJf6DkBZlRayg775h3HjV0GKF/oPua7l8wkLlJBtSk1kRDt55qSf6btSrgcupB/8bnpJfUUgZJ76saUrj29HukYHS1bq7SyuoCAY+5F9iBYTmW1G9QAAEX4N4Lazp2QAAAAAAAMtS3veAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAELEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VAAAAAIAAAAAAAAAAQAAAAAAAAABAAAAAAAAAGQAAAABAAAAAgAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAGQAAA7FAAAAGgAAAAAAAAAAAAAAAQAAAAAAAAABAAAAALvqzdVyRxgBMcLzbw1wNWcJYHPNPok1GdVSgmy4sjR2AAAAAVVTREMAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAACVAvkAAAAAAAAAAABhHevAAAAAEDq2yIDzXUoLboBHQkbr8U2oKqLzf0gfpwXbmRPLB6Ek3G8uCEYyry1vt5Sb+LCEd81fefFQcQN0nydr1FmiXcDAAAAAAAAAAAAAAABXFSiWcxpDRa8frBs1wbEaMUw4hMe7ctFtdw3Ci73IEwAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAIAAAADAAARfQAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3GPAAADsUAAAAZAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF9AAAAAGhTTAYAAAAAAAAAAQAAEX4AAAAAAAAAAODia2IsqMlWCuY6k734V/dcCafJwfI1Qq7+/0qEd68AAAAALpDtxdgAAA7FAAAAGQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAAARfQAAAABoU0wGAAAAAAAAAAMAAAAAAAAAAgAAAAMAABF+AAAAAAAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAC6Q7cXYAAAOxQAAABkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAAEX0AAAAAaFNMBgAAAAAAAAABAAARfgAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3F2AAADsUAAAAaAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF+AAAAAGhTTAsAAAAAAAAAAQAAAAIAAAADAAARcwAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAlQL5AAf/////////8AAAABAAAAAAAAAAAAAAABAAARfgAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAukO3QAf/////////8AAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8RxEAAAAAAAAAAA=="
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

func Test_ingestService_calculateBackfillGaps(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
				_, err := dbConnectionPool.ExecContext(ctx,
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
					_, err := dbConnectionPool.ExecContext(ctx,
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
					_, err := dbConnectionPool.ExecContext(ctx,
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
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
				for ledger := uint32(150); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
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
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 50)`)
				require.NoError(t, err)
				// Actual transactions only exist from 100 onwards
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
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
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store")
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

func Test_BackfillMode_Validation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                  string
		mode                  BackfillMode
		startLedger           uint32
		endLedger             uint32
		latestIngested        uint32
		expectValidationError bool
		errorContains         string
	}{
		{
			name:                  "historical_mode_valid_range",
			mode:                  BackfillModeHistorical,
			startLedger:           50,
			endLedger:             80,
			latestIngested:        100,
			expectValidationError: false,
		},
		{
			name:                  "historical_mode_end_exceeds_latest",
			mode:                  BackfillModeHistorical,
			startLedger:           50,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: true,
			errorContains:         "end ledger 150 cannot be greater than latest ingested ledger 100",
		},
		{
			name:                  "catchup_mode_valid_range",
			mode:                  BackfillModeCatchup,
			startLedger:           101,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: false,
		},
		{
			name:                  "catchup_mode_start_not_next_ledger",
			mode:                  BackfillModeCatchup,
			startLedger:           105,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: true,
			errorContains:         "catchup must start from ledger 101",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			// Set up latest ingested ledger cursor
			_, err = dbConnectionPool.ExecContext(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`,
				tc.latestIngested)
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
				tc.latestIngested)
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                mockArchive,
				SkipTxMeta:             false,
				SkipTxEnvelope:         false,
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startBackfilling(ctx, tc.startLedger, tc.endLedger, tc.mode)
			if tc.expectValidationError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				// For valid cases, validation passes but batch processing fails.
				// Behavior differs by mode:
				// - Catchup mode: returns error "optimized catchup failed" when batches fail
				// - Historical mode: logs failures but returns nil (continues processing)
				if tc.mode.isCatchup() {
					require.Error(t, err)
					assert.Contains(t, err.Error(), "optimized catchup failed")
					// Ensure it's NOT a validation error
					assert.NotContains(t, err.Error(), "cannot be greater than latest ingested ledger")
					assert.NotContains(t, err.Error(), "catchup must start from ledger")
				} else {
					// Historical mode does not return error on batch failures, just logs them
					require.NoError(t, err)
				}
			}
		})
	}
}

// ==================== Test Helper Functions ====================

// setupDBCursors sets up ingest_store cursors for testing
func setupDBCursors(t *testing.T, ctx context.Context, pool db.ConnectionPool, latestLedger, oldestLedger uint32) {
	_, err := pool.ExecContext(ctx, `DELETE FROM ingest_store`)
	require.NoError(t, err)
	if latestLedger > 0 {
		_, err = pool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`, latestLedger)
		require.NoError(t, err)
	}
	if oldestLedger > 0 {
		_, err = pool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`, oldestLedger)
		require.NoError(t, err)
	}
}

// ptrUint32 returns a pointer to the given uint32 value
func ptrUint32(v uint32) *uint32 { return &v }

// createTestTransaction creates a transaction with required fields for testing.
func createTestTransaction(hash string, toID int64) types.Transaction {
	now := time.Now()
	envelope := "test_envelope_xdr"
	meta := "test_meta_xdr"
	return types.Transaction{
		Hash:            types.HashBytea(hash),
		ToID:            toID,
		EnvelopeXDR:     &envelope,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta,
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
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		AccountID:           types.AddressBytea(accountID),
		OperationID:         opID,
		LedgerNumber:        1000,
		LedgerCreatedAt:     now,
		IngestedAt:          now,
	}
}

// ==================== New Tests ====================

func Test_BackfillMode_Methods(t *testing.T) {
	testCases := []struct {
		mode           BackfillMode
		wantHistorical bool
		wantCatchup    bool
	}{
		{
			mode:           BackfillModeHistorical,
			wantHistorical: true,
			wantCatchup:    false,
		},
		{
			mode:           BackfillModeCatchup,
			wantHistorical: false,
			wantCatchup:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("mode_%d", tc.mode), func(t *testing.T) {
			assert.Equal(t, tc.wantHistorical, tc.mode.isHistorical())
			assert.Equal(t, tc.wantCatchup, tc.mode.isCatchup())
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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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

func Test_ingestService_updateOldestCursor(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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

func Test_ingestService_initializeCursors(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name        string
		startLedger uint32
		setupDB     func(t *testing.T)
	}{
		{
			name:        "initializes_both_cursors_from_empty",
			startLedger: 1000,
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM ingest_store`)
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

			err = db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
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
			dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
			require.NoError(t, err)
			defer dbConnectionPool.Close()

			ctx := context.Background()

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

func Test_hasRegisteredParticipant(t *testing.T) {
	testCases := []struct {
		name         string
		participants []string
		registered   []string
		want         bool
	}{
		{
			name:         "empty_participants",
			participants: []string{},
			registered:   []string{"GABC"},
			want:         false,
		},
		{
			name:         "empty_registered",
			participants: []string{"GABC"},
			registered:   []string{},
			want:         false,
		},
		{
			name:         "has_registered_participant",
			participants: []string{"GABC", "GDEF"},
			registered:   []string{"GDEF", "GHIJ"},
			want:         true,
		},
		{
			name:         "no_registered_participant",
			participants: []string{"GABC", "GDEF"},
			registered:   []string{"GHIJ", "GKLM"},
			want:         false,
		},
		{
			name:         "all_participants_registered",
			participants: []string{"GABC", "GDEF"},
			registered:   []string{"GABC", "GDEF"},
			want:         true,
		},
		{
			name:         "single_match",
			participants: []string{"GABC", "GDEF", "GHIJ"},
			registered:   []string{"GDEF"},
			want:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participantSet := set.NewSet(tc.participants...)
			registeredSet := set.NewSet(tc.registered...)
			result := hasRegisteredParticipant(participantSet, registeredSet)
			assert.Equal(t, tc.want, result)
		})
	}
}

func Test_ingestService_flushBatchBufferWithRetry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                       string
		setupBuffer                func() *indexer.IndexerBuffer
		updateCursorTo             *uint32
		enableParticipantFiltering bool
		registeredAccounts         []string
		initialCursor              uint32
		wantCursor                 uint32
		wantTxCount                int
		wantOpCount                int
		wantStateChangeCount       int
		txHashes                   []string // For verification queries
	}{
		{
			name:                 "flush_empty_buffer_no_cursor_update",
			setupBuffer:          func() *indexer.IndexerBuffer { return indexer.NewIndexerBuffer() },
			updateCursorTo:       nil,
			initialCursor:        100,
			wantCursor:           100,
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
			updateCursorTo:       nil,
			initialCursor:        100,
			wantCursor:           100,
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
			updateCursorTo:       ptrUint32(50),
			initialCursor:        100,
			wantCursor:           50,
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
			updateCursorTo:       ptrUint32(150),
			initialCursor:        100,
			wantCursor:           100, // UpdateMin keeps lower
			wantTxCount:          1,
			wantOpCount:          0,
			wantStateChangeCount: 0,
			txHashes:             []string{flushTxHash4},
		},
		{
			name: "flush_with_filtering_only_inserts_registered",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash5, 5) // Registered participant
				tx2 := createTestTransaction(flushTxHash6, 6) // No registered participant

				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddrUnreg, tx2)
				return buf
			},
			enableParticipantFiltering: true,
			registeredAccounts:         []string{testAddr1},
			updateCursorTo:             nil,
			initialCursor:              100,
			wantCursor:                 100,
			wantTxCount:                1, // Only tx1
			wantOpCount:                0,
			wantStateChangeCount:       0,
			txHashes:                   []string{flushTxHash5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up test data from previous runs (using HashBytea for BYTEA column)
			for _, hash := range []string{flushTxHash1, flushTxHash2, flushTxHash3, flushTxHash4, flushTxHash5, flushTxHash6} {
				_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			// Also clean up any orphan operations
			_, err = dbConnectionPool.ExecContext(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
			require.NoError(t, err)

			// Set up initial cursor
			setupDBCursors(t, ctx, dbConnectionPool, 200, tc.initialCursor)

			// Add registered accounts if any
			for _, acc := range tc.registeredAccounts {
				_, insertErr := dbConnectionPool.ExecContext(ctx,
					`INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`, types.AddressBytea(acc))
				require.NoError(t, insertErr)
			}

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
			// Use variadic mock.Anything for any number of tx hashes
			mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
			mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
			mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
			mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:              IngestionModeBackfill,
				Models:                     models,
				LatestLedgerCursorName:     "latest_ledger_cursor",
				OldestLedgerCursorName:     "oldest_ledger_cursor",
				AppTracker:                 &apptracker.MockAppTracker{},
				RPCService:                 mockRPCService,
				LedgerBackend:              &LedgerBackendMock{},
				ChannelAccountStore:        mockChAccStore,
				MetricsService:             mockMetricsService,
				GetLedgersLimit:            defaultGetLedgersLimit,
				Network:                    network.TestNetworkPassphrase,
				NetworkPassphrase:          network.TestNetworkPassphrase,
				Archive:                    &HistoryArchiveMock{},
				EnableParticipantFiltering: tc.enableParticipantFiltering,
			})
			require.NoError(t, err)

			buffer := tc.setupBuffer()

			// Call flushBatchBuffer
			err = svc.flushBatchBufferWithRetry(ctx, buffer, tc.updateCursorTo, nil)
			require.NoError(t, err)

			// Verify the cursor value
			cursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor)

			// Verify transaction count in database
			if len(tc.txHashes) > 0 {
				hashBytes := make([][]byte, len(tc.txHashes))
				for i, h := range tc.txHashes {
					val, err := types.HashBytea(h).Value()
					require.NoError(t, err)
					hashBytes[i] = val.([]byte)
				}
				var txCount int
				err = dbConnectionPool.GetContext(ctx, &txCount,
					`SELECT COUNT(*) FROM transactions WHERE hash = ANY($1)`,
					pq.Array(hashBytes))
				require.NoError(t, err)
				assert.Equal(t, tc.wantTxCount, txCount, "transaction count mismatch")
			}

			// Verify operation count in database
			if tc.wantOpCount > 0 {
				var opCount int
				err = dbConnectionPool.GetContext(ctx, &opCount,
					`SELECT COUNT(*) FROM operations`)
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
				err = dbConnectionPool.GetContext(ctx, &scCount,
					`SELECT COUNT(*) FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = ANY($1))`,
					pq.Array(scHashBytes))
				require.NoError(t, err)
				assert.Equal(t, tc.wantStateChangeCount, scCount, "state change count mismatch")
			}
		})
	}
}

func Test_ingestService_filterParticipantData(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                       string
		enableParticipantFiltering bool
		registeredAccounts         []string
		setupBuffer                func() *indexer.IndexerBuffer
		wantTxCount                int
		wantOpCount                int
		wantStateChangeCount       int
		verifyParticipants         func(t *testing.T, filtered *filteredIngestionData)
	}{
		{
			name:                       "filtering_disabled_returns_all_data",
			enableParticipantFiltering: false,
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(txHash1, 1)
				tx2 := createTestTransaction(txHash2, 2)
				op1 := createTestOperation(100)
				op2 := createTestOperation(101)
				sc1 := createTestStateChange(1, testAddr1, 100)
				sc2 := createTestStateChange(2, testAddr2, 101)

				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddr2, tx2)
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushOperation(testAddr2, op2, tx2)
				buf.PushStateChange(tx1, op1, sc1)
				buf.PushStateChange(tx2, op2, sc2)
				return buf
			},
			wantTxCount:          2,
			wantOpCount:          2,
			wantStateChangeCount: 2,
		},
		{
			name:                       "filtering_enabled_includes_tx_with_registered_participant",
			enableParticipantFiltering: true,
			registeredAccounts:         []string{testAddr1},
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(txHash1, 1)
				op1 := createTestOperation(100)
				sc1 := createTestStateChange(1, testAddr1, 100)

				// Tx has 2 participants but only 1 is registered
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddrUnreg, tx1) // Unregistered participant on same tx
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushStateChange(tx1, op1, sc1)
				return buf
			},
			wantTxCount:          1,
			wantOpCount:          1,
			wantStateChangeCount: 1,
			verifyParticipants: func(t *testing.T, filtered *filteredIngestionData) {
				// Verify ALL participants are preserved (not just registered ones)
				participants := filtered.txParticipants[int64(1)]
				assert.Equal(t, 2, participants.Cardinality())
				assert.True(t, participants.Contains(testAddr1))
				assert.True(t, participants.Contains(testAddrUnreg))
			},
		},
		{
			name:                       "filtering_enabled_excludes_tx_without_registered",
			enableParticipantFiltering: true,
			registeredAccounts:         []string{testAddr1},
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(txHash1, 1) // Has registered
				tx2 := createTestTransaction(txHash2, 2) // No registered
				op1 := createTestOperation(100)
				op2 := createTestOperation(101)

				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddrUnreg, tx2)
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushOperation(testAddrUnreg, op2, tx2)
				return buf
			},
			wantTxCount:          1, // Only tx1
			wantOpCount:          1, // Only op1
			wantStateChangeCount: 0,
		},
		{
			name:                       "filtering_enabled_no_registered_accounts_returns_empty",
			enableParticipantFiltering: true,
			registeredAccounts:         []string{},
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(txHash1, 1)
				buf.PushTransaction(testAddrUnreg, tx1)
				return buf
			},
			wantTxCount:          0,
			wantOpCount:          0,
			wantStateChangeCount: 0,
		},
		{
			name:                       "filtering_state_changes_only_for_registered_accounts",
			enableParticipantFiltering: true,
			registeredAccounts:         []string{testAddr1, testAddr2},
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(txHash1, 1)
				op1 := createTestOperation(100)

				// 3 state changes: 2 for registered accounts, 1 for unregistered
				sc1 := createTestStateChange(1, testAddr1, 100)
				sc2 := createTestStateChange(2, testAddr2, 100)
				sc3 := createTestStateChange(3, testAddrUnreg, 100)

				buf.PushTransaction(testAddr1, tx1)
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushStateChange(tx1, op1, sc1)
				buf.PushStateChange(tx1, op1, sc2)
				buf.PushStateChange(tx1, op1, sc3)
				return buf
			},
			wantTxCount:          1,
			wantOpCount:          1,
			wantStateChangeCount: 2, // Only 2 registered accounts' state changes
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up
			_, execErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
			require.NoError(t, execErr)

			// Add registered accounts if any
			for _, acc := range tc.registeredAccounts {
				_, insertErr := dbConnectionPool.ExecContext(ctx,
					`INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`, types.AddressBytea(acc))
				require.NoError(t, insertErr)
			}

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, modelsErr)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, svcErr := NewIngestService(IngestServiceConfig{
				IngestionMode:              IngestionModeBackfill,
				Models:                     models,
				LatestLedgerCursorName:     "latest_ledger_cursor",
				OldestLedgerCursorName:     "oldest_ledger_cursor",
				AppTracker:                 &apptracker.MockAppTracker{},
				RPCService:                 mockRPCService,
				LedgerBackend:              &LedgerBackendMock{},
				MetricsService:             mockMetricsService,
				GetLedgersLimit:            defaultGetLedgersLimit,
				Network:                    network.TestNetworkPassphrase,
				NetworkPassphrase:          network.TestNetworkPassphrase,
				Archive:                    &HistoryArchiveMock{},
				EnableParticipantFiltering: tc.enableParticipantFiltering,
			})
			require.NoError(t, svcErr)

			buffer := tc.setupBuffer()

			// Call filterParticipantData within a transaction
			txErr := db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
				filtered, filterErr := svc.filterParticipantData(ctx, dbTx, buffer)
				if filterErr != nil {
					return filterErr
				}
				assert.Len(t, filtered.txs, tc.wantTxCount)
				assert.Len(t, filtered.ops, tc.wantOpCount)
				assert.Len(t, filtered.stateChanges, tc.wantStateChangeCount)

				// Run custom verification if provided
				if tc.verifyParticipants != nil {
					tc.verifyParticipants(t, filtered)
				}
				return nil
			})
			require.NoError(t, txErr)
		})
	}
}

// ==================== Backfill Failure Scenario Tests ====================

// Test_ingestService_processBackfillBatchesParallel_PartialFailure verifies that when one
// batch fails during parallel processing, other batches still complete successfully.
func Test_ingestService_processBackfillBatchesParallel_PartialFailure(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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

			results := svc.processBackfillBatchesParallel(ctx, BackfillModeHistorical, tc.batches, nil)

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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
			require.NoError(t, err)
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes`)
			require.NoError(t, err)

			// Set up initial cursors
			setupDBCursors(t, ctx, dbConnectionPool, tc.initialLatest, tc.initialOldest)

			// Insert anchor transaction so GetOldestLedger() returns initialOldest
			_, err = dbConnectionPool.ExecContext(ctx,
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
			backfillErr := svc.startBackfilling(ctx, tc.startLedger, tc.endLedger, BackfillModeHistorical)

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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Clean up database
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
	require.NoError(t, err)
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes`)
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
	results := svc.processBackfillBatchesParallel(ctx, BackfillModeHistorical, batches, nil)

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
	sqlxDB, sqlxErr := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, sqlxErr)
	err = sqlxDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM transactions WHERE ledger_number BETWEEN $1 AND $2`,
		100, 109).Scan(&failedBatchTxCount)
	require.NoError(t, err)
	assert.Equal(t, 0, failedBatchTxCount, "no transactions should be persisted for failed batch")

	// Verify transactions were persisted for successful batch
	// The ledgerMetadataWith1Tx fixture has a fixed ledger sequence of 4478
	// so we query for that ledger number (the XDR metadata determines the stored ledger)
	var successBatchTxCount int
	err = sqlxDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM transactions WHERE ledger_number = $1`,
		4478).Scan(&successBatchTxCount)
	require.NoError(t, err)
	assert.Equal(t, 1, successBatchTxCount, "1 transaction should be persisted for successful batch")
}

// Test_ingestService_startBackfilling_CatchupMode_PartialFailure_ReturnsError verifies
// that catchup mode returns an error when any batch fails.
func Test_ingestService_startBackfilling_CatchupMode_PartialFailure_ReturnsError(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Clean up database
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)

	// Set up initial cursors: latest = 99, so catchup starts at 100
	setupDBCursors(t, ctx, dbConnectionPool, 99, 50)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
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

	// Factory that fails for the middle batch
	factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		mockBackend := &LedgerBackendMock{}
		// Fail for batch starting at 110
		mockBackend.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
			return r.From() == 110
		})).Return(fmt.Errorf("simulated catchup failure"))
		mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
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

	// Run catchup backfilling: starts at latest+1 = 100, ends at 129 (3 batches)
	backfillErr := svc.startBackfilling(ctx, 100, 129, BackfillModeCatchup)

	// Verify error is returned for catchup mode
	require.Error(t, backfillErr)
	assert.Contains(t, backfillErr.Error(), "optimized catchup failed")
	assert.Contains(t, backfillErr.Error(), "batches failed")

	// Verify latest cursor was NOT updated
	finalLatest, getErr := models.IngestStore.Get(ctx, "latest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, uint32(99), finalLatest, "latest cursor should NOT be updated when catchup fails")
}

// Test_ingestService_startBackfilling_HistoricalMode_AllBatchesFail_CursorUnchanged
// verifies that the cursor remains unchanged when all batches fail during historical backfill.
func Test_ingestService_startBackfilling_HistoricalMode_AllBatchesFail_CursorUnchanged(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Clean up database
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
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
	backfillErr := svc.startBackfilling(ctx, 50, 99, BackfillModeHistorical)

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

// Test_ingestProcessedDataWithRetry tests the ingestProcessedDataWithRetry function covering success, failure, and retry scenarios.
func Test_ingestProcessedDataWithRetry(t *testing.T) {
	t.Run("success - processes data and updates cursor", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Clean up database
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
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
			IngestionMode:              IngestionModeLive,
			Models:                     models,
			LatestLedgerCursorName:     "latest_ledger_cursor",
			OldestLedgerCursorName:     "oldest_ledger_cursor",
			AppTracker:                 &apptracker.MockAppTracker{},
			RPCService:                 mockRPCService,
			LedgerBackend:              &LedgerBackendMock{},
			ChannelAccountStore:        mockChAccStore,
			TokenIngestionService:      mockTokenIngestionService,
			MetricsService:             mockMetricsService,
			GetLedgersLimit:            defaultGetLedgersLimit,
			Network:                    network.TestNetworkPassphrase,
			NetworkPassphrase:          network.TestNetworkPassphrase,
			Archive:                    &HistoryArchiveMock{},
			EnableParticipantFiltering: false,
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
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Clean up database
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
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
			IngestionMode:              IngestionModeLive,
			Models:                     models,
			LatestLedgerCursorName:     "latest_ledger_cursor",
			OldestLedgerCursorName:     "oldest_ledger_cursor",
			AppTracker:                 &apptracker.MockAppTracker{},
			RPCService:                 mockRPCService,
			LedgerBackend:              &LedgerBackendMock{},
			ChannelAccountStore:        mockChAccStore,
			TokenIngestionService:      mockTokenIngestionService,
			MetricsService:             mockMetricsService,
			GetLedgersLimit:            defaultGetLedgersLimit,
			Network:                    network.TestNetworkPassphrase,
			NetworkPassphrase:          network.TestNetworkPassphrase,
			Archive:                    &HistoryArchiveMock{},
			EnableParticipantFiltering: false,
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
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Clean up database
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
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
			IngestionMode:              IngestionModeLive,
			Models:                     models,
			LatestLedgerCursorName:     "latest_ledger_cursor",
			OldestLedgerCursorName:     "oldest_ledger_cursor",
			AppTracker:                 &apptracker.MockAppTracker{},
			RPCService:                 mockRPCService,
			LedgerBackend:              &LedgerBackendMock{},
			ChannelAccountStore:        mockChAccStore,
			TokenIngestionService:      mockTokenIngestionService,
			MetricsService:             mockMetricsService,
			GetLedgersLimit:            defaultGetLedgersLimit,
			Network:                    network.TestNetworkPassphrase,
			NetworkPassphrase:          network.TestNetworkPassphrase,
			Archive:                    &HistoryArchiveMock{},
			EnableParticipantFiltering: false,
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

// Test_ingestService_processBatchChanges tests the processBatchChanges method which processes
// aggregated batch changes after all parallel batches complete.
// NOTE: The implementation now:
// 1. Calls models.TrustlineAsset.BatchInsert for trustline assets (real DB operation)
// 2. Calls contractMetadataService.FetchMetadata for new contracts (mocked)
// 3. Calls models.Contract.BatchInsert for contracts (real DB operation)
// 4. Calls tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges) (mocked)
func Test_ingestService_processBatchChanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                 string
		trustlineChanges     map[indexer.TrustlineChangeKey]types.TrustlineChange
		contractChanges      []types.ContractChange
		uniqueAssets         map[uuid.UUID]data.TrustlineAsset
		uniqueContractTokens map[string]types.ContractType
		setupMocks           func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock)
		wantErr              bool
		wantErrContains      string
		verifySortedChanges  func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock)
	}{
		{
			name:                 "empty_data_calls_ProcessTokenChanges",
			trustlineChanges:     map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges is called with empty map and slice
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Data is now pre-deduplicated by caller
			name: "passes_through_deduplicated_trustline_changes",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USD", "GA1")}: {AccountID: "GA1", Asset: "USD:GA1", OperationID: toid.New(100, 0, 10).ToInt64(), LedgerNumber: 100},
				{AccountID: "GA2", TrustlineID: data.DeterministicAssetID("EUR", "GA2")}: {AccountID: "GA2", Asset: "EUR:GA2", OperationID: toid.New(101, 0, 1).ToInt64(), LedgerNumber: 101},
				{AccountID: "GA3", TrustlineID: data.DeterministicAssetID("GBP", "GA3")}: {AccountID: "GA3", Asset: "GBP:GA3", OperationID: toid.New(100, 0, 5).ToInt64(), LedgerNumber: 100},
			},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// Verify all 3 changes are in the map (different keys)
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 3
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Contract changes are passed through (not deduplicated here)
			name:             "passes_contract_changes_through",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			contractChanges: []types.ContractChange{
				{AccountID: "GA1", ContractID: "C1", OperationID: toid.New(200, 0, 20).ToInt64(), LedgerNumber: 200, ContractType: types.ContractTypeUnknown},
				{AccountID: "GA2", ContractID: "C2", OperationID: toid.New(200, 0, 5).ToInt64(), LedgerNumber: 200, ContractType: types.ContractTypeUnknown},
				{AccountID: "GA3", ContractID: "C3", OperationID: toid.New(201, 0, 1).ToInt64(), LedgerNumber: 201, ContractType: types.ContractTypeUnknown},
			},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// All 3 contract changes are passed through
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 3
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "inserts_trustline_assets_and_calls_ProcessTokenChanges",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")}: {AccountID: "GA1", Asset: "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", OperationID: 1, LedgerNumber: 100, Operation: types.TrustlineOpAdd},
			},
			contractChanges: []types.ContractChange{},
			uniqueAssets: map[uuid.UUID]data.TrustlineAsset{
				data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"): {ID: data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"), Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges is called after trustline assets are inserted to DB
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					if len(changes) != 1 {
						return false
					}
					for _, change := range changes {
						return change.Asset == "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
					}
					return false
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Note: Unknown contracts don't get inserted into contracts table but are still passed to ProcessTokenChanges
			name:                 "processes_unknown_contract_changes",
			trustlineChanges:     map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			contractChanges: []types.ContractChange{
				{AccountID: "GA1", ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100, ContractType: types.ContractTypeUnknown},
			},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// Unknown contracts don't trigger FetchMetadata but are passed to ProcessTokenChanges
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 1 && changes[0].ContractType == types.ContractTypeUnknown
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "calls_ProcessTokenChanges_with_trustline_and_contract_changes",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA")}: {AccountID: "GA1", Asset: "USDC:GA", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100},
			},
			contractChanges: []types.ContractChange{
				{AccountID: "GA2", ContractID: "C1", OperationID: toid.New(100, 0, 2).ToInt64(), LedgerNumber: 100, ContractType: types.ContractTypeUnknown},
			},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges receives both trustline and contract changes
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					if len(changes) != 1 {
						return false
					}
					for _, change := range changes {
						return change.Asset == "USDC:GA"
					}
					return false
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 1 && changes[0].ContractID == "C1"
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "propagates_ProcessTokenChanges_error",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA")}: {AccountID: "GA1", Asset: "USDC:GA", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100},
			},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges receives the actual trustline changes
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 1
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(fmt.Errorf("db error"))
			},
			wantErr:         true,
			wantErrContains: "processing token changes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			mockTokenIngestionService := NewTokenIngestionServiceMock(t)
			mockContractMetadataSvc := NewContractMetadataServiceMock(t)

			tc.setupMocks(t, mockTokenIngestionService, mockContractMetadataSvc)

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:           IngestionModeBackfill,
				Models:                  models,
				LatestLedgerCursorName:  "latest_ledger_cursor",
				OldestLedgerCursorName:  "oldest_ledger_cursor",
				AppTracker:              &apptracker.MockAppTracker{},
				RPCService:              mockRPCService,
				LedgerBackend:           &LedgerBackendMock{},
				TokenIngestionService:   mockTokenIngestionService,
				ContractMetadataService: mockContractMetadataSvc,
				MetricsService:          mockMetricsService,
				GetLedgersLimit:         defaultGetLedgersLimit,
				Network:                 network.TestNetworkPassphrase,
				NetworkPassphrase:       network.TestNetworkPassphrase,
				Archive:                 &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			err = db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
				return svc.processBatchChanges(ctx, dbTx, tc.trustlineChanges, tc.contractChanges, make(map[string]types.AccountChange), make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange), tc.uniqueAssets, tc.uniqueContractTokens, make(map[string]*data.Contract))
			})

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}

			if tc.verifySortedChanges != nil {
				tc.verifySortedChanges(t, mockTokenIngestionService)
			}
		})
	}
}

// Test_ingestService_flushBatchBuffer_batchChanges tests that batch changes are collected
// when flushBatchBuffer is called with a non-nil batchChanges parameter.
func Test_ingestService_flushBatchBuffer_batchChanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                      string
		setupBuffer               func() *indexer.IndexerBuffer
		batchChanges              *BatchChanges
		wantTrustlineChangesCount int
		wantContractChanges       []types.ContractChange
	}{
		{
			name: "collects_trustline_changes_when_batchChanges_provided",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash1, 1)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTrustlineChange(types.TrustlineChange{
					AccountID:    testAddr1,
					Asset:        "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					OperationID:  100,
					LedgerNumber: 1000,
					Operation:    types.TrustlineOpAdd,
				})
				return buf
			},
			batchChanges:              &BatchChanges{TrustlineChangesByKey: make(map[indexer.TrustlineChangeKey]types.TrustlineChange), UniqueTrustlineAssets: make(map[uuid.UUID]data.TrustlineAsset), UniqueContractTokensByID: make(map[string]types.ContractType)},
			wantTrustlineChangesCount: 1,
			wantContractChanges:       nil,
		},
		{
			name: "collects_contract_changes_when_batchChanges_provided",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash2, 2)
				buf.PushTransaction(testAddr2, tx1)
				buf.PushContractChange(types.ContractChange{
					AccountID:    testAddr2,
					ContractID:   "CCONTRACTID",
					OperationID:  101,
					LedgerNumber: 1001,
					ContractType: types.ContractTypeSAC,
				})
				return buf
			},
			batchChanges:              &BatchChanges{TrustlineChangesByKey: make(map[indexer.TrustlineChangeKey]types.TrustlineChange), UniqueTrustlineAssets: make(map[uuid.UUID]data.TrustlineAsset), UniqueContractTokensByID: make(map[string]types.ContractType)},
			wantTrustlineChangesCount: 0,
			wantContractChanges: []types.ContractChange{
				{
					AccountID:    testAddr2,
					ContractID:   "CCONTRACTID",
					OperationID:  101,
					LedgerNumber: 1001,
					ContractType: types.ContractTypeSAC,
				},
			},
		},
		{
			name: "nil_batchChanges_does_not_collect",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash5, 5)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTrustlineChange(types.TrustlineChange{
					AccountID:    testAddr1,
					Asset:        "EUR:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					OperationID:  102,
					LedgerNumber: 1002,
					Operation:    types.TrustlineOpAdd,
				})
				return buf
			},
			batchChanges:              nil, // nil means historical mode - no collection happens
			wantTrustlineChangesCount: 0,
			wantContractChanges:       nil,
		},
		{
			name: "accumulates_across_multiple_flushes",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash6, 6)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTrustlineChange(types.TrustlineChange{
					AccountID:    testAddr1,
					Asset:        "GBP:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					OperationID:  103,
					LedgerNumber: 1003,
					Operation:    types.TrustlineOpAdd,
				})
				return buf
			},
			// Pre-populate batchChanges to simulate accumulation from previous flush
			batchChanges: &BatchChanges{
				TrustlineChangesByKey: map[indexer.TrustlineChangeKey]types.TrustlineChange{
					{AccountID: "GPREV", TrustlineID: data.DeterministicAssetID("PREV", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")}: {AccountID: "GPREV", Asset: "PREV:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", OperationID: 50, LedgerNumber: 999, Operation: types.TrustlineOpAdd},
				},
				UniqueTrustlineAssets:    make(map[uuid.UUID]data.TrustlineAsset),
				UniqueContractTokensByID: make(map[string]types.ContractType),
			},
			wantTrustlineChangesCount: 2, // Pre-existing + new change
			wantContractChanges:       nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up test data from previous runs (using HashBytea for BYTEA column)
			for _, hash := range []string{catchupTxHash1, catchupTxHash2, catchupTxHash3, catchupTxHash4, catchupTxHash5, catchupTxHash6, prevTxHash} {
				_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			// Also clean up any orphan operations
			_, err = dbConnectionPool.ExecContext(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)

			// Set up cursors
			setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

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

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:              IngestionModeBackfill,
				Models:                     models,
				LatestLedgerCursorName:     "latest_ledger_cursor",
				OldestLedgerCursorName:     "oldest_ledger_cursor",
				AppTracker:                 &apptracker.MockAppTracker{},
				RPCService:                 mockRPCService,
				LedgerBackend:              &LedgerBackendMock{},
				MetricsService:             mockMetricsService,
				GetLedgersLimit:            defaultGetLedgersLimit,
				Network:                    network.TestNetworkPassphrase,
				NetworkPassphrase:          network.TestNetworkPassphrase,
				Archive:                    &HistoryArchiveMock{},
				EnableParticipantFiltering: false,
			})
			require.NoError(t, err)

			buffer := tc.setupBuffer()

			err = svc.flushBatchBufferWithRetry(ctx, buffer, nil, tc.batchChanges)
			require.NoError(t, err)

			// Verify collected token changes match expected values
			if tc.batchChanges != nil {
				// Verify trustline changes count
				require.Len(t, tc.batchChanges.TrustlineChangesByKey, tc.wantTrustlineChangesCount, "trustline changes count mismatch")

				// Verify contract changes
				require.Len(t, tc.batchChanges.ContractChanges, len(tc.wantContractChanges), "contract changes count mismatch")
				for i, want := range tc.wantContractChanges {
					got := tc.batchChanges.ContractChanges[i]
					assert.Equal(t, want.AccountID, got.AccountID, "ContractChange[%d].AccountID mismatch", i)
					assert.Equal(t, want.ContractID, got.ContractID, "ContractChange[%d].ContractID mismatch", i)
					assert.Equal(t, want.OperationID, got.OperationID, "ContractChange[%d].OperationID mismatch", i)
					assert.Equal(t, want.LedgerNumber, got.LedgerNumber, "ContractChange[%d].LedgerNumber mismatch", i)
					assert.Equal(t, want.ContractType, got.ContractType, "ContractChange[%d].ContractType mismatch", i)
				}
			}
		})
	}
}

// Test_ingestService_processLedgersInBatch_catchupMode tests that processLedgersInBatch
// returns catchup data for catchup mode and nil for historical mode.
func Test_ingestService_processLedgersInBatch_catchupMode(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Prepare a ledger metadata for the test
	var ledgerMeta xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &ledgerMeta)
	require.NoError(t, err)

	testCases := []struct {
		name                string
		mode                BackfillMode
		wantBatchChangesNil bool
	}{
		{
			name:                "catchup_mode_returns_token_changes",
			mode:                BackfillModeCatchup,
			wantBatchChangesNil: false,
		},
		{
			name:                "historical_mode_returns_nil_token_changes",
			mode:                BackfillModeHistorical,
			wantBatchChangesNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up and set up cursors
			setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			// Mock ledger backend that returns a ledger
			mockLedgerBackend := &LedgerBackendMock{}
			mockLedgerBackend.On("GetLedger", mock.Anything, uint32(4599)).Return(ledgerMeta, nil)

			mockChAccStore := &store.ChannelAccountStoreMock{}
			// Use variadic mock.Anything for any number of tx hashes
			mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          mockLedgerBackend,
				ChannelAccountStore:    mockChAccStore,
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			batch := BackfillBatch{StartLedger: 4599, EndLedger: 4599}
			ledgersProcessed, batchChanges, _, _, err := svc.processLedgersInBatch(ctx, mockLedgerBackend, batch, tc.mode)

			require.NoError(t, err)
			assert.Equal(t, 1, ledgersProcessed)

			if tc.wantBatchChangesNil {
				assert.Nil(t, batchChanges, "expected nil batch changes for historical mode")
			} else {
				assert.NotNil(t, batchChanges, "expected non-nil batch changes for catchup mode")
			}
		})
	}
}

// Test_ingestService_startBackfilling_CatchupMode_ProcessesBatchChanges tests the full catchup
// flow including batch change processing and cursor updates.
//
//nolint:unparam // Test backend factory always returns nil error - this is intentional for testing
func Test_ingestService_startBackfilling_CatchupMode_ProcessesBatchChanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Prepare a ledger metadata for the test
	var ledgerMeta xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &ledgerMeta)
	require.NoError(t, err)
	ledgerSeq := ledgerMeta.LedgerSequence()

	// NOTE: processTokenChanges now only calls:
	// 1. models.TrustlineAsset.BatchInsert (real DB operation)
	// 2. contractMetadataService.FetchMetadata (mocked, but not triggered with empty data)
	// 3. models.Contract.BatchInsert (real DB operation)
	// 4. tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges)
	testCases := []struct {
		name             string
		setupMocks       func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error))
		wantErr          bool
		wantErrContains  string
		wantLatestCursor uint32
	}{
		{
			name: "successful_catchup_processes_token_changes",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges is the only mock needed on tokenIngestionService
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          false,
			wantLatestCursor: ledgerSeq,
		},
		{
			name: "token_processing_error_returns_error",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges fails
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("db connection error"))

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          true,
			wantErrContains:  "processing token changes",
			wantLatestCursor: ledgerSeq - 1, // Cursor should NOT be updated
		},
		{
			name: "cursor_not_updated_if_token_processing_fails",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges fails - cursor should not be updated
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("db error"))

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          true,
			wantErrContains:  "processing token changes",
			wantLatestCursor: ledgerSeq - 1, // Cursor should NOT be updated
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up and set up cursors (latest = startLedger - 1 for catchup mode validation)
			setupDBCursors(t, ctx, dbConnectionPool, ledgerSeq-1, ledgerSeq-1)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			mockTokenIngestionService := NewTokenIngestionServiceMock(t)
			var backendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

			tc.setupMocks(t, mockTokenIngestionService, &backendFactory)

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				LedgerBackendFactory:   backendFactory,
				ChannelAccountStore:    &store.ChannelAccountStoreMock{},
				TokenIngestionService:  mockTokenIngestionService,
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startBackfilling(ctx, ledgerSeq, ledgerSeq, BackfillModeCatchup)

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}

			// Verify cursor state
			cursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantLatestCursor, cursor, "latest ledger cursor mismatch")
		})
	}
}

// Test_ingestService_processBackfillBatchesParallel_BothModes verifies
// that both historical and catchup modes process batches successfully.
func Test_ingestService_processBackfillBatchesParallel_BothModes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name string
		mode BackfillMode
	}{
		{
			name: "historical_mode_processes_batches",
			mode: BackfillModeHistorical,
		},
		{
			name: "catchup_mode_processes_batches",
			mode: BackfillModeCatchup,
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

			// Factory that returns a backend with minimal valid ledger data
			factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				mockBackend := &LedgerBackendMock{}
				mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{
					V: 0,
					V0: &xdr.LedgerCloseMetaV0{
						LedgerHeader: xdr.LedgerHeaderHistoryEntry{
							Header: xdr.LedgerHeader{
								LedgerSeq: xdr.Uint32(100),
							},
						},
					},
				}, nil)
				mockBackend.On("Close").Return(nil)
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

			batches := []BackfillBatch{
				{StartLedger: 100, EndLedger: 100},
				{StartLedger: 101, EndLedger: 101},
			}

			results := svc.processBackfillBatchesParallel(ctx, tc.mode, batches, nil)

			// All batches should succeed
			require.Len(t, results, 2)
			for i, result := range results {
				assert.NoError(t, result.Error, "batch %d should succeed", i)
			}
		})
	}
}
