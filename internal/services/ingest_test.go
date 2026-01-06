package services

import (
	"context"
	"fmt"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

const (
	defaultGetLedgersLimit = 50

	// Test fixtures for ledger metadata
	ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	ledgerMetadataWith1Tx = "AAAAAQAAAAD8G2qemHnBKFkbq90RTagxAypNnA7DXDc63Giipq9mNwAAABYLEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VPJvwbisrc9A0yzFxxCdkICgB3Gv7qHOi8ZdsK2CNks2AAAAAGhTTAsAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAunZtorOSbnRpgnykoDe4kzAvLwNXefncy1R/1ynBWyDv0DfdnqJ6Hcy/0AJf6DkBZlRayg775h3HjV0GKF/oPua7l8wkLlJBtSk1kRDt55qSf6btSrgcupB/8bnpJfUUgZJ76saUrj29HukYHS1bq7SyuoCAY+5F9iBYTmW1G9QAAEX4N4Lazp2QAAAAAAAMtS3veAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAELEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VAAAAAIAAAAAAAAAAQAAAAAAAAABAAAAAAAAAGQAAAABAAAAAgAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAGQAAA7FAAAAGgAAAAAAAAAAAAAAAQAAAAAAAAABAAAAALvqzdVyRxgBMcLzbw1wNWcJYHPNPok1GdVSgmy4sjR2AAAAAVVTREMAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAACVAvkAAAAAAAAAAABhHevAAAAAEDq2yIDzXUoLboBHQkbr8U2oKqLzf0gfpwXbmRPLB6Ek3G8uCEYyry1vt5Sb+LCEd81fefFQcQN0nydr1FmiXcDAAAAAAAAAAAAAAABXFSiWcxpDRa8frBs1wbEaMUw4hMe7ctFtdw3Ci73IEwAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAIAAAADAAARfQAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3GPAAADsUAAAAZAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF9AAAAAGhTTAYAAAAAAAAAAQAAEX4AAAAAAAAAAODia2IsqMlWCuY6k734V/dcCafJwfI1Qq7+/0qEd68AAAAALpDtxdgAAA7FAAAAGQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAAARfQAAAABoU0wGAAAAAAAAAAMAAAAAAAAAAgAAAAMAABF+AAAAAAAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAC6Q7cXYAAAOxQAAABkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAAEX0AAAAAaFNMBgAAAAAAAAABAAARfgAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3F2AAADsUAAAAaAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF+AAAAAGhTTAsAAAAAAAAAAQAAAAIAAAADAAARcwAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAlQL5AAf/////////8AAAABAAAAAAAAAAAAAAABAAARfgAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAukO3QAf/////////8AAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8RxEAAAAAAAAAAA=="
)

func Test_ingestService_getLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name                    string
		inputLedgerCloseMetaStr string
		wantErrContains         string
		wantResultTxHashes      []string
	}{
		{
			name:                    "🟢successful_transaction_reading_0_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith0Tx,
		},
		{
			name:                    "🟢successful_transaction_reading_1_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith1Tx,
			wantErrContains:         "",
			wantResultTxHashes:      []string{"5c54a259cc690d16bc7eb06cd706c468c530e2131eedcb45b5dc370a2ef7204c"},
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

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}
			ingestService, err := NewIngestService(IngestServiceConfig{
				IngestionMode:              IngestionModeLive,
				Models:                     models,
				LatestLedgerCursorName:     "testCursor",
				AppTracker:                 &mockAppTracker,
				RPCService:                 &mockRPCService,
				LedgerBackend:              mockLedgerBackend,
				ChannelAccountStore:        mockChAccStore,
				AccountTokenService:        nil,
				ContractMetadataService:    nil,
				MetricsService:             mockMetricsService,
				GetLedgersLimit:            defaultGetLedgersLimit,
				Network:                    network.TestNetworkPassphrase,
				NetworkPassphrase:          network.TestNetworkPassphrase,
				Archive:                    mockArchive,
				SkipTxMeta:                 false,
				SkipTxEnvelope:             false,
				EnableParticipantFiltering: false,
			})
			require.NoError(t, err)

			var xdrLedgerCloseMeta xdr.LedgerCloseMeta
			err = xdr.SafeUnmarshalBase64(tc.inputLedgerCloseMetaStr, &xdrLedgerCloseMeta)
			require.NoError(t, err)
			transactions, err := ingestService.getLedgerTransactions(ctx, xdrLedgerCloseMeta)

			// Verify results
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Len(t, transactions, len(tc.wantResultTxHashes))

				// Verify transaction hashes if we have expected results
				if len(tc.wantResultTxHashes) > 0 {
					for i, expectedHash := range tc.wantResultTxHashes {
						assert.Equal(t, expectedHash, transactions[i].Hash.HexString())
					}
				}
			}
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
				// Set oldest to 100, latest to 200
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 100)`)
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', 200)`)
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
				// Set oldest to 100, latest to 200
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 100)`)
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', 200)`)
				require.NoError(t, err)
				// Insert transactions for 100-200 (no gaps)
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 'res', 'meta', $3, NOW())`,
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
				// Set oldest to 100, latest to 200
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 100)`)
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', 200)`)
				require.NoError(t, err)
				// Insert transactions for 100-200 (no gaps)
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 'res', 'meta', $3, NOW())`,
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
				// Set oldest to 100, latest to 200
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 100)`)
				require.NoError(t, err)
				_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', 200)`)
				require.NoError(t, err)
				// Insert transactions with gaps: 100-120, 150-200 (gap at 121-149)
				for ledger := uint32(100); ledger <= 120; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 'res', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
				for ledger := uint32(150); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 'res', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 121, GapEnd: 149},
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

			err = svc.initializeCursors(ctx, tc.startLedger)
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

func Test_ingestService_flushBatchBuffer(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name           string
		updateCursorTo *uint32
		initialCursor  uint32
		wantCursor     uint32
	}{
		{
			name:           "flush_without_cursor_update",
			updateCursorTo: nil,
			initialCursor:  100,
			wantCursor:     100, // Unchanged
		},
		{
			name:           "flush_with_cursor_update_to_lower_value",
			updateCursorTo: ptrUint32(50),
			initialCursor:  100,
			wantCursor:     50, // Updated to lower
		},
		{
			name:           "flush_with_cursor_update_to_higher_value_keeps_existing",
			updateCursorTo: ptrUint32(150),
			initialCursor:  100,
			wantCursor:     100, // UpdateMin keeps lower
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
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
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

			// Create an empty buffer
			buffer := indexer.NewIndexerBuffer()

			// Call flushBatchBuffer
			err = svc.flushBatchBuffer(ctx, buffer, tc.updateCursorTo)
			require.NoError(t, err)

			// Verify the cursor value
			cursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor)
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
	}{
		{
			name:                       "filtering_disabled_returns_all",
			enableParticipantFiltering: false,
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				// Empty buffer - just testing the flow
				return buf
			},
			wantTxCount: 0,
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
					`INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`, acc)
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
				return nil
			})
			require.NoError(t, txErr)
		})
	}
}

func Test_ContractMetadataServiceMock(t *testing.T) {
	// Simple test to verify the mock can be created and used
	mockService := NewContractMetadataServiceMock(t)
	mockService.On("FetchAndStoreMetadata", mock.Anything, mock.Anything).Return(nil)

	ctx := context.Background()
	err := mockService.FetchAndStoreMetadata(ctx, nil)
	require.NoError(t, err)
}
