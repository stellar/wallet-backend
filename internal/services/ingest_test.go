package services

import (
	"context"
	"testing"

	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

const (
	defaultGetLedgersLimit = 50
)

func Test_ingestService_getLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	const ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	const ledgerMetadataWith1Tx = "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"

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
			result := svc.splitGapsIntoBatches(tc.gaps, tc.batchSize)
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
