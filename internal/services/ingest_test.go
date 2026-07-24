package services

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/stellar/wallet-backend/internal/utils"
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
	txHash1      = "1111111111111111111111111111111111111111111111111111111111111111"
	txHash2      = "2222222222222222222222222222222222222222222222222222222222222222"

	// Test fixtures for ledger metadata
	ledgerMetadataWith1Tx = "AAAAAQAAAAD8G2qemHnBKFkbq90RTagxAypNnA7DXDc63Giipq9mNwAAABYLEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VPJvwbisrc9A0yzFxxCdkICgB3Gv7qHOi8ZdsK2CNks2AAAAAGhTTAsAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAunZtorOSbnRpgnykoDe4kzAvLwNXefncy1R/1ynBWyDv0DfdnqJ6Hcy/0AJf6DkBZlRayg775h3HjV0GKF/oPua7l8wkLlJBtSk1kRDt55qSf6btSrgcupB/8bnpJfUUgZJ76saUrj29HukYHS1bq7SyuoCAY+5F9iBYTmW1G9QAAEX4N4Lazp2QAAAAAAAMtS3veAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAELEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VAAAAAIAAAAAAAAAAQAAAAAAAAABAAAAAAAAAGQAAAABAAAAAgAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAGQAAA7FAAAAGgAAAAAAAAAAAAAAAQAAAAAAAAABAAAAALvqzdVyRxgBMcLzbw1wNWcJYHPNPok1GdVSgmy4sjR2AAAAAVVTREMAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAACVAvkAAAAAAAAAAABhHevAAAAAEDq2yIDzXUoLboBHQkbr8U2oKqLzf0gfpwXbmRPLB6Ek3G8uCEYyry1vt5Sb+LCEd81fefFQcQN0nydr1FmiXcDAAAAAAAAAAAAAAABXFSiWcxpDRa8frBs1wbEaMUw4hMe7ctFtdw3Ci73IEwAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAIAAAADAAARfQAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3GPAAADsUAAAAZAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF9AAAAAGhTTAYAAAAAAAAAAQAAEX4AAAAAAAAAAODia2IsqMlWCuY6k734V/dcCafJwfI1Qq7+/0qEd68AAAAALpDtxdgAAA7FAAAAGQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAAARfQAAAABoU0wGAAAAAAAAAAMAAAAAAAAAAgAAAAMAABF+AAAAAAAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAC6Q7cXYAAAOxQAAABkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAAEX0AAAAAaFNMBgAAAAAAAAABAAARfgAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3F2AAADsUAAAAaAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF+AAAAAGhTTAsAAAAAAAAAAQAAAAIAAAADAAARcwAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAlQL5AAf/////////8AAAABAAAAAAAAAAAAAAABAAARfgAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAukO3QAf/////////8AAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8RxEAAAAAAAAAAA=="
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

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()
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
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startBackfilling(ctx, tc.startLedger, tc.endLedger)
			if tc.expectValidationError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				// For valid cases, validation passes but batch processing fails.
				// Backfill logs batch failures and returns nil rather than erroring.
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
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, data.LatestLedgerCursorName, fmt.Sprintf("%d", latestLedger))
		require.NoError(t, err)
	}
	if oldestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`, fmt.Sprintf("%d", oldestLedger))
		require.NoError(t, err)
	}
}

// ptrUint32 returns a pointer to the given uint32 value
func ptrUint32(v uint32) *uint32 { return &v }

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

// ==================== New Tests ====================

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
			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				LedgerBackendFactory:   tc.setupFactory(),
				Metrics:                m,
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

func Test_ingestService_flushBatchBufferWithRetry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                 string
		setupBuffer          func() *indexer.IndexerBuffer
		updateCursorTo       *uint32
		initialCursor        uint32
		wantCursor           uint32
		wantTxCount          int
		wantOpCount          int
		wantStateChangeCount int
		txHashes             []string // For verification queries
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

				buf.PushTransaction(testAddr1, &tx1)
				buf.PushTransaction(testAddr2, &tx2)
				buf.PushOperation(testAddr1, &op1, &tx1)
				buf.PushOperation(testAddr2, &op2, &tx2)
				buf.PushStateChange(&tx1, &op1, sc1)
				buf.PushStateChange(&tx2, &op2, sc2)
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
				buf.PushTransaction(testAddr1, &tx1)
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
				buf.PushTransaction(testAddr1, &tx1)
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

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, err := data.NewModels(dbConnectionPool, m.DB)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			// Use variadic mock.Anything for any number of tx hashes

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
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

			buffer := tc.setupBuffer()

			// Call flushBatchBuffer
			err = svc.flushBatchBufferWithRetry(ctx, buffer, tc.updateCursorTo)
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
			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
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
				BackfillBatchSize:      10,
			})
			require.NoError(t, svcErr)

			results := svc.processBackfillBatchesParallel(ctx, tc.batches, nil)

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
				`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
				VALUES ('anchor_hash', 1, 100, 'TransactionResultCodeTxSuccess', $1, NOW())`,
				tc.initialOldest)
			require.NoError(t, err)

			m := metrics.NewMetrics(prometheus.NewRegistry())

			models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
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
				BackfillBatchSize:      int(tc.batchSize),
			})
			require.NoError(t, svcErr)

			// Run backfilling
			backfillErr := svc.startBackfilling(ctx, tc.startLedger, tc.endLedger)

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

	m := metrics.NewMetrics(prometheus.NewRegistry())

	models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
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
		OldestLedgerCursorName:    "oldest_ledger_cursor",
		AppTracker:                &apptracker.MockAppTracker{},
		RPCService:                mockRPCService,
		LedgerBackend:             &LedgerBackendMock{},
		LedgerBackendFactory:      factory,
		Metrics:                   m,
		GetLedgersLimit:           defaultGetLedgersLimit,
		Network:                   network.TestNetworkPassphrase,
		NetworkPassphrase:         network.TestNetworkPassphrase,
		Archive:                   &HistoryArchiveMock{},
		BackfillBatchSize:         10,
		BackfillDBInsertBatchSize: 50,
	})
	require.NoError(t, svcErr)

	// Process both batches in parallel
	results := svc.processBackfillBatchesParallel(ctx, batches, nil)

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

	m := metrics.NewMetrics(prometheus.NewRegistry())

	models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
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
		BackfillBatchSize:      10,
	})
	require.NoError(t, svcErr)

	// Run backfilling with all batches failing
	backfillErr := svc.startBackfilling(ctx, 50, 99)

	// Historical mode should NOT return error even when all batches fail
	require.NoError(t, backfillErr, "historical mode should not return error even when all batches fail")

	// Verify cursor remains unchanged
	finalOldest, getErr := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, initialOldest, finalOldest,
		"oldest cursor should remain unchanged when all batches fail")

	finalLatest, getErr := models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
	require.NoError(t, getErr)
	assert.Equal(t, initialLatest, finalLatest,
		"latest cursor should remain unchanged when all batches fail")
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
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
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
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, nil, buffer)

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
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
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
		_, _, err = svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, nil, buffer)

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
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
		).Return(fmt.Errorf("transient error")).Once()
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
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
		numTx, numOps, err := svc.ingestProcessedDataWithRetry(ctx, 100, xdr.LedgerCloseMeta{}, nil, buffer)

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

// Test_ingestService_processBackfillBatchesParallel_Success verifies that
// backfill batches process successfully.
func Test_ingestService_processBackfillBatchesParallel_Success(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())

	models, modelsErr := data.NewModels(dbConnectionPool, m.DB)
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
		BackfillBatchSize:      10,
	})
	require.NoError(t, svcErr)

	batches := []BackfillBatch{
		{StartLedger: 100, EndLedger: 100},
		{StartLedger: 101, EndLedger: 101},
	}

	results := svc.processBackfillBatchesParallel(ctx, batches, nil)

	// All batches should succeed
	require.Len(t, results, 2)
	for i, result := range results {
		assert.NoError(t, result.Error, "batch %d should succeed", i)
	}
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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, // ctx
			mock.Anything, // dbTx
			mock.Anything, // trustlineChangesByTrustlineKey
			mock.Anything, // accountChangesByAccountID
			mock.Anything, // sacBalanceChangesByKey
			mock.Anything, // lpShareChangesByKey
			mock.Anything, // lpChangesByPoolID
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
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		buffer := indexer.NewIndexerBuffer()
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, nil, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
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

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, nil, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
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
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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
		require.NoError(t, svc.snapshotProtocolCursors(ctx))

		// First ledger succeeds and advances the current-state cursor to 100.
		processor.processedLedger = 100
		meta100 := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta100, nil, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
		require.NoError(t, err)

		// Next ledger fails inside PersistCurrentState, rolling back the whole
		// transaction — the current-state cursor must stay at 100.
		processor.processedLedger = 101
		meta101 := dummyLedgerMeta(101)
		_, _, err = svc.persistLedgerData(ctx, 101, &meta101, nil, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
		require.Error(t, err)

		currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
		require.NoError(t, err)
		assert.Equal(t, uint32(100), currentStateCursor)

		// Retrying the same ledger succeeds and advances the cursor.
		processor.failPersistCurrentStateAt = 0
		processor.processedLedger = 101
		_, _, err = svc.persistLedgerData(ctx, 101, &meta101, nil, indexer.NewIndexerBuffer(), "latest_ledger_cursor")
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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
		require.Error(t, err)
		assert.ErrorIs(t, err, data.ErrCASCursorMissing)

		// The transaction rolled back entirely: even the unrelated main cursor
		// (a different cursorName, not gated by any snapshot) did not advance.
		mainCursor, getErr := models.IngestStore.Get(ctx, "latest_ledger_cursor")
		require.NoError(t, getErr)
		assert.Equal(t, uint32(0), mainCursor)

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
		meta := dummyLedgerMeta(100)
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
		require.NoError(t, err)

		histCursor, getErr := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
		require.NoError(t, getErr)
		assert.Equal(t, uint32(100), histCursor)
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
		_, _, err := svc.persistLedgerData(ctx, 100, &meta, nil, buffer, "latest_ledger_cursor")
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

		plan, err := svc.prepareClassificationPlan(ctx, nil, nil, nil)
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
			},
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
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}})
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
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}})
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
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}})
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
			map[string]data.ProtocolContracts{string(c2): {ContractID: c2, WasmHash: w2}})
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
		mockTokenIngestionService.On("ProcessTokenChanges",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything,
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

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, plan, buffer, "latest_ledger_cursor")
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

		meta := dummyLedgerMeta(100)
		_, _, err = svc.persistLedgerData(ctx, 100, &meta, plan, buffer, "latest_ledger_cursor")
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
	mockTokenIngestionService.On("ProcessTokenChanges",
		mock.Anything, // ctx
		mock.Anything, // dbTx
		mock.Anything, // trustlineChangesByTrustlineKey
		mock.Anything, // accountChangesByAccountID
		mock.Anything, // sacBalanceChangesByKey
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

	noopCheckLockSession := func(context.Context) error { return nil }
	done := make(chan error, 1)
	go func() { done <- svc.ingestLiveLedgers(ctx, startLedger, noopCheckLockSession) }()

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

// Test_ingestService_ingestLiveLedgers_DeadLockSessionExitsFatally is a regression test for
// ING-05: a CNPG failover can kill the Postgres session holding the advisory lock without this
// process observing the disconnect, silently releasing the lock while pgxpool never destroys the
// (now server-dead) pooled connection. checkLockSession must be probed every ledger and, on
// failure, ingestLiveLedgers must return immediately — not after exhausting the ledger-fetch
// retry ladder (maxLedgerFetchRetries attempts, up to ~2.5 minutes) — so the process can exit and
// re-acquire the lock cleanly on restart.
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

	// GetLedger must never be reached: the liveness probe is checked at the top of the loop,
	// before any ledger fetch.
	mockBackend := &LedgerBackendMock{}

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockBackend,
		Metrics:                m,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
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
	mockBackend.AssertNotCalled(t, "GetLedger", mock.Anything, mock.Anything)
}
