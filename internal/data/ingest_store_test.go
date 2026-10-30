package data

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func Test_IngestStoreModel_GetLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		key            string
		setupDB        func(t *testing.T)
		expectedLedger uint32
	}{
		{
			name:           "returns_0_if_key_does_not_exist",
			key:            "ingest_store_key",
			expectedLedger: 0,
		},
		{
			name: "returns_value_if_key_exists",
			key:  "ingest_store_key",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "ingest_store_key", "123")
				require.NoError(t, err)
			},
			expectedLedger: 123,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}
			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			lastSyncedLedger, err := m.Get(ctx, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedLedger, lastSyncedLedger)
		})
	}
}

func Test_IngestStoreModel_GetMany(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name     string
		keys     []string
		setupDB  func(t *testing.T)
		expected map[string]uint32
	}{
		{
			name:     "nil_keys_returns_nil",
			keys:     nil,
			expected: nil,
		},
		{
			name:     "empty_keys_returns_nil",
			keys:     []string{},
			expected: nil,
		},
		{
			name:     "no_matching_keys",
			keys:     []string{"missing_a", "missing_b"},
			expected: map[string]uint32{},
		},
		{
			name: "all_keys_present",
			keys: []string{"key_a", "key_b", "key_c"},
			setupDB: func(t *testing.T) {
				for _, kv := range []struct {
					k string
					v string
				}{{"key_a", "10"}, {"key_b", "20"}, {"key_c", "30"}} {
					_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, kv.k, kv.v)
					require.NoError(t, err)
				}
			},
			expected: map[string]uint32{"key_a": 10, "key_b": 20, "key_c": 30},
		},
		{
			name: "partial_match",
			keys: []string{"key_a", "missing", "key_c"},
			setupDB: func(t *testing.T) {
				for _, kv := range []struct {
					k string
					v string
				}{{"key_a", "10"}, {"key_c", "30"}} {
					_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, kv.k, kv.v)
					require.NoError(t, err)
				}
			},
			expected: map[string]uint32{"key_a": 10, "key_c": 30},
		},
		{
			name: "single_key",
			keys: []string{"key_a"},
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "key_a", "42")
				require.NoError(t, err)
			},
			expected: map[string]uint32{"key_a": 42},
		},
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}
			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			result, err := m.GetMany(ctx, tc.keys)
			require.NoError(t, err)
			if tc.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func Test_IngestStoreModel_UpdateLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		key            string
		ledgerToUpsert uint32
		setupDB        func(t *testing.T)
	}{
		{
			name:           "inserts_if_key_does_not_exist",
			key:            "ingest_store_key",
			ledgerToUpsert: 123,
		},
		{
			name: "updates_if_key_exists",
			key:  "ingest_store_key",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "ingest_store_key", "123")
				require.NoError(t, err)
			},
			ledgerToUpsert: 456,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			err = db.RunInTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				if err := m.Update(ctx, dbTx, tc.key, tc.ledgerToUpsert); err != nil {
					return err
				}
				return nil
			})
			require.NoError(t, err)

			dbStoredLedger, err := db.QueryOne[uint32](ctx, m.DB, `SELECT value::int FROM ingest_store WHERE key = $1`, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.ledgerToUpsert, dbStoredLedger)
		})
	}
}

func Test_IngestStoreModel_UpdateGuarded(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	const key = "guarded_cursor"

	testCases := []struct {
		name          string
		setupDB       func(t *testing.T) // nil means no row inserted
		ledger        uint32
		expectErr     bool
		expectedValue string // DB value after the call; only checked when !expectErr
	}{
		{
			// Normal sequential case: current value is ledger-1.
			name: "advances_from_ledger_minus_one",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, '99')`, key)
				require.NoError(t, err)
			},
			ledger:        100,
			expectedValue: "100",
		},
		{
			// Self-value case: the first ledger processed right after
			// startLiveIngestion's initializeCursors already set the cursor to
			// this same starting ledger.
			name: "self_value_is_a_noop_success",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, '100')`, key)
				require.NoError(t, err)
			},
			ledger:        100,
			expectedValue: "100",
		},
		{
			// Regression guard: a second writer already advanced the cursor past
			// what this caller expected (e.g. a second live-ingestion instance
			// that acquired the lock after this session's Postgres session died
			// in a failover). Must refuse rather than overwrite.
			name: "refuses_when_cursor_already_advanced_past_expected",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, '105')`, key)
				require.NoError(t, err)
			},
			ledger:    100,
			expectErr: true,
		},
		{
			name:      "errors_when_row_missing",
			ledger:    100,
			expectErr: true,
		},
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}
			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			err = db.RunInTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				return m.UpdateGuarded(ctx, dbTx, key, tc.ledger)
			})

			if tc.expectErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrCursorGuardFailed), "expected ErrCursorGuardFailed, got %v", err)
				return
			}
			require.NoError(t, err)

			var dbValue string
			scanErr := m.DB.QueryRow(ctx, `SELECT value FROM ingest_store WHERE key = $1`, key).Scan(&dbValue)
			require.NoError(t, scanErr)
			assert.Equal(t, tc.expectedValue, dbValue)
		})
	}
}

func Test_IngestStoreModel_CompareAndSwap(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name          string
		setupDB       func(t *testing.T)
		expectedValue string
		newValue      string
		expectedSwap  bool
		expectedErr   error  // sentinel error expected (nil means no error)
		expectedDB    string // expected value in DB after CAS; empty means key should not exist
	}{
		{
			name: "succeeds_when_value_matches",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "cas_cursor", "100")
				require.NoError(t, err)
			},
			expectedValue: "100",
			newValue:      "200",
			expectedSwap:  true,
			expectedDB:    "200",
		},
		{
			name: "fails_when_value_mismatches",
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "cas_cursor", "100")
				require.NoError(t, err)
			},
			expectedValue: "50",
			newValue:      "200",
			expectedSwap:  false,
			expectedDB:    "100",
		},
		{
			name:          "errors_when_key_not_found",
			expectedValue: "100",
			newValue:      "200",
			expectedSwap:  false,
			expectedErr:   ErrCASCursorMissing,
			expectedDB:    "",
		},
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			var swapped bool
			var casErr error
			err = db.RunInTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				swapped, casErr = m.CompareAndSwap(ctx, dbTx, "cas_cursor", tc.expectedValue, tc.newValue)
				// Return nil so the transaction commits and we can assert on
				// the post-CAS DB state below. casErr is asserted separately.
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expectedSwap, swapped)
			if tc.expectedErr != nil {
				require.Error(t, casErr)
				assert.True(t, errors.Is(casErr, tc.expectedErr), "expected error %v, got %v", tc.expectedErr, casErr)
			} else {
				require.NoError(t, casErr)
			}

			if tc.expectedDB != "" {
				var dbValue string
				err = m.DB.QueryRow(ctx, `SELECT value FROM ingest_store WHERE key = $1`, "cas_cursor").Scan(&dbValue)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedDB, dbValue)
			} else {
				var count int
				err = m.DB.QueryRow(ctx, `SELECT COUNT(*) FROM ingest_store WHERE key = $1`, "cas_cursor").Scan(&count)
				require.NoError(t, err)
				assert.Equal(t, 0, count)
			}
		})
	}
}

func Test_IngestStoreModel_UpdateMin(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		key            string
		initialValue   uint32
		newValue       uint32
		expectedResult uint32
	}{
		{
			name:           "updates_to_smaller_value",
			key:            "oldest_ingest_ledger",
			initialValue:   1000,
			newValue:       500,
			expectedResult: 500,
		},
		{
			name:           "keeps_existing_smaller_value",
			key:            "oldest_ingest_ledger",
			initialValue:   500,
			newValue:       1000,
			expectedResult: 500,
		},
		{
			name:           "keeps_same_value",
			key:            "oldest_ingest_ledger",
			initialValue:   500,
			newValue:       500,
			expectedResult: 500,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			// Insert initial value — ingest_store.value is varchar, must pass string
			_, err = dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, tc.key, strconv.FormatUint(uint64(tc.initialValue), 10))
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			err = db.RunInTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				return m.UpdateMin(ctx, dbTx, tc.key, tc.newValue)
			})
			require.NoError(t, err)

			dbStoredLedger, err := db.QueryOne[uint32](ctx, m.DB, `SELECT value::int FROM ingest_store WHERE key = $1`, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedResult, dbStoredLedger)
		})
	}
}

func Test_IngestStoreModel_GetLedgerGaps(t *testing.T) {
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
		expectedGaps []LedgerRange
	}{
		{
			name:         "returns_empty_when_no_transactions",
			startLedger:  1,
			endLedger:    100,
			expectedGaps: []LedgerRange{},
		},
		{
			name:        "returns_empty_when_no_gaps",
			startLedger: 100,
			endLedger:   102,
			setupDB: func(t *testing.T) {
				// Insert consecutive ledgers: 100, 101, 102
				for i, ledger := range []uint32{100, 101, 102} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{},
		},
		{
			name:        "returns_single_gap",
			startLedger: 100,
			endLedger:   105,
			setupDB: func(t *testing.T) {
				// Insert ledgers 100 and 105, creating gap 101-104
				for i, ledger := range []uint32{100, 105} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 104},
			},
		},
		{
			name:        "returns_multiple_gaps",
			startLedger: 100,
			endLedger:   110,
			setupDB: func(t *testing.T) {
				// Insert ledgers 100, 105, 110, creating gaps 101-104 and 106-109
				for i, ledger := range []uint32{100, 105, 110} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 104},
				{GapStart: 106, GapEnd: 109},
			},
		},
		{
			name:        "handles_single_ledger_gap",
			startLedger: 100,
			endLedger:   102,
			setupDB: func(t *testing.T) {
				// Insert ledgers 100 and 102, creating gap of just 101
				for i, ledger := range []uint32{100, 102} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 101},
			},
		},
		{
			// SQL-05: the real next ledger (200) lies beyond the requested window (150). The
			// trailing gap must be reported clipped to endLedger, not dropped just because LEAD
			// finds no next row inside the window.
			name:        "clips_trailing_gap_still_open_at_window_boundary",
			startLedger: 100,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				for i, ledger := range []uint32{100, 200} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 150},
			},
		},
		{
			// Ledgers outside [startLedger, endLedger] must not affect the result: a gap beyond
			// endLedger (106-109) is excluded entirely, proving the query is bounded rather than
			// scanning the whole table.
			name:        "ignores_gaps_entirely_outside_the_window",
			startLedger: 100,
			endLedger:   104,
			setupDB: func(t *testing.T) {
				for i, ledger := range []uint32{100, 105, 110} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 104},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			gaps, err := m.GetLedgerGaps(ctx, tc.startLedger, tc.endLedger)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func Test_IngestStoreModel_GetOldestLedger(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		setupDB        func(t *testing.T)
		expectedLedger uint32
	}{
		{
			name:           "returns_zero_when_table_is_empty",
			expectedLedger: 0,
		},
		{
			name: "returns_oldest_ledger_when_data_exists",
			setupDB: func(t *testing.T) {
				// Insert ledgers with distinct timestamps so ORDER BY ledger_created_at
				// returns the correct oldest regardless of to_id ordering.
				for i, ledger := range []uint32{150, 100, 200} {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
						VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, NOW() - INTERVAL '1 day' * $4)`,
						fmt.Sprintf("hash%d", i), i+1, ledger, 300-int(ledger))
					require.NoError(t, err)
				}
			},
			expectedLedger: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &IngestStoreModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			oldest, err := m.GetOldestLedger(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedLedger, oldest)
		})
	}
}

// Test_IngestStoreModel_DeleteRowsAboveLedger seeds two consecutive ledgers across all five
// bulk-COPY tables and verifies the startup reconciliation removes exactly the rows above the
// cursor ledger — the shape a crash between persistLedgerData's sibling commits and its
// coordinating commit leaves behind — and is a no-op when run again.
func Test_IngestStoreModel_DeleteRowsAboveLedger(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &IngestStoreModel{
		DB:      dbConnectionPool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	const cursorLedger = uint32(100)
	seedBulkCopyLedger(ctx, t, dbConnectionPool, cursorLedger, "2026-01-01T00:00:00Z")
	seedBulkCopyLedger(ctx, t, dbConnectionPool, cursorLedger+1, "2026-01-01T00:00:05Z")

	// Lookup rows carry no TOID; they are split at the cursor ledger's close time.
	countHashes := func(t *testing.T) [2]int {
		var atCursor, aboveCursor int
		require.NoError(t, dbConnectionPool.QueryRow(ctx,
			`SELECT count(*) FILTER (WHERE ledger_close_epoch <= $1), count(*) FILTER (WHERE ledger_close_epoch > $1) FROM transaction_hashes`,
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&atCursor, &aboveCursor))
		return [2]int{atCursor, aboveCursor}
	}
	require.Equal(t, [2]int{1, 1}, countHashes(t))

	countRows := func(t *testing.T) map[string][2]int {
		counts := make(map[string][2]int)
		for _, target := range BulkCopyTables {
			var atCursor, aboveCursor int
			boundary := toid.New(int32(cursorLedger+1), 0, 0).ToInt64()
			require.NoError(t, dbConnectionPool.QueryRow(ctx,
				fmt.Sprintf(`SELECT count(*) FILTER (WHERE %[1]s < $1), count(*) FILTER (WHERE %[1]s >= $1) FROM %[2]s`, target.TOIDColumn, target.Table),
				boundary).Scan(&atCursor, &aboveCursor))
			counts[target.Table] = [2]int{atCursor, aboveCursor}
		}
		return counts
	}

	for table, c := range countRows(t) {
		require.Equal(t, [2]int{1, 1}, c, "seed should place one row per ledger in %s", table)
	}

	require.NoError(t, m.DeleteRowsAboveLedger(ctx, cursorLedger))
	for table, c := range countRows(t) {
		assert.Equal(t, [2]int{1, 0}, c, "%s must keep the cursor ledger's row and lose the orphan", table)
	}
	assert.Equal(t, [2]int{1, 0}, countHashes(t), "transaction_hashes must keep the cursor ledger's row and lose the orphan")

	// Idempotent: nothing above the cursor remains, so a second run changes nothing.
	require.NoError(t, m.DeleteRowsAboveLedger(ctx, cursorLedger))
	for table, c := range countRows(t) {
		assert.Equal(t, [2]int{1, 0}, c, "%s must be unchanged by a reconciliation no-op", table)
	}

	// A cursor whose successor does not fit the toid ledger field must be
	// rejected before the bound is computed, not wrap and delete every row.
	require.Error(t, m.DeleteRowsAboveLedger(ctx, math.MaxInt32))
	for table, c := range countRows(t) {
		assert.Equal(t, [2]int{1, 0}, c, "%s must be untouched by a rejected cursor", table)
	}

	// The close-time bound comes from the cursor ledger's own transactions
	// rows. With no such row the bound is unresolvable and the deletes fall
	// back to scanning unbounded — slower, but orphans must still go.
	seedBulkCopyLedger(ctx, t, dbConnectionPool, cursorLedger+2, "2026-01-01T00:00:10Z")
	_, delErr := dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
	require.NoError(t, delErr)
	require.NoError(t, m.DeleteRowsAboveLedger(ctx, cursorLedger))
	for table, c := range countRows(t) {
		if table == "transactions" {
			assert.Equal(t, [2]int{0, 0}, c, "transactions was emptied by the test itself")
			continue
		}
		assert.Equal(t, [2]int{1, 0}, c, "%s must lose its orphan even without a resolvable close-time bound", table)
	}
	assert.Equal(t, [2]int{1, 1}, countHashes(t), "without a close-time bound the orphan lookup row stays; lookups tolerate it")
}

// seedBulkCopyLedger writes one row per bulk-COPY table for the given ledger, all
// closed at closedAt, plus that transaction's transaction_hashes lookup row.
func seedBulkCopyLedger(ctx context.Context, t *testing.T, dbConnectionPool *pgxpool.Pool, ledger uint32, closedAt string) {
	t.Helper()
	txTOID := toid.New(int32(ledger), 1, 0).ToInt64()
	opTOID := toid.New(int32(ledger), 1, 1).ToInt64()
	_, seedErr := dbConnectionPool.Exec(ctx,
		`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
		 VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, $4::timestamptz)`,
		fmt.Appendf(nil, "hash-%d", ledger), txTOID, ledger, closedAt)
	require.NoError(t, seedErr)
	_, seedErr = dbConnectionPool.Exec(ctx,
		`INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id)
		 VALUES ($1::timestamptz, $2, $3)`, closedAt, txTOID, []byte{1})
	require.NoError(t, seedErr)
	_, seedErr = dbConnectionPool.Exec(ctx,
		`INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		 VALUES ($1, 'PAYMENT', $2, 'op_success', true, $3, $4::timestamptz)`,
		opTOID, []byte{0}, ledger, closedAt)
	require.NoError(t, seedErr)
	_, seedErr = dbConnectionPool.Exec(ctx,
		`INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		 VALUES ($1::timestamptz, $2, $3)`, closedAt, opTOID, []byte{1})
	require.NoError(t, seedErr)
	_, seedErr = dbConnectionPool.Exec(ctx,
		`INSERT INTO state_changes (to_id, operation_id, state_change_id, state_change_category, state_change_reason, ledger_number, account_id, ledger_created_at)
		 VALUES ($1, $2, 1, 'BALANCE', 'CREDIT', $3, $4, $5::timestamptz)`,
		txTOID, opTOID, ledger, []byte{1}, closedAt)
	require.NoError(t, seedErr)
	// The transaction's lookup row, written by the same sibling COPY transaction.
	_, seedErr = dbConnectionPool.Exec(ctx,
		`INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch)
		 VALUES (1, 1, extract(epoch FROM $1::timestamptz)::bigint)`, closedAt)
	require.NoError(t, seedErr)
}

// bulkCopyLedgersPerTable returns, for each bulk-COPY table, the ledgers its rows
// belong to, read from the high 32 bits of the table's TOID column.
func bulkCopyLedgersPerTable(ctx context.Context, t *testing.T, dbConnectionPool *pgxpool.Pool) map[string][]uint32 {
	t.Helper()
	ledgers := make(map[string][]uint32)
	for _, target := range BulkCopyTables {
		rows, err := dbConnectionPool.Query(ctx,
			fmt.Sprintf(`SELECT (%s >> 32)::bigint FROM %s ORDER BY 1`, target.TOIDColumn, target.Table))
		require.NoError(t, err)
		found, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		require.NoError(t, err)
		ledgers[target.Table] = []uint32{}
		for _, l := range found {
			ledgers[target.Table] = append(ledgers[target.Table], uint32(l))
		}
	}
	return ledgers
}

// Test_IngestStoreModel_DeleteRowsInLedgerRange seeds ledgers around a backfill window and
// verifies that only rows inside both the window's ledger range and its close-time range are
// deleted from the five bulk-COPY tables, and that transaction_hashes is left alone.
func Test_IngestStoreModel_DeleteRowsInLedgerRange(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &IngestStoreModel{
		DB:      dbConnectionPool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	rangeStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := rangeStart.Add(time.Hour)
	const firstLedger, lastLedger = uint32(100), uint32(102)

	seedBulkCopyLedger(ctx, t, dbConnectionPool, 99, "2026-01-01T00:00:10Z")  // below the ledger range, inside the time range
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 100, "2026-01-01T00:00:20Z") // inside both
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 101, "2026-01-01T00:00:25Z") // inside both
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 102, "2026-01-01T02:00:00Z") // inside the ledger range, outside the time range
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 103, "2026-01-01T00:00:30Z") // above the ledger range, inside the time range

	for table, ledgers := range bulkCopyLedgersPerTable(ctx, t, dbConnectionPool) {
		require.Equal(t, []uint32{99, 100, 101, 102, 103}, ledgers, "seed should place one row per ledger in %s", table)
	}
	countHashes := func(t *testing.T) int {
		n, countErr := db.QueryOne[int](ctx, dbConnectionPool, `SELECT count(*) FROM transaction_hashes`)
		require.NoError(t, countErr)
		return n
	}
	require.Equal(t, 5, countHashes(t))

	t.Run("rejects ranges that would silently delete nothing", func(t *testing.T) {
		for name, call := range map[string]func(tx pgx.Tx) error{
			"first after last": func(tx pgx.Tx) error {
				_, e := m.DeleteRowsInLedgerRange(ctx, tx, lastLedger, firstLedger, rangeStart, rangeEnd)
				return e
			},
			"ledger past the toid range": func(tx pgx.Tx) error {
				_, e := m.DeleteRowsInLedgerRange(ctx, tx, firstLedger, math.MaxInt32+1, rangeStart, rangeEnd)
				return e
			},
			"empty close-time range": func(tx pgx.Tx) error {
				_, e := m.DeleteRowsInLedgerRange(ctx, tx, firstLedger, lastLedger, rangeStart, rangeStart)
				return e
			},
		} {
			require.Error(t, db.RunInTransaction(ctx, dbConnectionPool, call), name)
		}
		for table, ledgers := range bulkCopyLedgersPerTable(ctx, t, dbConnectionPool) {
			assert.Equal(t, []uint32{99, 100, 101, 102, 103}, ledgers, "%s must be untouched by a rejected range", table)
		}
	})

	t.Run("deletes only rows inside both bounds", func(t *testing.T) {
		var deleted int64
		require.NoError(t, db.RunInTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
			var delErr error
			deleted, delErr = m.DeleteRowsInLedgerRange(ctx, tx, firstLedger, lastLedger, rangeStart, rangeEnd)
			return delErr
		}))

		// Ledgers 100 and 101 go from each of the five tables. Ledger 102 stays: its
		// TOIDs are in range but its close time is not, and a row needs both.
		assert.Equal(t, int64(2*len(BulkCopyTables)), deleted)
		for table, ledgers := range bulkCopyLedgersPerTable(ctx, t, dbConnectionPool) {
			assert.Equal(t, []uint32{99, 102, 103}, ledgers, "%s must keep only rows outside one of the bounds", table)
		}
		assert.Equal(t, 5, countHashes(t), "transaction_hashes must be untouched")

		// Idempotent: a second run over the same window finds nothing.
		require.NoError(t, db.RunInTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
			var delErr error
			deleted, delErr = m.DeleteRowsInLedgerRange(ctx, tx, firstLedger, lastLedger, rangeStart, rangeEnd)
			return delErr
		}))
		assert.Zero(t, deleted)
	})
}

// Test_IngestStoreModel_DeleteRowsInLedgerRange_CompressedChunks verifies the delete removes
// the window's rows when every chunk of the five tables is already compressed.
func Test_IngestStoreModel_DeleteRowsInLedgerRange_CompressedChunks(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &IngestStoreModel{
		DB:      dbConnectionPool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	seedBulkCopyLedger(ctx, t, dbConnectionPool, 100, "2026-01-01T00:00:10Z")
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 101, "2026-01-01T00:00:15Z")
	seedBulkCopyLedger(ctx, t, dbConnectionPool, 102, "2026-01-01T00:00:20Z")

	for _, table := range BulkCopyTableNames() {
		_, err = dbConnectionPool.Exec(ctx, fmt.Sprintf(`SELECT compress_chunk(c) FROM show_chunks('%s') c`, table))
		require.NoError(t, err)
	}
	uncompressed, err := db.QueryOne[int](ctx, dbConnectionPool,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = ANY($1) AND NOT is_compressed`,
		BulkCopyTableNames())
	require.NoError(t, err)
	require.Zero(t, uncompressed, "every chunk of the five tables is compressed")

	var deleted int64
	require.NoError(t, db.RunInTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
		var delErr error
		deleted, delErr = m.DeleteRowsInLedgerRange(ctx, tx, 100, 101,
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
		return delErr
	}))

	assert.Equal(t, int64(2*len(BulkCopyTables)), deleted)
	for table, ledgers := range bulkCopyLedgersPerTable(ctx, t, dbConnectionPool) {
		assert.Equal(t, []uint32{102}, ledgers, "%s must lose the window's rows from its compressed chunk", table)
	}
}
