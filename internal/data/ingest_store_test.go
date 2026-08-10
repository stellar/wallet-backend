package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
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
	seedLedger := func(t *testing.T, ledger uint32, closedAt string) {
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
	}
	seedLedger(t, cursorLedger, "2026-01-01T00:00:00Z")
	seedLedger(t, cursorLedger+1, "2026-01-01T00:00:05Z")

	countRows := func(t *testing.T) map[string][2]int {
		counts := make(map[string][2]int)
		for table, column := range map[string]string{
			"transactions":          "to_id",
			"transactions_accounts": "tx_to_id",
			"operations":            "id",
			"operations_accounts":   "operation_id",
			"state_changes":         "to_id",
		} {
			var atCursor, aboveCursor int
			boundary := toid.New(int32(cursorLedger+1), 0, 0).ToInt64()
			require.NoError(t, dbConnectionPool.QueryRow(ctx,
				fmt.Sprintf(`SELECT count(*) FILTER (WHERE %[1]s < $1), count(*) FILTER (WHERE %[1]s >= $1) FROM %[2]s`, column, table),
				boundary).Scan(&atCursor, &aboveCursor))
			counts[table] = [2]int{atCursor, aboveCursor}
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

	// Idempotent: nothing above the cursor remains, so a second run changes nothing.
	require.NoError(t, m.DeleteRowsAboveLedger(ctx, cursorLedger))
	for table, c := range countRows(t) {
		assert.Equal(t, [2]int{1, 0}, c, "%s must be unchanged by a reconciliation no-op", table)
	}
}
