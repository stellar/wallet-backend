package data

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func Test_IngestStoreModel_GetLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "ingest_store_key", 123)
				require.NoError(t, err)
			},
			expectedLedger: 123,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "Get", "ingest_store", mock.Anything).Return().
				On("IncDBQuery", "Get", "ingest_store").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
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

func Test_IngestStoreModel_UpdateLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
				_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, "ingest_store_key", 123)
				require.NoError(t, err)
			},
			ledgerToUpsert: 456,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "Update", "ingest_store", mock.Anything).Return().Once().
				On("IncDBQuery", "Update", "ingest_store").Return().Once()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			err = db.RunInPgxTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				if err := m.Update(ctx, dbTx, tc.key, tc.ledgerToUpsert); err != nil {
					return err
				}
				return nil
			})
			require.NoError(t, err)

			var dbStoredLedger uint32
			err = m.DB.GetContext(ctx, &dbStoredLedger, `SELECT value FROM ingest_store WHERE key = $1`, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.ledgerToUpsert, dbStoredLedger)
		})
	}
}

func Test_IngestStoreModel_UpdateMin(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name           string
		key            string
		initialValue   uint32
		newValue       uint32
		expectedResult uint32
	}{
		{
			name:           "updates_to_smaller_value",
			key:            "oldest_ledger_cursor",
			initialValue:   1000,
			newValue:       500,
			expectedResult: 500,
		},
		{
			name:           "keeps_existing_smaller_value",
			key:            "oldest_ledger_cursor",
			initialValue:   500,
			newValue:       1000,
			expectedResult: 500,
		},
		{
			name:           "keeps_same_value",
			key:            "oldest_ledger_cursor",
			initialValue:   500,
			newValue:       500,
			expectedResult: 500,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			// Insert initial value
			_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, tc.key, tc.initialValue)
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			err = db.RunInPgxTransaction(ctx, m.DB, func(dbTx pgx.Tx) error {
				return m.UpdateMin(ctx, dbTx, tc.key, tc.newValue)
			})
			require.NoError(t, err)

			var dbStoredLedger uint32
			err = m.DB.GetContext(ctx, &dbStoredLedger, `SELECT value FROM ingest_store WHERE key = $1`, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedResult, dbStoredLedger)
		})
	}
}

func Test_IngestStoreModel_GetLedgerGaps(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testCases := []struct {
		name         string
		setupDB      func(t *testing.T)
		expectedGaps []LedgerRange
	}{
		{
			name:         "returns_empty_when_no_transactions",
			expectedGaps: nil,
		},
		{
			name: "returns_empty_when_no_gaps",
			setupDB: func(t *testing.T) {
				// Insert consecutive ledgers: 100, 101, 102
				for i, ledger := range []uint32{100, 101, 102} {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: nil,
		},
		{
			name: "returns_single_gap",
			setupDB: func(t *testing.T) {
				// Insert ledgers 100 and 105, creating gap 101-104
				for i, ledger := range []uint32{100, 105} {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 104},
			},
		},
		{
			name: "returns_multiple_gaps",
			setupDB: func(t *testing.T) {
				// Insert ledgers 100, 105, 110, creating gaps 101-104 and 106-109
				for i, ledger := range []uint32{100, 105, 110} {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
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
			name: "handles_single_ledger_gap",
			setupDB: func(t *testing.T) {
				// Insert ledgers 100 and 102, creating gap of just 101
				for i, ledger := range []uint32{100, 102} {
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						fmt.Sprintf("hash%d", i), i+1, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []LedgerRange{
				{GapStart: 101, GapEnd: 101},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM transactions")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "GetLedgerGaps", "transactions", mock.Anything).Return().
				On("IncDBQuery", "GetLedgerGaps", "transactions").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			gaps, err := m.GetLedgerGaps(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func Test_IngestStoreModel_GetOldestLedger(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

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
					_, err := dbConnectionPool.ExecContext(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW() - INTERVAL '1 day' * $4)`,
						fmt.Sprintf("hash%d", i), i+1, ledger, 300-int(ledger))
					require.NoError(t, err)
				}
			},
			expectedLedger: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM transactions")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "GetOldestLedger", "transactions", mock.Anything).Return().
				On("IncDBQuery", "GetOldestLedger", "transactions").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
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
