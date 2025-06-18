package data

import (
	"context"
	"testing"

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
				On("ObserveDBQueryDuration", "SELECT", "ingest_store", mock.Anything).Return().
				On("IncDBQuery", "SELECT", "ingest_store").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}
			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			lastSyncedLedger, err := m.GetLatestLedgerSynced(ctx, tc.key)
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
				On("ObserveDBQueryDuration", "INSERT", "ingest_store", mock.Anything).Return().Once().
				On("IncDBQuery", "INSERT", "ingest_store").Return().Once()
			defer mockMetricsService.AssertExpectations(t)

			m := &IngestStoreModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			err = m.UpdateLatestLedgerSynced(ctx, tc.key, tc.ledgerToUpsert)
			require.NoError(t, err)

			var dbStoredLedger uint32
			err = m.DB.GetContext(ctx, &dbStoredLedger, `SELECT value FROM ingest_store WHERE key = $1`, tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.ledgerToUpsert, dbStoredLedger)
		})
	}
}
