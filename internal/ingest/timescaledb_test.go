// Package ingest - tests for configureHypertableSettings verifying chunk interval
// and retention policy configuration against a real TimescaleDB instance.
package ingest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func TestConfigureHypertableSettings(t *testing.T) {
	t.Run("chunk_interval", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify chunk interval was updated for all hypertables
		for _, table := range hypertables {
			var intervalSecs float64
			err := dbConnectionPool.GetContext(ctx, &intervalSecs,
				`SELECT EXTRACT(EPOCH FROM d.time_interval)
				 FROM timescaledb_information.dimensions d
				 WHERE d.hypertable_name = $1 AND d.column_name = 'ledger_created_at'`,
				table,
			)
			require.NoError(t, err, "querying dimensions for %s", table)
			// 7 days in seconds = 7 * 24 * 60 * 60
			assert.Equal(t, float64(7*24*60*60), intervalSecs, "chunk interval for %s", table)
		}
	})

	t.Run("retention_policy", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify retention policy was created for all hypertables
		for _, table := range hypertables {
			var count int
			err := dbConnectionPool.GetContext(ctx, &count,
				`SELECT COUNT(*)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_retention'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention policy for %s", table)
			assert.Equal(t, 1, count, "expected exactly 1 retention policy for %s", table)
		}
	})

	t.Run("no_retention_when_empty", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify no retention policies were created
		var count int
		err = dbConnectionPool.GetContext(ctx, &count,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'policy_retention'`,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "expected no retention policies when retention period is empty")
	})

	t.Run("retention_policy_idempotent", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Apply retention policy twice with different values to simulate restarts
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "90 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify exactly 1 retention policy per table (not duplicated)
		for _, table := range hypertables {
			var count int
			err := dbConnectionPool.GetContext(ctx, &count,
				`SELECT COUNT(*)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_retention'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention policy for %s", table)
			assert.Equal(t, 1, count, "expected exactly 1 retention policy for %s after re-application", table)
		}
	})

	t.Run("reconciliation_job_created", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify reconciliation job was created
		var count int
		err = dbConnectionPool.GetContext(ctx, &count,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "expected exactly 1 reconciliation job")
	})

	t.Run("reconciliation_job_idempotent", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Apply twice to simulate restarts
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "90 days", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify exactly 1 reconciliation job (not duplicated)
		var count int
		err = dbConnectionPool.GetContext(ctx, &count,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "expected exactly 1 reconciliation job after re-application")
	})

	t.Run("no_reconciliation_job_without_retention", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor")
		require.NoError(t, err)

		// Verify no reconciliation job was created
		var count int
		err = dbConnectionPool.GetContext(ctx, &count,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "expected no reconciliation job when retention is disabled")
	})

	t.Run("invalid_chunk_interval", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "not-an-interval", "", "oldest_ledger_cursor")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "setting chunk interval")
	})

	t.Run("invalid_retention_period", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "not-an-interval", "oldest_ledger_cursor")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "adding retention policy")
	})
}
