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

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify chunk interval was updated for all hypertables
		for _, table := range hypertables {
			var intervalSecs float64
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT EXTRACT(EPOCH FROM d.time_interval)
				 FROM timescaledb_information.dimensions d
				 WHERE d.hypertable_name = $1 AND d.column_name = 'ledger_created_at'`,
				table,
			).Scan(&intervalSecs)
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify retention policy was created for all hypertables
		for _, table := range hypertables {
			var count int
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT COUNT(*)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_retention'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&count)
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify no retention policies were created
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'policy_retention'`,
		).Scan(&count)
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
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "90 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify exactly 1 retention policy per table (not duplicated)
		for _, table := range hypertables {
			var count int
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT COUNT(*)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_retention'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&count)
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify reconciliation job was created
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		).Scan(&count)
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
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "90 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify exactly 1 reconciliation job (not duplicated)
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		).Scan(&count)
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify no reconciliation job was created
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		).Scan(&count)
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "not-an-interval", "", "oldest_ledger_cursor", "", "")
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

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "not-an-interval", "oldest_ledger_cursor", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "adding retention policy")
	})

	t.Run("compression_schedule_interval", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Compression policies already exist from columnstore hypertable creation.
		// Configure with a 4-hour compression schedule interval.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "4 hours", "")
		require.NoError(t, err)

		// Verify schedule_interval was updated for all compression policy jobs
		for _, table := range hypertables {
			var intervalSecs float64
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&intervalSecs)
			require.NoError(t, err, "querying compression schedule for %s", table)
			// 4 hours in seconds = 4 * 60 * 60
			assert.Equal(t, float64(4*60*60), intervalSecs, "compression schedule interval for %s", table)
		}
	})

	t.Run("compress_after", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Compression policies already exist from columnstore hypertable creation.
		// Configure with a 12-hour compress_after value.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "", "12 hours")
		require.NoError(t, err)

		// Verify compress_after was updated in the config JSONB for all compression policy jobs
		for _, table := range hypertables {
			var compressAfter string
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&compressAfter)
			require.NoError(t, err, "querying compress_after for %s", table)
			assert.Equal(t, "12 hours", compressAfter, "compress_after for %s", table)
		}
	})

	t.Run("no_compress_after_when_empty", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Compression policies already exist from columnstore hypertable creation.
		// Record the default compress_after value before calling configureHypertableSettings.
		defaultValues := make(map[string]string)
		for _, table := range hypertables {
			var compressAfter string
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&compressAfter)
			require.NoError(t, err, "querying default compress_after for %s", table)
			defaultValues[table] = compressAfter
		}

		// Configure with empty compress_after (should skip)
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify compress_after was NOT changed
		for _, table := range hypertables {
			var compressAfter string
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&compressAfter)
			require.NoError(t, err, "querying compress_after for %s", table)
			assert.Equal(t, defaultValues[table], compressAfter, "compress_after should remain unchanged for %s", table)
		}
	})

	t.Run("no_compression_schedule_when_empty", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		// Compression policies already exist from columnstore hypertable creation.
		// Record the default schedule interval before calling configureHypertableSettings.
		defaultIntervals := make(map[string]float64)
		for _, table := range hypertables {
			var intervalSecs float64
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&intervalSecs)
			require.NoError(t, err, "querying default compression schedule for %s", table)
			defaultIntervals[table] = intervalSecs
		}

		// Configure with empty compression schedule interval (should skip)
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Verify schedule_interval was NOT changed
		for _, table := range hypertables {
			var intervalSecs float64
			err := dbConnectionPool.Pool().QueryRow(ctx,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&intervalSecs)
			require.NoError(t, err, "querying compression schedule for %s", table)
			assert.Equal(t, defaultIntervals[table], intervalSecs, "compression schedule interval should remain unchanged for %s", table)
		}
	})

	t.Run("reconciliation_job_scheduled_after_retention", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		ctx := context.Background()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ledger_cursor", "", "")
		require.NoError(t, err)

		// Reconciliation runs every 1 hour, independent of the retention schedule.
		var reconScheduleSecs float64
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
			 FROM timescaledb_information.jobs j
			 WHERE j.proc_name = 'reconcile_oldest_cursor'`).Scan(&reconScheduleSecs)
		require.NoError(t, err)

		var reconFixedSchedule bool
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT j.fixed_schedule
			 FROM timescaledb_information.jobs j
			 WHERE j.proc_name = 'reconcile_oldest_cursor'`).Scan(&reconFixedSchedule)
		require.NoError(t, err)

		assert.Equal(t, float64(3600), reconScheduleSecs,
			"reconciliation schedule_interval should be 1 hour (3600s)")
		assert.True(t, reconFixedSchedule,
			"reconciliation job should use fixed_schedule")
	})
}
