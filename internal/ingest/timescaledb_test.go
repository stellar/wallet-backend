// Package ingest - tests for configureHypertableSettings verifying chunk interval
// and retention policy configuration against a real TimescaleDB instance.
package ingest

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func TestConfigureHypertableSettings(t *testing.T) {
	t.Run("chunk_interval", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify chunk interval was updated for all hypertables
		for _, table := range hypertables {
			intervalSecs, err := db.QueryOne[float64](ctx, dbConnectionPool,
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
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify retention policy was created for all hypertables
		for _, table := range hypertables {
			count, err := db.QueryOne[int](ctx, dbConnectionPool,
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
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify no retention policies were created
		count, err := db.QueryOne[int](ctx, dbConnectionPool,
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
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Apply retention policy twice with different values to simulate restarts
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "90 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify exactly 1 retention policy per table (not duplicated)
		for _, table := range hypertables {
			count, err := db.QueryOne[int](ctx, dbConnectionPool,
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

	t.Run("retention_policy_job_id_stable_across_calls", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		jobIDsBefore := make(map[string]int)
		for _, table := range hypertables {
			jobID, err := db.QueryOne[int](ctx, dbConnectionPool,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_retention' AND hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention job_id for %s", table)
			jobIDsBefore[table] = jobID
		}

		// Re-applying the same retention period must not delete/recreate the job.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		for _, table := range hypertables {
			jobID, err := db.QueryOne[int](ctx, dbConnectionPool,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_retention' AND hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention job_id for %s", table)
			assert.Equal(t, jobIDsBefore[table], jobID, "retention job_id should be stable across identical re-application for %s", table)
		}
	})

	t.Run("retention_policy_altered_not_recreated", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		jobIDsBefore := make(map[string]int)
		for _, table := range hypertables {
			jobID, err := db.QueryOne[int](ctx, dbConnectionPool,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_retention' AND hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention job_id for %s", table)
			jobIDsBefore[table] = jobID
		}

		// Changing the retention period must alter the existing job in place,
		// not delete and recreate it.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "90 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		for _, table := range hypertables {
			jobID, err := db.QueryOne[int](ctx, dbConnectionPool,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_retention' AND hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying retention job_id for %s", table)
			assert.Equal(t, jobIDsBefore[table], jobID, "retention job_id should be stable when the period changes for %s", table)

			dropAfterMatches, err := db.QueryOne[bool](ctx, dbConnectionPool,
				`SELECT (config->>'drop_after')::interval = '90 days'::interval
				 FROM timescaledb_information.jobs WHERE job_id = $1`,
				jobID,
			)
			require.NoError(t, err, "querying drop_after for %s", table)
			assert.True(t, dropAfterMatches, "drop_after should be updated to 90 days for %s", table)
		}
	})

	t.Run("reconciliation_job_created", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify reconciliation job was created
		count, err := db.QueryOne[int](ctx, dbConnectionPool,
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
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Apply twice to simulate restarts
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		err = configureHypertableSettings(ctx, dbConnectionPool, "7 days", "90 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify exactly 1 reconciliation job (not duplicated)
		count, err := db.QueryOne[int](ctx, dbConnectionPool,
			`SELECT COUNT(*)
			 FROM timescaledb_information.jobs
			 WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "expected exactly 1 reconciliation job after re-application")
	})

	t.Run("reconciliation_job_id_stable_across_calls", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		jobIDBefore, err := db.QueryOne[int](ctx, dbConnectionPool,
			`SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)

		// Re-applying with a different retention period but the same cursor name
		// must not delete/recreate the reconciliation job.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "90 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		jobIDAfter, err := db.QueryOne[int](ctx, dbConnectionPool,
			`SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, jobIDBefore, jobIDAfter, "reconciliation job_id should be stable across re-application")
	})

	t.Run("reconciliation_job_altered_not_recreated_when_cursor_changes", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		jobIDBefore, err := db.QueryOne[int](ctx, dbConnectionPool,
			`SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)

		// A changed cursor name must alter the existing job's config in place,
		// not delete and recreate the job.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "renamed_cursor", "", "", 0)
		require.NoError(t, err)

		jobIDAfter, err := db.QueryOne[int](ctx, dbConnectionPool,
			`SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'`,
		)
		require.NoError(t, err)
		assert.Equal(t, jobIDBefore, jobIDAfter, "reconciliation job_id should be stable when the cursor name changes")

		cursorName, err := db.QueryOne[string](ctx, dbConnectionPool,
			`SELECT config->>'cursor_name' FROM timescaledb_information.jobs WHERE job_id = $1`,
			jobIDAfter,
		)
		require.NoError(t, err)
		assert.Equal(t, "renamed_cursor", cursorName, "cursor_name should be updated in the reconciliation job's config")
	})

	t.Run("no_reconciliation_job_without_retention", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify no reconciliation job was created
		count, err := db.QueryOne[int](ctx, dbConnectionPool,
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
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "not-an-interval", "", "oldest_ingest_ledger", "", "", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "setting chunk interval")
	})

	t.Run("invalid_retention_period", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "not-an-interval", "oldest_ingest_ledger", "", "", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "adding retention policy")
	})

	t.Run("compression_schedule_interval", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Compression policies already exist from columnstore hypertable creation.
		// Configure with a 4-hour compression schedule interval.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "4 hours", "", 0)
		require.NoError(t, err)

		// Verify schedule_interval was updated for all compression policy jobs
		for _, table := range hypertables {
			intervalSecs, err := db.QueryOne[float64](ctx, dbConnectionPool,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying compression schedule for %s", table)
			// 4 hours in seconds = 4 * 60 * 60
			assert.Equal(t, float64(4*60*60), intervalSecs, "compression schedule interval for %s", table)
		}
	})

	t.Run("compress_after", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Compression policies already exist from columnstore hypertable creation.
		// Configure with a 12-hour compress_after value.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "12 hours", 0)
		require.NoError(t, err)

		// Verify compress_after was updated in the config JSONB for all compression policy jobs
		for _, table := range hypertables {
			compressAfter, err := db.QueryOne[string](ctx, dbConnectionPool,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying compress_after for %s", table)
			assert.Equal(t, "12 hours", compressAfter, "compress_after for %s", table)
		}
	})

	t.Run("no_compress_after_when_empty", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Compression policies already exist from columnstore hypertable creation.
		// Record the default compress_after value before calling configureHypertableSettings.
		defaultValues := make(map[string]string)
		for _, table := range hypertables {
			compressAfter, err := db.QueryOne[string](ctx, dbConnectionPool,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying default compress_after for %s", table)
			defaultValues[table] = compressAfter
		}

		// Configure with empty compress_after (should skip)
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify compress_after was NOT changed
		for _, table := range hypertables {
			compressAfter, err := db.QueryOne[string](ctx, dbConnectionPool,
				`SELECT j.config->>'compress_after'
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying compress_after for %s", table)
			assert.Equal(t, defaultValues[table], compressAfter, "compress_after should remain unchanged for %s", table)
		}
	})

	t.Run("no_compression_schedule_when_empty", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Compression policies already exist from columnstore hypertable creation.
		// Record the default schedule interval before calling configureHypertableSettings.
		defaultIntervals := make(map[string]float64)
		for _, table := range hypertables {
			intervalSecs, err := db.QueryOne[float64](ctx, dbConnectionPool,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying default compression schedule for %s", table)
			defaultIntervals[table] = intervalSecs
		}

		// Configure with empty compression schedule interval (should skip)
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify schedule_interval was NOT changed
		for _, table := range hypertables {
			intervalSecs, err := db.QueryOne[float64](ctx, dbConnectionPool,
				`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			)
			require.NoError(t, err, "querying compression schedule for %s", table)
			assert.Equal(t, defaultIntervals[table], intervalSecs, "compression schedule interval should remain unchanged for %s", table)
		}
	})

	t.Run("reconciliation_job_scheduled_after_retention", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "30 days", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Reconciliation runs every 1 hour, independent of the retention schedule.
		reconScheduleSecs, err := db.QueryOne[float64](ctx, dbConnectionPool,
			`SELECT EXTRACT(EPOCH FROM j.schedule_interval)
			 FROM timescaledb_information.jobs j
			 WHERE j.proc_name = 'reconcile_oldest_cursor'`)
		require.NoError(t, err)

		reconFixedSchedule, err := db.QueryOne[bool](ctx, dbConnectionPool,
			`SELECT j.fixed_schedule
			 FROM timescaledb_information.jobs j
			 WHERE j.proc_name = 'reconcile_oldest_cursor'`)
		require.NoError(t, err)

		assert.Equal(t, float64(3600), reconScheduleSecs,
			"reconciliation schedule_interval should be 1 hour (3600s)")
		assert.True(t, reconFixedSchedule,
			"reconciliation job should use fixed_schedule")
	})

	t.Run("max_chunks_to_compress", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Compression policies already exist from columnstore hypertable creation.
		// Configure with maxChunksToCompress = 10.
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 10)
		require.NoError(t, err)

		// Verify maxchunks_to_compress was set in the config JSONB for all compression policy jobs
		for _, table := range hypertables {
			var maxChunks int
			err := dbConnectionPool.QueryRow(ctx,
				`SELECT (j.config->>'maxchunks_to_compress')::int
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&maxChunks)
			require.NoError(t, err, "querying maxchunks_to_compress for %s", table)
			assert.Equal(t, 10, maxChunks, "maxchunks_to_compress for %s", table)
		}
	})

	t.Run("no_max_chunks_when_zero", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		ctx := context.Background()
		dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Record default maxchunks_to_compress before calling configureHypertableSettings.
		defaultValues := make(map[string]*int)
		for _, table := range hypertables {
			var maxChunks *int
			err := dbConnectionPool.QueryRow(ctx,
				`SELECT (j.config->>'maxchunks_to_compress')::int
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&maxChunks)
			require.NoError(t, err, "querying default maxchunks_to_compress for %s", table)
			defaultValues[table] = maxChunks
		}

		// Configure with maxChunksToCompress = 0 (should skip)
		err = configureHypertableSettings(ctx, dbConnectionPool, "1 day", "", "oldest_ingest_ledger", "", "", 0)
		require.NoError(t, err)

		// Verify maxchunks_to_compress was NOT changed
		for _, table := range hypertables {
			var maxChunks *int
			err := dbConnectionPool.QueryRow(ctx,
				`SELECT (j.config->>'maxchunks_to_compress')::int
				 FROM timescaledb_information.jobs j
				 WHERE j.proc_name = 'policy_compression'
				   AND j.hypertable_name = $1`,
				table,
			).Scan(&maxChunks)
			require.NoError(t, err, "querying maxchunks_to_compress for %s", table)
			assert.Equal(t, defaultValues[table], maxChunks, "maxchunks_to_compress should remain unchanged for %s", table)
		}
	})
}

func TestConfigureTransactionHashesMaintenance(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	type maintenanceJob struct {
		ID           int
		ScheduleSecs float64
		Workers      int
		Worker       int
		Scheduled    bool
	}
	maintenanceJobs := func() []maintenanceJob {
		t.Helper()
		rows, err := pool.Query(ctx,
			`SELECT job_id, EXTRACT(EPOCH FROM schedule_interval)::float8, (config->>'workers')::int, (config->>'worker')::int, scheduled
			 FROM timescaledb_information.jobs WHERE proc_name = 'transaction_hashes_maintenance'
			 ORDER BY (config->>'worker')::int, job_id`)
		require.NoError(t, err)
		jobs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[maintenanceJob])
		require.NoError(t, err)
		return jobs
	}
	const day = float64(24 * 60 * 60)

	jobs := maintenanceJobs()
	require.Len(t, jobs, 1, "the migration creates one maintenance job")
	original := jobs[0]
	assert.Equal(t, maintenanceJob{ID: original.ID, ScheduleSecs: day, Workers: 1, Worker: 0, Scheduled: false}, original,
		"the migration leaves the job for the ingester to schedule")

	require.NoError(t, configureTransactionHashesMaintenance(ctx, pool, "1 day", 3))
	jobs = maintenanceJobs()
	require.Len(t, jobs, 3)
	for i, j := range jobs {
		assert.Equal(t, maintenanceJob{ID: j.ID, ScheduleSecs: day, Workers: 3, Worker: i, Scheduled: true}, j)
	}
	assert.Equal(t, original.ID, jobs[0].ID, "worker 0 keeps the migration's job")

	t.Run("a repeated call with the same settings keeps the jobs", func(t *testing.T) {
		require.NoError(t, configureTransactionHashesMaintenance(ctx, pool, "1 day", 3))
		assert.Equal(t, jobs, maintenanceJobs())
	})

	t.Run("fewer workers delete the extra jobs", func(t *testing.T) {
		require.NoError(t, configureTransactionHashesMaintenance(ctx, pool, "7 days", 1))
		assert.Equal(t, []maintenanceJob{{ID: original.ID, ScheduleSecs: 7 * day, Workers: 1, Worker: 0, Scheduled: true}}, maintenanceJobs())
	})

	t.Run("zero workers is an error", func(t *testing.T) {
		require.Error(t, configureTransactionHashesMaintenance(ctx, pool, "7 days", 0))
		assert.Len(t, maintenanceJobs(), 1)
	})

	t.Run("the bulk-table settings leave the lookup table alone", func(t *testing.T) {
		require.NoError(t, configureHypertableSettings(ctx, pool, "1 day", "30 days", "oldest_ingest_ledger", "1 hour", "2 hours", 5))
		assert.Equal(t, []maintenanceJob{{ID: original.ID, ScheduleSecs: 7 * day, Workers: 1, Worker: 0, Scheduled: true}}, maintenanceJobs())
		policies, err := db.QueryOne[int](ctx, pool,
			`SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_name = 'transaction_hashes'`)
		require.NoError(t, err)
		assert.Zero(t, policies, "no columnstore or retention policy on the lookup table")
	})
}

func TestTimescaleDBVersionAtLeast(t *testing.T) {
	testCases := []struct {
		name     string
		found    string
		required string
		want     bool
		wantErr  bool
	}{
		{name: "equal", found: "2.31.0", required: "2.31.0", want: true},
		{name: "newer_patch", found: "2.31.1", required: "2.31.0", want: true},
		{name: "newer_minor", found: "2.32.0", required: "2.31.0", want: true},
		{name: "newer_major", found: "3.0.0", required: "2.31.0", want: true},
		{name: "older_patch", found: "2.31.0", required: "2.31.1", want: false},
		{name: "older_minor", found: "2.28.2", required: "2.31.0", want: false},
		{name: "older_major", found: "1.99.99", required: "2.31.0", want: false},
		{name: "minor_compared_numerically", found: "2.9.0", required: "2.31.0", want: false},
		{name: "two_components", found: "2.31", required: "2.31.0", want: true},
		{name: "pre_release_suffix", found: "2.31.0-dev", required: "2.31.0", want: true},
		{name: "build_suffix", found: "2.31.0+build1", required: "2.31.0", want: true},
		{name: "single_component", found: "2", required: "2.31.0", wantErr: true},
		{name: "four_components", found: "2.31.0.1", required: "2.31.0", wantErr: true},
		{name: "non_numeric", found: "2.x.0", required: "2.31.0", wantErr: true},
		{name: "empty", found: "", required: "2.31.0", wantErr: true},
		{name: "malformed_required", found: "2.31.0", required: "latest", wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := timescaleDBVersionAtLeast(tc.found, tc.required)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRequireTimescaleDBVersion(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	installed, err := db.QueryOne[string](ctx, dbConnectionPool, timescaleDBVersionQuery)
	require.NoError(t, err)
	supported, err := timescaleDBVersionAtLeast(installed, minTimescaleDBVersion)
	require.NoError(t, err)

	err = requireTimescaleDBVersion(ctx, dbConnectionPool)
	if supported {
		require.NoError(t, err)
		return
	}
	require.EqualError(t, err, "timescaledb "+installed+" is installed, "+minTimescaleDBVersion+" or newer is required")
}
