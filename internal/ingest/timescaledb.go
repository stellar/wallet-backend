// Package ingest - configureHypertableSettings applies TimescaleDB chunk interval
// and retention policy settings to hypertables at startup.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// hypertables lists all TimescaleDB hypertables managed by the ingestion system.
var hypertables = []string{
	"transactions",
	"transactions_accounts",
	"operations",
	"operations_accounts",
	"state_changes",
}

// configureHypertableSettings applies chunk interval, retention policy, and
// compression schedule settings to all hypertables. Chunk interval only affects
// future chunks. Retention policy and the reconciliation job are converged
// in place: an existing job is altered when its config differs from the
// desired value and left untouched when it already matches, so job_id and
// run history survive repeated calls (e.g. every process restart). When
// retention is enabled, the reconciliation job keeps oldest_ingest_ledger in
// sync with the actual minimum ledger remaining after chunk drops. Compression
// schedule interval updates how frequently existing compression policy jobs
// run (does not create new policies). Compress after updates how long after a
// chunk closes before it becomes eligible for compression.
func configureHypertableSettings(ctx context.Context, pool *pgxpool.Pool, chunkInterval, retentionPeriod, oldestCursorName, compressionScheduleInterval, compressAfter string, maxChunksToCompress int) error {
	for _, table := range hypertables {
		if _, err := pool.Exec(ctx,
			"SELECT set_chunk_time_interval($1::regclass, $2::interval)",
			table, chunkInterval,
		); err != nil {
			return fmt.Errorf("setting chunk interval on %s: %w", table, err)
		}
		log.Ctx(ctx).Infof("Set chunk interval %q on %s", chunkInterval, table)
	}

	for _, table := range hypertables {
		if err := configureRetentionPolicy(ctx, pool, table, retentionPeriod); err != nil {
			return fmt.Errorf("configuring retention policy on %s: %w", table, err)
		}
	}

	if err := configureReconciliationJob(ctx, pool, retentionPeriod, oldestCursorName); err != nil {
		return fmt.Errorf("configuring reconciliation job: %w", err)
	}

	if compressionScheduleInterval != "" {
		for _, table := range hypertables {
			var jobID int
			err := pool.QueryRow(ctx,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_compression'
				   AND hypertable_name = $1`,
				table,
			).Scan(&jobID)
			if err != nil {
				log.Ctx(ctx).Warnf("No compression policy found for %s, skipping schedule update", table)
				continue
			}

			if _, err := pool.Exec(ctx,
				"SELECT alter_job($1, schedule_interval => $2::interval)",
				jobID, compressionScheduleInterval,
			); err != nil {
				return fmt.Errorf("updating compression schedule interval on %s (job %d): %w", table, jobID, err)
			}
			log.Ctx(ctx).Infof("Set compression schedule interval %q on %s (job %d)", compressionScheduleInterval, table, jobID)
		}
	}

	if compressAfter != "" {
		for _, table := range hypertables {
			var jobID int
			err := pool.QueryRow(ctx,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_compression'
				   AND hypertable_name = $1`,
				table,
			).Scan(&jobID)
			if err != nil {
				log.Ctx(ctx).Warnf("No compression policy found for %s, skipping compress_after update", table)
				continue
			}

			if _, err := pool.Exec(ctx,
				`SELECT alter_job($1, config => jsonb_set(
					(SELECT config FROM timescaledb_information.jobs WHERE job_id = $1),
					'{compress_after}', to_jsonb($2::text)))`,
				jobID, compressAfter,
			); err != nil {
				return fmt.Errorf("updating compress_after on %s (job %d): %w", table, jobID, err)
			}
			log.Ctx(ctx).Infof("Set compress_after %q on %s (job %d)", compressAfter, table, jobID)
		}
	}

	if maxChunksToCompress > 0 {
		for _, table := range hypertables {
			var jobID int
			err := pool.QueryRow(ctx,
				`SELECT job_id FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_compression'
				   AND hypertable_name = $1`,
				table,
			).Scan(&jobID)
			if err != nil {
				log.Ctx(ctx).Warnf("No compression policy found for %s, skipping maxchunks_to_compress update", table)
				continue
			}

			if _, err := pool.Exec(ctx,
				`SELECT alter_job($1, config => config || jsonb_build_object('maxchunks_to_compress', $2::int))
				 FROM timescaledb_information.jobs WHERE job_id = $1`,
				jobID, maxChunksToCompress,
			); err != nil {
				return fmt.Errorf("updating maxchunks_to_compress on %s (job %d): %w", table, jobID, err)
			}
			log.Ctx(ctx).Infof("Set maxchunks_to_compress %d on %s (job %d)", maxChunksToCompress, table, jobID)
		}
	}

	return nil
}

// configureRetentionPolicy converges table's retention policy on the desired
// drop_after. An empty retentionPeriod disables retention, removing any
// existing policy. Otherwise, a missing policy is added; an existing one
// whose drop_after differs is altered in place (job_id and run history
// survive); one that already matches is left untouched.
func configureRetentionPolicy(ctx context.Context, pool *pgxpool.Pool, table, retentionPeriod string) error {
	if retentionPeriod == "" {
		if _, err := pool.Exec(ctx,
			"SELECT remove_retention_policy($1::regclass, if_exists => true)",
			table,
		); err != nil {
			return fmt.Errorf("removing retention policy: %w", err)
		}
		return nil
	}

	var jobID int
	err := pool.QueryRow(ctx,
		`SELECT job_id FROM timescaledb_information.jobs
		 WHERE proc_name = 'policy_retention' AND hypertable_name = $1`,
		table,
	).Scan(&jobID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err = pool.Exec(ctx,
			"SELECT add_retention_policy($1::regclass, drop_after => $2::interval)",
			table, retentionPeriod,
		); err != nil {
			return fmt.Errorf("adding retention policy: %w", err)
		}
		log.Ctx(ctx).Infof("Added retention policy %q on %s", retentionPeriod, table)
		return nil
	case err != nil:
		return fmt.Errorf("looking up existing retention policy: %w", err)
	}

	var matches bool
	if err := pool.QueryRow(ctx,
		`SELECT (config->>'drop_after')::interval = $2::interval
		 FROM timescaledb_information.jobs WHERE job_id = $1`,
		jobID, retentionPeriod,
	).Scan(&matches); err != nil {
		return fmt.Errorf("comparing existing retention policy (job %d): %w", jobID, err)
	}
	if matches {
		return nil
	}

	if _, err := pool.Exec(ctx,
		`SELECT alter_job($1, config => jsonb_set(
			(SELECT config FROM timescaledb_information.jobs WHERE job_id = $1),
			'{drop_after}', to_jsonb($2::text)))`,
		jobID, retentionPeriod,
	); err != nil {
		return fmt.Errorf("altering retention policy (job %d): %w", jobID, err)
	}
	log.Ctx(ctx).Infof("Updated retention policy to %q on %s (job %d)", retentionPeriod, table, jobID)
	return nil
}

// configureReconciliationJob converges the reconcile_oldest_cursor job, which
// keeps oldestCursorName in sync with the actual minimum ledger remaining
// after retention drops chunks. An empty retentionPeriod removes the job
// entirely — it has nothing to reconcile when retention is disabled.
// Otherwise, a missing job is added; an existing one whose cursor_name
// differs is altered in place (job_id and run history survive); one that
// already matches is left untouched.
func configureReconciliationJob(ctx context.Context, pool *pgxpool.Pool, retentionPeriod, oldestCursorName string) error {
	if retentionPeriod == "" {
		if _, err := pool.Exec(ctx,
			"SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'",
		); err != nil {
			return fmt.Errorf("removing reconciliation job: %w", err)
		}
		return nil
	}

	// Create or replace the PL/pgSQL function that advances the cursor.
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION reconcile_oldest_cursor(job_id INT, config JSONB)
		RETURNS VOID LANGUAGE plpgsql AS $$
		DECLARE
			actual_min INTEGER;
			stored    INTEGER;
		BEGIN
			SELECT ledger_number INTO actual_min FROM transactions
				ORDER BY ledger_created_at ASC, to_id ASC LIMIT 1;
			IF actual_min IS NULL THEN RETURN; END IF;
			SELECT value::integer INTO stored FROM ingest_store WHERE key = config->>'cursor_name';
			IF stored IS NULL OR actual_min <= stored THEN RETURN; END IF;
			UPDATE ingest_store SET value = actual_min::text WHERE key = config->>'cursor_name';
			RAISE LOG 'reconcile_oldest_cursor: advanced % from % to %', config->>'cursor_name', stored, actual_min;
		END $$;
	`); err != nil {
		return fmt.Errorf("creating reconcile_oldest_cursor function: %w", err)
	}

	var jobID int
	err := pool.QueryRow(ctx,
		"SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'",
	).Scan(&jobID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Runs every 1 hour: cheap enough (oldest chunk metadata + 1 row from
		// ingest_store) to not need coordination with the retention job's own
		// schedule, and idempotent — a no-op once the cursor is already correct.
		if _, err = pool.Exec(ctx, `
			SELECT add_job(
				'reconcile_oldest_cursor',
				'1 hour',
				fixed_schedule => true,
				config => $1::jsonb)`,
			fmt.Sprintf(`{"cursor_name":"%s"}`, oldestCursorName),
		); err != nil {
			return fmt.Errorf("scheduling reconciliation job: %w", err)
		}
		log.Ctx(ctx).Infof("Scheduled reconcile_oldest_cursor job (1h fixed interval) for cursor %q", oldestCursorName)
		return nil
	case err != nil:
		return fmt.Errorf("looking up existing reconciliation job: %w", err)
	}

	var matches bool
	if err := pool.QueryRow(ctx,
		`SELECT config->>'cursor_name' = $2 FROM timescaledb_information.jobs WHERE job_id = $1`,
		jobID, oldestCursorName,
	).Scan(&matches); err != nil {
		return fmt.Errorf("comparing existing reconciliation job (job %d): %w", jobID, err)
	}
	if matches {
		return nil
	}

	if _, err := pool.Exec(ctx,
		"SELECT alter_job($1, config => jsonb_build_object('cursor_name', $2::text))",
		jobID, oldestCursorName,
	); err != nil {
		return fmt.Errorf("altering reconciliation job (job %d): %w", jobID, err)
	}
	log.Ctx(ctx).Infof("Updated reconcile_oldest_cursor cursor to %q (job %d)", oldestCursorName, jobID)
	return nil
}
