// Package ingest - configureHypertableSettings applies TimescaleDB chunk interval
// and retention policy settings to hypertables at startup.
package ingest

import (
	"context"
	"fmt"

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
// future chunks. Retention policy is idempotent: any existing policy is removed
// before re-adding. When retention is enabled, a reconciliation job keeps
// oldest_ingest_ledger in sync with the actual minimum ledger remaining after
// chunk drops. Compression schedule interval updates how frequently existing
// compression policy jobs run (does not create new policies).
// Compress after updates how long after a chunk closes before it becomes
// eligible for compression.
func configureHypertableSettings(ctx context.Context, pool *pgxpool.Pool, chunkInterval, retentionPeriod, oldestCursorName, compressionScheduleInterval, compressAfter string) error {
	for _, table := range hypertables {
		if _, err := pool.Exec(ctx,
			"SELECT set_chunk_time_interval($1::regclass, $2::interval)",
			table, chunkInterval,
		); err != nil {
			return fmt.Errorf("setting chunk interval on %s: %w", table, err)
		}
		log.Ctx(ctx).Infof("Set chunk interval %q on %s", chunkInterval, table)
	}

	// We first remove existing retention policy
	for _, table := range hypertables {
		if _, err := pool.Exec(ctx,
			"SELECT remove_retention_policy($1::regclass, if_exists => true)",
			table,
		); err != nil {
			return fmt.Errorf("removing retention policy on %s: %w", table, err)
		}
	}
	// Reconciliation job: keeps oldestCursorName in sync after retention drops chunks.
	// Remove any existing job first (idempotent re-registration on every restart).
	if _, err := pool.Exec(ctx,
		"SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'reconcile_oldest_cursor'",
	); err != nil {
		return fmt.Errorf("removing existing reconciliation job: %w", err)
	}
	if retentionPeriod != "" {
		// Add new retention period policy
		for _, table := range hypertables {
			if _, err := pool.Exec(ctx,
				"SELECT add_retention_policy($1::regclass, drop_after => $2::interval)",
				table, retentionPeriod,
			); err != nil {
				return fmt.Errorf("adding retention policy on %s: %w", table, err)
			}
			log.Ctx(ctx).Infof("Set retention policy %q on %s", retentionPeriod, table)
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

		// Schedule the reconciliation job to run every 1 hour.
		//
		// The job checks whether retention has dropped chunks and advances the
		// oldest ledger cursor if so. It is idempotent — a no-op when the cursor
		// is already correct — and the query is microsecond-cheap (reads oldest
		// chunk metadata + 1 row from ingest_store). Running on a fixed 1-hour
		// interval keeps the cursor at most 1 hour stale after retention fires,
		// with no coordination required with the retention job schedule.
		if _, err := pool.Exec(ctx, `
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

	return nil
}
