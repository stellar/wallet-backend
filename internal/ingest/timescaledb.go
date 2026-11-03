// Package ingest - configureHypertableSettings applies TimescaleDB chunk interval
// and retention policy settings to hypertables at startup.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
)

// hypertables lists all TimescaleDB hypertables managed by the ingestion
// system: exactly the tables ingestion bulk-loads with COPY.
var hypertables = data.BulkCopyTableNames()

// minTimescaleDBVersion is the oldest TimescaleDB extension ingest runs
// against. Backfill attaches chunks it built offline with attach_chunk, and it
// relies on the compression behaviour of this release for those chunks.
const minTimescaleDBVersion = "2.31.0"

const timescaleDBVersionQuery = `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`

// requireTimescaleDBVersion fails unless the installed TimescaleDB extension
// is minTimescaleDBVersion or newer. Ingest checks this before touching any
// hypertable, so an older extension stops the process with a clear error
// instead of failing later in the middle of a backfill or a policy change.
func requireTimescaleDBVersion(ctx context.Context, pool *pgxpool.Pool) error {
	var found string
	err := pool.QueryRow(ctx, timescaleDBVersionQuery).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("timescaledb extension is not installed")
	}
	if err != nil {
		return fmt.Errorf("reading timescaledb extension version: %w", err)
	}

	ok, err := timescaleDBVersionAtLeast(found, minTimescaleDBVersion)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("timescaledb %s is installed, %s or newer is required", found, minTimescaleDBVersion)
	}
	log.Ctx(ctx).Infof("TimescaleDB %s", found)
	return nil
}

// timescaleDBVersionAtLeast reports whether version found is the same as or
// newer than required. Both are "MAJOR.MINOR[.PATCH]" with an optional
// pre-release or build suffix after '-' or '+', which is ignored: extversion
// carries such suffixes on development builds, and the numeric release is what
// decides the available features.
func timescaleDBVersionAtLeast(found, required string) (bool, error) {
	f, err := parseTimescaleDBVersion(found)
	if err != nil {
		return false, err
	}
	r, err := parseTimescaleDBVersion(required)
	if err != nil {
		return false, err
	}
	for i := range f {
		if f[i] != r[i] {
			return f[i] > r[i], nil
		}
	}
	return true, nil
}

// parseTimescaleDBVersion splits a "MAJOR.MINOR[.PATCH][-suffix|+suffix]"
// version into its numeric parts. A missing PATCH reads as 0.
func parseTimescaleDBVersion(version string) ([3]int, error) {
	var parts [3]int
	numeric := version
	if i := strings.IndexAny(numeric, "-+"); i >= 0 {
		numeric = numeric[:i]
	}
	fields := strings.Split(numeric, ".")
	if len(fields) < 2 || len(fields) > 3 {
		return parts, fmt.Errorf("parsing timescaledb version %q: want MAJOR.MINOR[.PATCH]", version)
	}
	for i, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil {
			return parts, fmt.Errorf("parsing timescaledb version %q: invalid component %q", version, field)
		}
		parts[i] = n
	}
	return parts, nil
}

// scheduleCompressionPolicies turns scheduling back on for every columnstore
// policy that is paused. A backfill pauses the policies for its run, the
// lookup table's maintenance jobs with them, and resumes them when it exits;
// a backfill killed before that point leaves them paused, so live mode
// schedules them again at every start: the policies here, the maintenance
// jobs in configureTransactionHashesMaintenance.
func scheduleCompressionPolicies(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx,
		`SELECT job_id, hypertable_name FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND NOT scheduled`)
	if err != nil {
		return fmt.Errorf("listing paused compression policies: %w", err)
	}
	type pausedJob struct {
		id    int
		table string
	}
	paused, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (pausedJob, error) {
		var j pausedJob
		err := row.Scan(&j.id, &j.table)
		return j, err //nolint:wrapcheck // CollectRows wraps
	})
	if err != nil {
		return fmt.Errorf("collecting paused compression policies: %w", err)
	}
	for _, job := range paused {
		if _, err := pool.Exec(ctx, `SELECT alter_job($1, scheduled => true)`, job.id); err != nil {
			return fmt.Errorf("scheduling compression policy %d of %s: %w", job.id, job.table, err)
		}
		log.Ctx(ctx).Infof("Scheduled the compression policy of %s again (job %d); a backfill had paused it", job.table, job.id)
	}
	return nil
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
		if err := setChunkInterval(ctx, pool, table, chunkInterval); err != nil {
			return err
		}
	}

	for _, table := range hypertables {
		if err := configureRetentionPolicy(ctx, pool, table, retentionPeriod); err != nil {
			return fmt.Errorf("configuring retention policy on %s: %w", table, err)
		}
	}

	if err := configureReconciliationJob(ctx, pool, retentionPeriod, oldestCursorName); err != nil {
		return fmt.Errorf("configuring reconciliation job: %w", err)
	}

	for _, table := range hypertables {
		if err := configureCompressionPolicy(ctx, pool, table, compressionScheduleInterval, compressAfter, maxChunksToCompress); err != nil {
			return err
		}
	}

	return nil
}

func setChunkInterval(ctx context.Context, pool *pgxpool.Pool, table, chunkInterval string) error {
	if _, err := pool.Exec(ctx,
		"SELECT set_chunk_time_interval($1::regclass, $2::interval)",
		table, chunkInterval,
	); err != nil {
		return fmt.Errorf("setting chunk interval on %s: %w", table, err)
	}
	log.Ctx(ctx).Infof("Set chunk interval %q on %s", chunkInterval, table)
	return nil
}

// configureCompressionPolicy tunes table's existing compression policy: each
// non-empty setting is applied, an empty one leaves the job as it is. A table
// without a policy is skipped with a warning.
func configureCompressionPolicy(ctx context.Context, pool *pgxpool.Pool, table, scheduleInterval, compressAfter string, maxChunksToCompress int) error {
	if scheduleInterval == "" && compressAfter == "" && maxChunksToCompress <= 0 {
		return nil
	}

	var jobID int
	err := pool.QueryRow(ctx,
		`SELECT job_id FROM timescaledb_information.jobs
		 WHERE proc_name = 'policy_compression' AND hypertable_name = $1`,
		table,
	).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Ctx(ctx).Warnf("No compression policy found for %s, skipping compression settings", table)
		return nil
	}
	if err != nil {
		return fmt.Errorf("looking up compression policy on %s: %w", table, err)
	}

	if scheduleInterval != "" {
		if _, err := pool.Exec(ctx,
			"SELECT alter_job($1, schedule_interval => $2::interval)",
			jobID, scheduleInterval,
		); err != nil {
			return fmt.Errorf("updating compression schedule interval on %s (job %d): %w", table, jobID, err)
		}
		log.Ctx(ctx).Infof("Set compression schedule interval %q on %s (job %d)", scheduleInterval, table, jobID)
	}

	if compressAfter != "" {
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

	if maxChunksToCompress > 0 {
		if _, err := pool.Exec(ctx,
			`SELECT alter_job($1, config => config || jsonb_build_object('maxchunks_to_compress', $2::int))
			 FROM timescaledb_information.jobs WHERE job_id = $1`,
			jobID, maxChunksToCompress,
		); err != nil {
			return fmt.Errorf("updating maxchunks_to_compress on %s (job %d): %w", table, jobID, err)
		}
		log.Ctx(ctx).Infof("Set maxchunks_to_compress %d on %s (job %d)", maxChunksToCompress, table, jobID)
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

// configureTransactionHashesMaintenance converges the transaction_hashes
// maintenance jobs on exactly workers jobs (at least 1), each running the
// transaction_hashes_maintenance procedure the migration creates every
// scheduleInterval with config {"workers": workers, "worker": i}. Job i
// deletes, from the chunks whose prefix range index modulo workers is i, the
// lookup rows of periods that closed before the oldest transactions row, then
// recompresses them: the lookup table's retention follows the oldest
// transactions row, and --retention-period and the COMPRESSION_* settings
// apply to the five bulk tables only. The migration creates worker
// 0's job unscheduled; this is what schedules it. A job whose schedule and
// config already match is left untouched, so job_id and run history survive
// restarts; a missing worker's job is added; jobs past the count, or a second
// job for the same worker, are deleted.
func configureTransactionHashesMaintenance(ctx context.Context, pool *pgxpool.Pool, scheduleInterval string, workers int) error {
	if workers < 1 {
		return fmt.Errorf("transaction_hashes maintenance needs at least 1 worker, got %d", workers)
	}

	type maintenanceJob struct {
		id      int
		worker  *int
		matches bool
	}
	rows, err := pool.Query(ctx,
		`SELECT job_id, (config->>'worker')::int,
		        scheduled AND schedule_interval = $1::interval
		        AND config = jsonb_build_object('workers', $2::int, 'worker', (config->>'worker')::int)
		 FROM timescaledb_information.jobs
		 WHERE proc_name = 'transaction_hashes_maintenance'
		 ORDER BY job_id`,
		scheduleInterval, workers,
	)
	if err != nil {
		return fmt.Errorf("listing transaction_hashes maintenance jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (maintenanceJob, error) {
		var j maintenanceJob
		if scanErr := row.Scan(&j.id, &j.worker, &j.matches); scanErr != nil {
			return j, fmt.Errorf("scanning transaction_hashes maintenance job: %w", scanErr)
		}
		return j, nil
	})
	if err != nil {
		return fmt.Errorf("listing transaction_hashes maintenance jobs: %w", err)
	}

	byWorker := make(map[int]maintenanceJob, workers)
	for _, j := range jobs {
		if j.worker != nil && *j.worker >= 0 && *j.worker < workers {
			if _, seen := byWorker[*j.worker]; !seen {
				byWorker[*j.worker] = j
				continue
			}
		}
		if _, err := pool.Exec(ctx, "SELECT delete_job($1)", j.id); err != nil {
			return fmt.Errorf("deleting transaction_hashes maintenance job %d: %w", j.id, err)
		}
		log.Ctx(ctx).Infof("Deleted transaction_hashes maintenance job %d", j.id)
	}

	for worker := range workers {
		config := fmt.Sprintf(`{"workers": %d, "worker": %d}`, workers, worker)
		j, ok := byWorker[worker]
		switch {
		case !ok:
			var jobID int
			if err := pool.QueryRow(ctx,
				"SELECT add_job('transaction_hashes_maintenance', $1::interval, config => $2::jsonb)",
				scheduleInterval, config,
			).Scan(&jobID); err != nil {
				return fmt.Errorf("adding transaction_hashes maintenance job for worker %d: %w", worker, err)
			}
			log.Ctx(ctx).Infof("Added transaction_hashes maintenance job %d (worker %d of %d, every %q)", jobID, worker, workers, scheduleInterval)
		case !j.matches:
			if _, err := pool.Exec(ctx,
				"SELECT alter_job($1, schedule_interval => $2::interval, config => $3::jsonb, scheduled => true)",
				j.id, scheduleInterval, config,
			); err != nil {
				return fmt.Errorf("altering transaction_hashes maintenance job %d: %w", j.id, err)
			}
			log.Ctx(ctx).Infof("Updated transaction_hashes maintenance job %d (worker %d of %d, every %q)", j.id, worker, workers, scheduleInterval)
		}
	}

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
