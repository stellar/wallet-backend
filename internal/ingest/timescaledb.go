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

// transactionHashesDefaultCompressAfter is the compress_after applied to
// transaction_hashes when --compression-compress-after is empty. TimescaleDB
// creates the table's policy with compress_after equal to its 7-day chunk
// interval, which would keep two chunks in the rowstore at a time; one day
// matches the other hypertables.
const transactionHashesDefaultCompressAfter = "1 day"

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
// policy that is paused. A backfill pauses the policies for its run and
// resumes them when it exits; a backfill killed before that point leaves them
// paused, so live mode schedules them again at every start.
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

// intervalSQL says how a PostgreSQL INTERVAL string reaches a hypertable's
// partition column type. The bulk-loaded hypertables are partitioned by a
// timestamptz and take the interval as is; transaction_hashes is partitioned
// by an integer number of seconds, so every interval is converted with
// extract(epoch FROM ...).
type intervalSQL struct {
	value      string // $2 as the partition column's type, for policy and chunk-interval arguments
	configCast string // cast applied to a job config value before comparing it with value
	jsonb      string // $2 as the jsonb value stored in a job config
}

var (
	timeInterval    = intervalSQL{value: "$2::interval", configCast: "::interval", jsonb: "to_jsonb($2::text)"}
	secondsInterval = intervalSQL{value: "extract(epoch FROM $2::interval)::bigint", configCast: "::bigint", jsonb: "to_jsonb(extract(epoch FROM $2::interval)::bigint)"}
)

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
		if err := setChunkInterval(ctx, pool, table, chunkInterval, timeInterval); err != nil {
			return err
		}
	}

	for _, table := range hypertables {
		if err := configureRetentionPolicy(ctx, pool, table, retentionPeriod, timeInterval); err != nil {
			return fmt.Errorf("configuring retention policy on %s: %w", table, err)
		}
	}

	if err := configureReconciliationJob(ctx, pool, retentionPeriod, oldestCursorName); err != nil {
		return fmt.Errorf("configuring reconciliation job: %w", err)
	}

	for _, table := range hypertables {
		if err := configureCompressionPolicy(ctx, pool, table, compressionScheduleInterval, compressAfter, maxChunksToCompress, timeInterval); err != nil {
			return err
		}
	}

	return nil
}

// configureTransactionHashesSettings applies the same chunk interval, retention
// and compression tuning to the transaction_hashes lookup table, converted to
// seconds for its integer partition column. TimescaleDB creates the compression
// policy with the table; an empty compressAfter sets it to
// transactionHashesDefaultCompressAfter instead of leaving it unchanged.
func configureTransactionHashesSettings(ctx context.Context, pool *pgxpool.Pool, chunkInterval, retentionPeriod, compressionScheduleInterval, compressAfter string, maxChunksToCompress int) error {
	const table = data.TransactionHashesTable
	if err := setChunkInterval(ctx, pool, table, chunkInterval, secondsInterval); err != nil {
		return err
	}
	if err := configureRetentionPolicy(ctx, pool, table, retentionPeriod, secondsInterval); err != nil {
		return fmt.Errorf("configuring retention policy on %s: %w", table, err)
	}
	if compressAfter == "" {
		compressAfter = transactionHashesDefaultCompressAfter
	}
	return configureCompressionPolicy(ctx, pool, table, compressionScheduleInterval, compressAfter, maxChunksToCompress, secondsInterval)
}

func setChunkInterval(ctx context.Context, pool *pgxpool.Pool, table, chunkInterval string, interval intervalSQL) error {
	if _, err := pool.Exec(ctx,
		"SELECT set_chunk_time_interval($1::regclass, "+interval.value+")",
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
func configureCompressionPolicy(ctx context.Context, pool *pgxpool.Pool, table, scheduleInterval, compressAfter string, maxChunksToCompress int, interval intervalSQL) error {
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
				'{compress_after}', `+interval.jsonb+`))`,
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
func configureRetentionPolicy(ctx context.Context, pool *pgxpool.Pool, table, retentionPeriod string, interval intervalSQL) error {
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
			"SELECT add_retention_policy($1::regclass, drop_after => "+interval.value+")",
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
		`SELECT (config->>'drop_after')`+interval.configCast+` = `+interval.value+`
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
			'{drop_after}', `+interval.jsonb+`))`,
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
