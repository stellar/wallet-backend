package db

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// droppableIndexes lists the parent index names (from migrations) that should be
// dropped on backfill chunks. TimescaleDB chunk indexes embed the parent name,
// e.g. parent "idx_transactions_hash" becomes "_hyper_1_5_chunk_idx_transactions_hash".
//
// Update this list when adding new indexes to hypertable migrations.
var droppableIndexes = []string{
	// TimescaleDB auto-created time-dimension indexes (safe to drop on backfill chunks;
	// only used for chunk exclusion on uncompressed reads, not needed during bulk writes
	// and replaced by segmentby/orderby metadata after compression)
	"transactions_ledger_created_at_idx",
	"transactions_accounts_ledger_created_at_idx",
	"operations_ledger_created_at_idx",
	"operations_accounts_ledger_created_at_idx",
	"state_changes_ledger_created_at_idx",
	// transactions (2025-06-10.2-transactions.sql)
	"idx_transactions_hash",
	"idx_transactions_toid_time",
	"idx_transactions_accounts_tx_to_id",
	"idx_transactions_accounts_account_id",
	// operations (2025-06-10.3-operations.sql)
	"idx_operations_id_time",
	"idx_operations_accounts_operation_id",
	"idx_operations_accounts_account_id",
	// state_changes (2025-06-10.4-statechanges.sql)
	"idx_state_changes_operation_id",
	"idx_state_changes_toid_opid_scid_time",
	"idx_state_changes_account_category",
}

type Chunk struct {
	Name       string
	Start      time.Time
	End        time.Time
	NumWriters atomic.Int64
}

// chunkRange is a time boundary for a single chunk.
type chunkRange struct {
	start time.Time
	end   time.Time
}

// PreCreateChunks pre-creates empty TimescaleDB chunks for the given hypertables
// covering the time range [rangeStart, rangeEnd]. Chunk boundaries are aligned to
// match TimescaleDB's internal alignment by reading interval_length from the catalog
// and replicating the same integer division used in calculate_open_range_default()
// (see timescale/timescaledb src/dimension.c):
//
//	range_start = (value / interval_length) * interval_length
//
// Existing chunks are skipped (create_chunk is idempotent for exact boundary matches).
//
// This is used before backfill to ensure chunks exist so their indexes can be
// dropped before bulk INSERTs, avoiding the ~40% write overhead from B-tree maintenance.
//
// Returns the aligned start time of the first chunk boundary (for use as a lower bound
// in downstream chunk queries like the progressive recompressor).
func PreCreateChunks(ctx context.Context, pool *pgxpool.Pool, hypertables []string, rangeStart, rangeEnd time.Time) ([]*Chunk, error) {
	// Generate aligned chunk boundaries using the same integer division as TimescaleDB's
	// C code. We read interval_length (microseconds) from the catalog and use
	// generate_series over chunk indices: floor(usec / interval) to ceil(usec / interval).
	rows, err := pool.Query(ctx, `
		WITH dim AS (
			SELECT d.interval_length
			FROM _timescaledb_catalog.dimension d
			JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
			WHERE h.table_name = $3 AND d.column_name = 'ledger_created_at'
		)
		SELECT
			to_timestamp((gs * dim.interval_length)::double precision / 1000000) AS chunk_start,
			to_timestamp(((gs + 1) * dim.interval_length)::double precision / 1000000) AS chunk_end
		FROM dim,
		LATERAL generate_series(
			(extract(epoch from $1::timestamptz) * 1000000)::bigint / dim.interval_length,
			(extract(epoch from $2::timestamptz) * 1000000)::bigint / dim.interval_length
		) AS gs
		ORDER BY gs`,
		rangeStart, rangeEnd, hypertables[0],
	)
	if err != nil {
		return []*Chunk{}, fmt.Errorf("generating chunk boundaries: %w", err)
	}
	var boundaries []chunkRange
	for rows.Next() {
		var boundary chunkRange
		if scanErr := rows.Scan(&boundary.start, &boundary.end); scanErr != nil {
			rows.Close()
			return []*Chunk{}, fmt.Errorf("scanning chunk boundary: %w", scanErr)
		}
		boundaries = append(boundaries, boundary)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return []*Chunk{}, fmt.Errorf("iterating chunk boundaries: %w", err)
	}

	var chunks []*Chunk
	for _, table := range hypertables {
		start := time.Now()
		numChunks := 0
		for _, boundary := range boundaries {
			chunk, err := prepareNewChunk(ctx, pool, table, boundary)
			if err != nil {
				return nil, err
			}
			if chunk != nil {
				chunks = append(chunks, chunk)
				numChunks++
			}
		}
		log.Ctx(ctx).Infof("Pre-created %d chunks for %s covering [%s, %s] in %v",
			numChunks, table, rangeStart.Format(time.RFC3339), rangeEnd.Format(time.RFC3339), time.Since(start))
	}
	return chunks, nil
}

// prepareNewChunk creates a chunk for the given boundary, drops its indexes, sets it
// UNLOGGED, and disables autovacuum. Returns nil if the chunk already existed.
func prepareNewChunk(ctx context.Context, pool *pgxpool.Pool, table string, boundary chunkRange) (*Chunk, error) {
	chunk := Chunk{Start: boundary.start, End: boundary.end}
	slices := fmt.Sprintf(`{"ledger_created_at": [%d, %d]}`,
		boundary.start.UnixMicro(), boundary.end.UnixMicro())
	var created bool
	if err := pool.QueryRow(ctx,
		"SELECT schema_name || '.' || table_name, created FROM _timescaledb_functions.create_chunk($1::regclass, $2::jsonb)",
		table, slices,
	).Scan(&chunk.Name, &created); err != nil {
		return nil, fmt.Errorf("creating chunk for %s at %s: %w", table, boundary.start.Format(time.RFC3339), err)
	}
	if !created {
		return nil, nil
	}

	numDropped, err := dropChunkIndexes(ctx, pool, chunk.Name)
	if err != nil {
		return nil, fmt.Errorf("dropping indexes on %s: %w", chunk.Name, err)
	}
	log.Ctx(ctx).Infof("Dropped %d indexes on chunk %s", numDropped, chunk.Name)

	// UNLOGGED disables WAL writes (~2-3x faster inserts, ~20x less WAL).
	// Safe for backfill data that can be re-ingested. Set back to LOGGED after compression.
	if err := executeChunkDDL(ctx, pool, chunk.Name, "SET UNLOGGED"); err != nil {
		return nil, err
	}
	// Disable autovacuum — no useful work during bulk COPY into UNLOGGED tables.
	// Re-enabled in SetChunkLogged after compression.
	if err := executeChunkDDL(ctx, pool, chunk.Name, "SET (autovacuum_enabled = false)"); err != nil {
		return nil, err
	}
	return &chunk, nil
}

// dropChunkIndexes drops indexes on a single chunk that match the droppableIndexes list.
// TimescaleDB chunk indexes embed the parent index name as a suffix, so we match using
// strings.HasSuffix. Returns the number of indexes dropped.
func dropChunkIndexes(ctx context.Context, pool *pgxpool.Pool, chunkName string) (int, error) {
	parts := strings.SplitN(chunkName, ".", 2)
	schemaName, tableName := parts[0], parts[1]

	rows, err := pool.Query(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
	`, schemaName, tableName)
	if err != nil {
		return 0, fmt.Errorf("querying indexes: %w", err)
	}

	var chunkIndexes []string
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			rows.Close()
			return 0, fmt.Errorf("scanning index name: %w", scanErr)
		}
		chunkIndexes = append(chunkIndexes, name)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterating index rows: %w", err)
	}

	dropped := 0
	for _, idx := range chunkIndexes {
		if !shouldDropIndex(idx) {
			continue
		}
		dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", schemaName, idx)
		if _, execErr := pool.Exec(ctx, dropSQL); execErr != nil {
			return dropped, fmt.Errorf("dropping index %s: %w", idx, execErr)
		}
		log.Ctx(ctx).Debugf("Dropped index %s.%s on %s", schemaName, idx, chunkName)
		dropped++
	}

	return dropped, nil
}

// shouldDropIndex checks if a chunk index name matches any parent index in droppableIndexes.
// TimescaleDB names chunk indexes as "<chunk_name>_<parent_index_name>",
// so we check if the chunk index name ends with the parent index name.
func shouldDropIndex(chunkIndexName string) bool {
	for _, parentIdx := range droppableIndexes {
		if strings.HasSuffix(chunkIndexName, parentIdx) {
			return true
		}
	}
	return false
}

// DisableInsertAutovacuum suppresses insert-triggered autovacuum on the given
// hypertables by setting autovacuum_vacuum_insert_threshold = -1. This prevents
// autovacuum from competing for I/O during bulk backfill. Call RestoreInsertAutovacuum
// after backfill to restore default behavior.
func DisableInsertAutovacuum(ctx context.Context, pool *pgxpool.Pool, hypertables []string) error {
	for _, table := range hypertables {
		sql := fmt.Sprintf("ALTER TABLE %s SET (autovacuum_vacuum_insert_threshold = -1)", table)
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("disabling insert autovacuum on %s: %w", table, err)
		}
		log.Ctx(ctx).Infof("Disabled insert-triggered autovacuum on %s", table)
	}
	return nil
}

// RestoreInsertAutovacuum resets autovacuum_vacuum_insert_threshold to the
// Postgres default on the given hypertables.
func RestoreInsertAutovacuum(ctx context.Context, pool *pgxpool.Pool, hypertables []string) error {
	for _, table := range hypertables {
		sql := fmt.Sprintf("ALTER TABLE %s RESET (autovacuum_vacuum_insert_threshold)", table)
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("restoring insert autovacuum on %s: %w", table, err)
		}
		log.Ctx(ctx).Infof("Restored insert-triggered autovacuum on %s", table)
	}
	return nil
}

// executeChunkDDL runs a single ALTER TABLE statement on a chunk.
func executeChunkDDL(ctx context.Context, pool *pgxpool.Pool, chunkName, ddl string) error {
	sql := fmt.Sprintf("ALTER TABLE %s %s", chunkName, ddl)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("alter table %s (%s): %w", chunkName, ddl, err)
	}
	return nil
}

// SetChunkLogged sets a single chunk back to LOGGED and re-enables autovacuum.
// Call this after compress_chunk() to restore crash safety and normal maintenance
// on the compressed data. TimescaleDB auto-propagates the LOGGED change to the
// internal compressed chunk.
func SetChunkLogged(ctx context.Context, pool *pgxpool.Pool, chunkName string) error {
	if err := executeChunkDDL(ctx, pool, chunkName, "SET LOGGED"); err != nil {
		return err
	}
	// Re-enable autovacuum (disabled during backfill by PrepareChunksForBackfill).
	if err := executeChunkDDL(ctx, pool, chunkName, "SET (autovacuum_enabled = true)"); err != nil {
		return err
	}
	return nil
}
