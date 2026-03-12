package db

import (
	"context"
	"fmt"
	"strings"
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
	"idx_transactions_accounts_tx_to_id",
	"idx_transactions_accounts_account_id",
	// operations (2025-06-10.3-operations.sql)
	"idx_operations_accounts_operation_id",
	"idx_operations_accounts_account_id",
	// state_changes (2025-06-10.4-statechanges.sql)
	"idx_state_changes_operation_id",
	"idx_state_changes_account_category",
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
func PreCreateChunks(ctx context.Context, pool *pgxpool.Pool, hypertables []string, rangeStart, rangeEnd time.Time) (time.Time, error) {
	// Generate aligned chunk boundaries using the same integer division as TimescaleDB's
	// C code. We read interval_length (microseconds) from the catalog and use
	// generate_series over chunk indices: floor(usec / interval) to ceil(usec / interval).
	type chunkRange struct {
		Start time.Time
		End   time.Time
	}
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
		) AS gs`,
		rangeStart, rangeEnd, hypertables[0],
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("generating chunk boundaries: %w", err)
	}
	var boundaries []chunkRange
	for rows.Next() {
		var cr chunkRange
		if scanErr := rows.Scan(&cr.Start, &cr.End); scanErr != nil {
			rows.Close()
			return time.Time{}, fmt.Errorf("scanning chunk boundary: %w", scanErr)
		}
		boundaries = append(boundaries, cr)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterating chunk boundaries: %w", err)
	}

	for _, table := range hypertables {
		start := time.Now()
		created := 0
		for _, b := range boundaries {
			// create_chunk expects slices as {"dimension": [start_usec, end_usec]}.
			// It is idempotent: returns created=false if a chunk already covers this range.
			slices := fmt.Sprintf(`{"ledger_created_at": [%d, %d]}`,
				b.Start.UnixMicro(), b.End.UnixMicro())
			_, execErr := pool.Exec(ctx,
				"SELECT * FROM _timescaledb_functions.create_chunk($1::regclass, $2::jsonb)",
				table, slices)
			if execErr != nil {
				if strings.Contains(execErr.Error(), "collision") {
					log.Ctx(ctx).Warnf("Chunk collision for %s at %s (pre-existing chunk), skipping", table, b.Start.Format(time.RFC3339))
					continue
				}
				return time.Time{}, fmt.Errorf("creating chunk for %s at %s: %w", table, b.Start.Format(time.RFC3339), execErr)
			}
			created++
		}
		log.Ctx(ctx).Infof("Pre-created %d chunks for %s covering [%s, %s] in %v",
			created, table, rangeStart.Format(time.RFC3339), rangeEnd.Format(time.RFC3339), time.Since(start))
	}

	// Return the aligned start of the first chunk boundary.
	var alignedStart time.Time
	if len(boundaries) > 0 {
		alignedStart = boundaries[0].Start
	}
	return alignedStart, nil
}

// PrepareChunksForBackfill drops indexes and sets chunks UNLOGGED on uncompressed
// chunks within the given time range for the specified hypertables.
//
// Dropping indexes removes B-tree maintenance overhead (~40% write speedup).
// Setting UNLOGGED disables WAL writes (~2-3x faster inserts, ~20x less WAL).
// Unlogged chunks are truncated on crash recovery, so this is only safe for backfill
// data that can be re-ingested. Caller must set chunks back to LOGGED after compression.
func PrepareChunksForBackfill(ctx context.Context, pool *pgxpool.Pool, hypertables []string, rangeStart, rangeEnd time.Time) error {
	for _, table := range hypertables {
		// Find uncompressed chunks in range
		rows, err := pool.Query(ctx, `
			SELECT chunk_schema, chunk_name
			FROM timescaledb_information.chunks
			WHERE hypertable_name = $1
			  AND NOT is_compressed
			  AND range_start < $3
			  AND range_end > $2
		`, table, rangeStart, rangeEnd)
		if err != nil {
			return fmt.Errorf("querying chunks for %s: %w", table, err)
		}

		type chunk struct {
			Schema string
			Name   string
		}
		var chunks []chunk
		for rows.Next() {
			var c chunk
			if scanErr := rows.Scan(&c.Schema, &c.Name); scanErr != nil {
				rows.Close()
				return fmt.Errorf("scanning chunk row for %s: %w", table, scanErr)
			}
			chunks = append(chunks, c)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return fmt.Errorf("iterating chunk rows for %s: %w", table, err)
		}

		dropped := 0
		for _, c := range chunks {
			n, dropErr := dropChunkIndexes(ctx, pool, c.Schema, c.Name)
			if dropErr != nil {
				return fmt.Errorf("dropping indexes on %s.%s: %w", c.Schema, c.Name, dropErr)
			}
			dropped += n

			alterSQL := fmt.Sprintf("ALTER TABLE %s.%s SET UNLOGGED", c.Schema, c.Name)
			if _, execErr := pool.Exec(ctx, alterSQL); execErr != nil {
				return fmt.Errorf("setting chunk %s.%s unlogged: %w", c.Schema, c.Name, execErr)
			}
		}

		log.Ctx(ctx).Infof("Prepared %d chunks for backfill (dropped %d indexes, set UNLOGGED) for %s",
			len(chunks), dropped, table)
	}
	return nil
}

// dropChunkIndexes drops indexes on a single chunk that match the droppableIndexes list.
// TimescaleDB chunk indexes embed the parent index name as a suffix, so we match using
// strings.HasSuffix. Returns the number of indexes dropped.
func dropChunkIndexes(ctx context.Context, pool *pgxpool.Pool, chunkSchema, chunkName string) (int, error) {
	rows, err := pool.Query(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
	`, chunkSchema, chunkName)
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
		dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", chunkSchema, idx)
		if _, execErr := pool.Exec(ctx, dropSQL); execErr != nil {
			return dropped, fmt.Errorf("dropping index %s: %w", idx, execErr)
		}
		log.Ctx(ctx).Debugf("Dropped index %s.%s on %s.%s", chunkSchema, idx, chunkSchema, chunkName)
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

// SetChunkLogged sets a single chunk back to LOGGED, re-enabling WAL writes.
// Call this after compress_chunk() to restore crash safety on the compressed data.
// TimescaleDB auto-propagates the change to the internal compressed chunk.
func SetChunkLogged(ctx context.Context, pool *pgxpool.Pool, chunkName string) error {
	alterSQL := fmt.Sprintf("ALTER TABLE %s SET LOGGED", chunkName)
	if _, err := pool.Exec(ctx, alterSQL); err != nil {
		return fmt.Errorf("setting chunk %s logged: %w", chunkName, err)
	}
	return nil
}
