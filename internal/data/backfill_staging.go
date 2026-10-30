package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// BackfillStagingSchema holds the tables a backfill fills before attaching
// them as chunks. An attached table keeps its name and schema as a chunk, so
// the schema also names every chunk the backfill built this way.
const BackfillStagingSchema = "backfill"

// StagingTableName is the staging table of one bulk table for the window
// starting at rangeStart: the table name followed by the window start as
// YYYYMMDDHHMM in UTC.
func StagingTableName(table string, rangeStart time.Time) string {
	return fmt.Sprintf("%s_%s", table, rangeStart.UTC().Format("200601021504"))
}

// StagingRelation is the schema-qualified identifier of a staging table.
func StagingRelation(table string, rangeStart time.Time) pgx.Identifier {
	return pgx.Identifier{BackfillStagingSchema, StagingTableName(table, rangeStart)}
}

// CreateStagingTables creates one empty staging table per bulk table for the
// window starting at rangeStart, dropping any table of the same name first.
// A staging table copies the bulk table's columns, defaults and CHECK
// constraints, which attach_chunk requires, and nothing else: no primary key
// and no indexes, which attach_chunk builds in one pass once the table is
// full. Autovacuum is off because the table is rewritten by compression right
// after it is attached. The tables are logged: an unlogged chunk stays
// unlogged after compression and would never reach a streaming replica.
func (m *IngestStoreModel) CreateStagingTables(ctx context.Context, rangeStart time.Time) error {
	if _, err := m.DB.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+BackfillStagingSchema); err != nil {
		return fmt.Errorf("creating schema %s: %w", BackfillStagingSchema, err)
	}
	if err := m.DropStagingTables(ctx, rangeStart); err != nil {
		return err
	}
	for _, table := range BulkCopyTableNames() {
		relation := StagingRelation(table, rangeStart).Sanitize()
		if _, err := m.DB.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (autovacuum_enabled = off)`,
			relation, pgx.Identifier{table}.Sanitize())); err != nil {
			return fmt.Errorf("creating %s: %w", relation, err)
		}
	}
	return nil
}

// stagingTableIsLooseQuery is true for a table in the staging schema that
// TimescaleDB does not know: a staging table an interrupted window left
// behind. An attached table is a chunk, and its compressed relation (created
// next to it in the same schema) is recorded as that chunk's compress_relid;
// both are catalogued and must never be dropped here.
const stagingTableIsLooseQuery = `
	SELECT EXISTS (
		SELECT 1 FROM pg_tables t
		 WHERE t.schemaname = $1 AND t.tablename = $2
		   AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk ch WHERE ch.relid = format('%I.%I', t.schemaname, t.tablename)::regclass)
		   AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.compression_settings cs WHERE cs.compress_relid = format('%I.%I', t.schemaname, t.tablename)::regclass))`

// DropStagingTables removes the window's staging tables that were never
// attached. A staging table that already became a chunk is left alone: the
// window is then written in place and the chunk keeps its rows.
func (m *IngestStoreModel) DropStagingTables(ctx context.Context, rangeStart time.Time) error {
	for _, table := range BulkCopyTableNames() {
		name := StagingTableName(table, rangeStart)
		var loose bool
		if err := m.DB.QueryRow(ctx, stagingTableIsLooseQuery, BackfillStagingSchema, name).Scan(&loose); err != nil {
			return fmt.Errorf("checking %s.%s: %w", BackfillStagingSchema, name, err)
		}
		if !loose {
			continue
		}
		relation := pgx.Identifier{BackfillStagingSchema, name}.Sanitize()
		if _, err := m.DB.Exec(ctx, `DROP TABLE `+relation); err != nil {
			return fmt.Errorf("dropping %s: %w", relation, err)
		}
	}
	return nil
}

// ListOrphanStagingTables returns the tables in the staging schema that
// TimescaleDB does not know: what an interrupted window left behind.
// Attached tables and their compressed relations are catalogued and are not
// orphans.
func (m *IngestStoreModel) ListOrphanStagingTables(ctx context.Context) ([]string, error) {
	rows, err := m.DB.Query(ctx,
		`SELECT t.tablename
		   FROM pg_tables t
		  WHERE t.schemaname = $1
		    AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk ch WHERE ch.relid = format('%I.%I', t.schemaname, t.tablename)::regclass)
		    AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.compression_settings cs WHERE cs.compress_relid = format('%I.%I', t.schemaname, t.tablename)::regclass)
		  ORDER BY 1`, BackfillStagingSchema)
	if err != nil {
		return nil, fmt.Errorf("listing orphan staging tables: %w", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("collecting orphan staging tables: %w", err)
	}
	return names, nil
}

// DropOrphanStagingTables drops the tables ListOrphanStagingTables returns.
func (m *IngestStoreModel) DropOrphanStagingTables(ctx context.Context) ([]string, error) {
	orphans, err := m.ListOrphanStagingTables(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range orphans {
		relation := pgx.Identifier{BackfillStagingSchema, name}.Sanitize()
		if _, err := m.DB.Exec(ctx, `DROP TABLE `+relation); err != nil {
			return orphans, fmt.Errorf("dropping orphan %s: %w", relation, err)
		}
	}
	return orphans, nil
}

// AttachChunk makes the staging table of table for the window
// [rangeStart, rangeEnd) one of the hypertable's chunks. TimescaleDB builds
// the primary key and the hypertable's indexes on the table during the call
// and adds the range as a CHECK constraint. Slices are microseconds since the
// Unix epoch, the internal representation of timestamptz; the range must sit
// on the hypertable's chunk boundaries and overlap no existing chunk.
//
// The call runs in the caller's transaction together with the chunk's
// chunk-skipping statistics, so a reader sees the chunk with its statistics
// or not at all. A chunk created by an insert gets one full-range row per
// chunk-skipping column at creation; attach_chunk creates none, and the
// planner prunes a chunk that has no row for a column its predicate names
// (verified on 2.28.2 and 2.31.0), which would hide the window from every
// TOID- or ledger-keyed read, from GetLedgerGaps and from the pre-fill
// delete until compression computes the real ranges. Inserting the rows the
// way chunk creation does closes that gap; compression later narrows them.
func AttachChunk(ctx context.Context, tx pgx.Tx, table string, rangeStart, rangeEnd time.Time) error {
	slices := fmt.Sprintf(`{"ledger_created_at": [%d, %d]}`, rangeStart.UnixMicro(), rangeEnd.UnixMicro())
	relation := StagingRelation(table, rangeStart)
	if _, err := tx.Exec(ctx, `CALL attach_chunk($1::regclass, $2::regclass, $3::jsonb)`, table, relation.Sanitize(), slices); err != nil {
		return fmt.Errorf("attaching %s to %s: %w", relation.Sanitize(), table, err)
	}
	if _, err := tx.Exec(ctx, insertChunkSkippingStatsQuery, relation.Sanitize()); err != nil {
		return fmt.Errorf("recording chunk-skipping statistics for %s: %w", relation.Sanitize(), err)
	}
	return nil
}

// insertChunkSkippingStatsQuery gives a chunk the full-range chunk-skipping
// statistics row for every column its hypertable tracks, like chunk creation
// does; rows that already exist are left alone.
const insertChunkSkippingStatsQuery = `
	INSERT INTO _timescaledb_catalog.chunk_column_stats (hypertable_id, chunk_id, column_name, range_start, range_end, valid)
	SELECT s.hypertable_id, ch.id, s.column_name, s.range_start, s.range_end, true
	  FROM _timescaledb_catalog.chunk ch
	  JOIN _timescaledb_catalog.chunk_column_stats s ON s.hypertable_id = ch.hypertable_id AND s.chunk_id IS NULL
	 WHERE ch.relid = $1::regclass
	   AND NOT EXISTS (
	       SELECT 1 FROM _timescaledb_catalog.chunk_column_stats x
	        WHERE x.chunk_id = ch.id AND x.column_name = s.column_name)`
