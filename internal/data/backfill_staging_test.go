package data

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func openStagingTestDB(t *testing.T) (*pgxpool.Pool, *IngestStoreModel) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	pool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	return pool, &IngestStoreModel{DB: pool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB}
}

func Test_StagingTableName(t *testing.T) {
	start := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, "transactions_202610270000", StagingTableName("transactions", start))
	assert.Equal(t, "state_changes_202610271300", StagingTableName("state_changes", start.Add(13*time.Hour).In(time.FixedZone("x", -3600))))
	assert.Equal(t, `"backfill"."operations_202610270000"`, StagingRelation("operations", start).Sanitize())
}

func Test_IngestStoreModel_StagingTablesAndAttachChunk(t *testing.T) {
	pool, m := openStagingTestDB(t)
	ctx := context.Background()
	day := 24 * time.Hour
	start := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	end := start.Add(day)

	require.NoError(t, m.CreateStagingTables(ctx, start))

	t.Run("staging tables copy columns and CHECK constraints but no indexes, logged, autovacuum off", func(t *testing.T) {
		for _, table := range BulkCopyTableNames() {
			name := StagingTableName(table, start)
			var persistence string
			var reloptions []string
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT c.relpersistence::text, c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`,
				BackfillStagingSchema, name).Scan(&persistence, &reloptions))
			assert.Equal(t, "p", persistence, "%s is logged", name)
			assert.Contains(t, reloptions, "autovacuum_enabled=off", "%s", name)

			var indexes int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = $2`, BackfillStagingSchema, name).Scan(&indexes))
			assert.Zero(t, indexes, "%s has no indexes before attach", name)

			var hypertableChecks, stagingChecks int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c'`, table).Scan(&hypertableChecks))
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c'`, StagingRelation(table, start).Sanitize()).Scan(&stagingChecks))
			assert.Equal(t, hypertableChecks, stagingChecks, "%s carries the hypertable's CHECK constraints", name)
		}
	})

	// Fill the transactions staging table, then attach it.
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(pgxTx pgx.Tx) error {
		_, err := pgxTx.Exec(ctx,
			`INSERT INTO `+StagingRelation("transactions", start).Sanitize()+` (to_id, hash, fee_charged, result_code, ledger_number, is_fee_bump, ledger_created_at)
			 SELECT (1000::bigint << 32) + i, decode(md5(i::text), 'hex') || decode(md5(i::text || 'b'), 'hex'), 100, 'txSUCCESS', 1000, false, $1::timestamptz + (i || ' minutes')::interval
			   FROM generate_series(1, 500) i`, start)
		return err
	}))

	t.Run("attach builds the primary key, routes later inserts and reads, stays logged", func(t *testing.T) {
		require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return AttachChunk(ctx, tx, "transactions", start, end)
		}))

		// The chunk is visible to reads keyed by the chunk-skipping columns,
		// to the gap query and to the pre-fill delete, which all prune a chunk
		// without statistics rows.
		var statsRows int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM _timescaledb_catalog.chunk_column_stats s JOIN _timescaledb_catalog.chunk ch ON ch.id = s.chunk_id WHERE ch.relid = $1::regclass`,
			StagingRelation("transactions", start).Sanitize()).Scan(&statsRows))
		assert.Equal(t, 2, statsRows, "one statistics row per chunk-skipping column (to_id, ledger_number)")
		var byTOID, byLedger int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE to_id = (1000::bigint << 32) + 7`).Scan(&byTOID))
		assert.Equal(t, 1, byTOID, "a TOID-keyed read sees the attached chunk")
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(DISTINCT ledger_number) FROM transactions WHERE ledger_number BETWEEN 999 AND 1001`).Scan(&byLedger))
		assert.Equal(t, 1, byLedger, "a ledger-keyed read sees the attached chunk")

		var chunkSchema, chunkName string
		var rangeStart, rangeEnd time.Time
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT chunk_schema, chunk_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'transactions'`).
			Scan(&chunkSchema, &chunkName, &rangeStart, &rangeEnd))
		assert.Equal(t, BackfillStagingSchema, chunkSchema)
		assert.Equal(t, StagingTableName("transactions", start), chunkName)
		assert.True(t, rangeStart.Equal(start) && rangeEnd.Equal(end), "range %s..%s", rangeStart, rangeEnd)

		var pkIndexes int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = $1::regclass AND i.indisprimary`,
			StagingRelation("transactions", start).Sanitize()).Scan(&pkIndexes))
		assert.Equal(t, 1, pkIndexes, "attach builds the primary key")

		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE ledger_created_at >= $1 AND ledger_created_at < $2`, start, end).Scan(&count))
		assert.Equal(t, 500, count, "rows are read through the hypertable")

		_, err := pool.Exec(ctx,
			`INSERT INTO transactions (to_id, hash, fee_charged, result_code, ledger_number, is_fee_bump, ledger_created_at)
			 VALUES ((1001::bigint << 32), decode(md5('late'), 'hex') || decode(md5('late2'), 'hex'), 1, 'txSUCCESS', 1001, false, $1)`, start.Add(12*time.Hour))
		require.NoError(t, err)
		var owner string
		require.NoError(t, pool.QueryRow(ctx, `SELECT tableoid::regclass::text FROM transactions WHERE to_id = (1001::bigint << 32)`).Scan(&owner))
		assert.Equal(t, BackfillStagingSchema+"."+StagingTableName("transactions", start), owner, "a later insert routes to the attached chunk")

		var persistence string
		require.NoError(t, pool.QueryRow(ctx, `SELECT relpersistence::text FROM pg_class WHERE oid = $1::regclass`, StagingRelation("transactions", start).Sanitize()).Scan(&persistence))
		assert.Equal(t, "p", persistence)
	})

	t.Run("attach rejects a table without the hypertable's CHECK constraints", func(t *testing.T) {
		_, err := pool.Exec(ctx, `CREATE TABLE backfill.operations_nochecks (LIKE operations INCLUDING DEFAULTS)`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `CALL attach_chunk('operations', 'backfill.operations_nochecks', $1::jsonb)`,
			`{"ledger_created_at": [`+itoa(start.UnixMicro())+`, `+itoa(end.UnixMicro())+`]}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing constraint")
	})

	t.Run("orphans are the staging tables that are not chunks", func(t *testing.T) {
		orphans, err := m.ListOrphanStagingTables(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{
			"operations_nochecks",
			StagingTableName("transactions_accounts", start),
			StagingTableName("operations", start),
			StagingTableName("operations_accounts", start),
			StagingTableName("state_changes", start),
		}, orphans, "the attached transactions table is a chunk, not an orphan")

		dropped, err := m.DropOrphanStagingTables(ctx)
		require.NoError(t, err)
		assert.Len(t, dropped, 5)
		remaining, err := m.ListOrphanStagingTables(ctx)
		require.NoError(t, err)
		assert.Empty(t, remaining)

		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&count))
		assert.Equal(t, 501, count, "the attached chunk is untouched")
	})

	t.Run("a compressed attached chunk and its compressed relation are not orphans", func(t *testing.T) {
		_, err := pool.Exec(ctx, `SELECT compress_chunk($1::regclass)`, StagingRelation("transactions", start).Sanitize())
		require.NoError(t, err)
		var compressedRelations int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename LIKE '%_compressed'`, BackfillStagingSchema).Scan(&compressedRelations))
		require.Equal(t, 1, compressedRelations, "the compressed relation lives in the staging schema")
		orphans, err := m.ListOrphanStagingTables(ctx)
		require.NoError(t, err)
		assert.Empty(t, orphans)

		// Dropping the window's staging tables leaves the attached chunk alone.
		require.NoError(t, m.DropStagingTables(ctx, start))
		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&count))
		assert.Equal(t, 501, count)
	})

	t.Run("DropStagingTables is idempotent", func(t *testing.T) {
		require.NoError(t, m.DropStagingTables(ctx, start.Add(day)))
		require.NoError(t, m.CreateStagingTables(ctx, start.Add(day)))
		require.NoError(t, m.DropStagingTables(ctx, start.Add(day)))
		orphans, err := m.ListOrphanStagingTables(ctx)
		require.NoError(t, err)
		assert.Empty(t, orphans)
	})
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
