package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

// queryRelpersistence returns 'p' (LOGGED/permanent) or 'u' (UNLOGGED) for the given
// schema-qualified chunk name (e.g. "_timescaledb_internal.chunk_name").
func queryRelpersistence(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chunkName string) string {
	t.Helper()
	parts := strings.SplitN(chunkName, ".", 2)
	require.Len(t, parts, 2, "chunkName must be schema.table")
	var persistence string
	err := pool.QueryRow(ctx, `
		SELECT relpersistence::text
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, parts[0], parts[1]).Scan(&persistence)
	require.NoError(t, err, "querying relpersistence for %s", chunkName)
	return persistence
}

// countChunkIndexes returns the number of indexes on the given schema-qualified chunk.
func countChunkIndexes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chunkName string) int {
	t.Helper()
	parts := strings.SplitN(chunkName, ".", 2)
	require.Len(t, parts, 2)
	var count int
	err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
	`, parts[0], parts[1]).Scan(&count)
	require.NoError(t, err, "counting indexes for %s", chunkName)
	return count
}

// countDroppableIndexes returns the number of indexes on the chunk that match droppableIndexes.
func countDroppableIndexes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chunkName string) int {
	t.Helper()
	parts := strings.SplitN(chunkName, ".", 2)
	require.Len(t, parts, 2)

	rows, err := pool.Query(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
	`, parts[0], parts[1])
	require.NoError(t, err)

	count := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		if shouldDropIndex(name) {
			count++
		}
	}
	rows.Close()
	require.NoError(t, rows.Err())
	return count
}

// hasReloption checks if the given schema-qualified table has the specified reloption.
// The option should be in "key=value" format (e.g. "autovacuum_enabled=false").
func hasReloption(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName, option string) bool {
	t.Helper()
	parts := strings.SplitN(tableName, ".", 2)
	schema, table := "public", tableName
	if len(parts) == 2 {
		schema, table = parts[0], parts[1]
	}
	var has bool
	// Use fmt.Sprintf for the option value because pgx doesn't support parameterized
	// array literals like ARRAY[$3::text] (it counts placeholders incorrectly).
	query := fmt.Sprintf(`
		SELECT coalesce(c.reloptions @> ARRAY['%s'], false)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, option)
	err := pool.QueryRow(ctx, query, schema, table).Scan(&has)
	require.NoError(t, err, "checking reloption %q on %s", option, tableName)
	return has
}

// createTestChunk creates a single chunk via _timescaledb_functions.create_chunk and returns
// its schema-qualified name (e.g. "_timescaledb_internal._hyper_1_1_chunk").
func createTestChunk(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, start, end time.Time) string {
	t.Helper()
	slices := fmt.Sprintf(`{"ledger_created_at": [%d, %d]}`, start.UnixMicro(), end.UnixMicro())
	var chunkName string
	var created bool
	err := pool.QueryRow(ctx,
		"SELECT schema_name || '.' || table_name, created FROM _timescaledb_functions.create_chunk($1::regclass, $2::jsonb)",
		table, slices,
	).Scan(&chunkName, &created)
	require.NoError(t, err, "creating test chunk for %s", table)
	return chunkName
}

// openChunksTestPool opens a migrated test DB and returns a pgx pool suitable for chunk tests.
func openChunksTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(dbt.Close)
	ctx := context.Background()
	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, ctx
}

// --- Test: shouldDropIndex (pure function, no DB) ---

func TestShouldDropIndex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "matches chunk-prefixed droppable index (transactions hash)",
			input:    "_hyper_1_5_chunk_idx_transactions_hash",
			expected: true,
		},
		{
			name:     "matches time-dimension index (transactions)",
			input:    "_hyper_1_5_chunk_transactions_ledger_created_at_idx",
			expected: true,
		},
		{
			name:     "matches transactions_accounts tx_to_id",
			input:    "_hyper_2_10_chunk_idx_transactions_accounts_tx_to_id",
			expected: true,
		},
		{
			name:     "matches transactions_accounts account_id",
			input:    "_hyper_2_10_chunk_idx_transactions_accounts_account_id",
			expected: true,
		},
		{
			name:     "matches operations_accounts operation_id",
			input:    "_hyper_3_15_chunk_idx_operations_accounts_operation_id",
			expected: true,
		},
		{
			name:     "matches operations_accounts account_id",
			input:    "_hyper_3_15_chunk_idx_operations_accounts_account_id",
			expected: true,
		},
		{
			name:     "matches state_changes operation_id",
			input:    "_hyper_5_20_chunk_idx_state_changes_operation_id",
			expected: true,
		},
		{
			name:     "matches state_changes account_category",
			input:    "_hyper_5_20_chunk_idx_state_changes_account_category",
			expected: true,
		},
		{
			name:     "matches operations time-dimension index",
			input:    "_hyper_3_15_chunk_operations_ledger_created_at_idx",
			expected: true,
		},
		{
			name:     "matches state_changes time-dimension index",
			input:    "_hyper_5_20_chunk_state_changes_ledger_created_at_idx",
			expected: true,
		},
		{
			name:     "exact parent name matches via HasSuffix",
			input:    "idx_transactions_hash",
			expected: true,
		},
		{
			name:     "matches transactions toid_time (PK replacement)",
			input:    "_hyper_1_5_chunk_idx_transactions_toid_time",
			expected: true,
		},
		{
			name:     "matches operations id_time (PK replacement)",
			input:    "_hyper_3_15_chunk_idx_operations_id_time",
			expected: true,
		},
		{
			name:     "matches state_changes toid_opid_scid_time (PK replacement)",
			input:    "_hyper_5_20_chunk_idx_state_changes_toid_opid_scid_time",
			expected: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "unrelated index",
			input:    "_hyper_1_5_chunk_some_other_index",
			expected: false,
		},
		{
			name:     "partial prefix match with extra suffix should not match",
			input:    "idx_transactions_hash_extra_suffix",
			expected: false,
		},
		{
			name:     "substring of droppable but not suffix",
			input:    "prefix_idx_transactions_hash_nope",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := shouldDropIndex(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// --- Test: dropChunkIndexes (real DB) ---

func TestDropChunkIndexes(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	// Use Jan 2025 — isolated from other tests
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	chunkName := createTestChunk(t, ctx, pool, "transactions", start, end)

	t.Run("drops expected indexes on new chunk", func(t *testing.T) {
		indexesBefore := countChunkIndexes(t, ctx, pool, chunkName)
		droppableBefore := countDroppableIndexes(t, ctx, pool, chunkName)
		require.Greater(t, droppableBefore, 0, "new chunk should have droppable indexes")

		dropped, err := dropChunkIndexes(ctx, pool, chunkName)
		require.NoError(t, err)
		assert.Equal(t, droppableBefore, dropped, "should drop all droppable indexes")

		indexesAfter := countChunkIndexes(t, ctx, pool, chunkName)
		assert.Equal(t, indexesBefore-dropped, indexesAfter, "total indexes should decrease by dropped count")

		// No droppable indexes should remain
		assert.Equal(t, 0, countDroppableIndexes(t, ctx, pool, chunkName))
	})

	t.Run("returns zero on second call", func(t *testing.T) {
		dropped, err := dropChunkIndexes(ctx, pool, chunkName)
		require.NoError(t, err)
		assert.Equal(t, 0, dropped)
	})
}

// --- Test: executeChunkDDL (real DB) ---

func TestExecuteChunkDDL(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	// Use Feb 2025
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)
	chunkName := createTestChunk(t, ctx, pool, "transactions", start, end)

	t.Run("SET UNLOGGED", func(t *testing.T) {
		err := executeChunkDDL(ctx, pool, chunkName, "SET UNLOGGED")
		require.NoError(t, err)
		assert.Equal(t, "u", queryRelpersistence(t, ctx, pool, chunkName))
	})

	t.Run("SET LOGGED", func(t *testing.T) {
		err := executeChunkDDL(ctx, pool, chunkName, "SET LOGGED")
		require.NoError(t, err)
		assert.Equal(t, "p", queryRelpersistence(t, ctx, pool, chunkName))
	})

	t.Run("SET autovacuum disabled", func(t *testing.T) {
		err := executeChunkDDL(ctx, pool, chunkName, "SET (autovacuum_enabled = false)")
		require.NoError(t, err)
		assert.True(t, hasReloption(t, ctx, pool, chunkName, "autovacuum_enabled=false"))
	})

	t.Run("invalid DDL returns error", func(t *testing.T) {
		err := executeChunkDDL(ctx, pool, chunkName, "INVALID_DDL")
		require.Error(t, err)
		assert.ErrorContains(t, err, "alter table")
	})

	t.Run("nonexistent chunk returns error", func(t *testing.T) {
		err := executeChunkDDL(ctx, pool, "_timescaledb_internal.nonexistent_chunk", "SET LOGGED")
		require.Error(t, err)
		assert.ErrorContains(t, err, "alter table")
	})
}

// --- Test: prepareNewChunk (real DB) ---

func TestPrepareNewChunk(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	t.Run("creates chunk with correct state", func(t *testing.T) {
		// Use Mar 2025
		boundary := chunkRange{
			start: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC),
		}
		chunk, err := prepareNewChunk(ctx, pool, "transactions", boundary)
		require.NoError(t, err)
		require.NotNil(t, chunk)

		// Name should be populated
		assert.NotEmpty(t, chunk.Name)
		assert.Contains(t, chunk.Name, "_timescaledb_internal.")

		// Start and End should match boundary
		assert.Equal(t, boundary.start, chunk.Start)
		assert.Equal(t, boundary.end, chunk.End)

		// Should be UNLOGGED
		assert.Equal(t, "u", queryRelpersistence(t, ctx, pool, chunk.Name))

		// No droppable indexes should remain
		assert.Equal(t, 0, countDroppableIndexes(t, ctx, pool, chunk.Name))

		// Autovacuum should be disabled
		assert.True(t, hasReloption(t, ctx, pool, chunk.Name, "autovacuum_enabled=false"))
	})

	t.Run("returns nil for existing chunk", func(t *testing.T) {
		// Same boundary as above — chunk already exists
		boundary := chunkRange{
			start: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC),
		}
		chunk, err := prepareNewChunk(ctx, pool, "transactions", boundary)
		require.NoError(t, err)
		assert.Nil(t, chunk, "should return nil for already-existing chunk")
	})

	t.Run("works for all hypertables", func(t *testing.T) {
		tables := []string{"transactions", "transactions_accounts", "operations", "operations_accounts", "state_changes"}
		// Use Apr 2025 — fresh boundary for all tables
		boundary := chunkRange{
			start: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2025, 4, 2, 0, 0, 0, 0, time.UTC),
		}
		for _, table := range tables {
			t.Run(table, func(t *testing.T) {
				chunk, err := prepareNewChunk(ctx, pool, table, boundary)
				require.NoError(t, err)
				require.NotNil(t, chunk, "should create chunk for %s", table)
				assert.Equal(t, "u", queryRelpersistence(t, ctx, pool, chunk.Name))
			})
		}
	})
}

// --- Test: PreCreateChunks (real DB) ---

func TestPreCreateChunks(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	t.Run("single-day range, 2 tables", func(t *testing.T) {
		// Use May 2025
		start := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2025, 5, 1, 23, 59, 59, 0, time.UTC)
		tables := []string{"transactions", "operations"}

		chunks, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		require.NotEmpty(t, chunks)

		// Each chunk should be UNLOGGED
		for _, chunk := range chunks {
			assert.Equal(t, "u", queryRelpersistence(t, ctx, pool, chunk.Name),
				"chunk %s should be UNLOGGED", chunk.Name)
		}
	})

	t.Run("multi-day range", func(t *testing.T) {
		// Use Jun 2025 — 3-day range
		start := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2025, 6, 3, 23, 59, 59, 0, time.UTC)
		tables := []string{"transactions"}

		chunks, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		// With default 1-day chunk interval, 3-day range should yield at least 3 chunks
		assert.GreaterOrEqual(t, len(chunks), 3, "3-day range should produce at least 3 chunks")
	})

	t.Run("idempotent (skips existing)", func(t *testing.T) {
		// Use Jul 2025
		start := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2025, 7, 1, 23, 59, 59, 0, time.UTC)
		tables := []string{"transactions"}

		chunks1, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		require.NotEmpty(t, chunks1)

		chunks2, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		assert.Empty(t, chunks2, "second call should return empty — all chunks already exist")
	})

	t.Run("unknown table returns empty chunks (no matching dimension)", func(t *testing.T) {
		start := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2025, 8, 1, 23, 59, 59, 0, time.UTC)
		// The first table is used to look up the dimension interval in the catalog.
		// A nonexistent table has no dimension entry, so the boundary query returns
		// zero rows and PreCreateChunks returns an empty slice without error.
		tables := []string{"nonexistent_table"}

		chunks, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		assert.Empty(t, chunks, "nonexistent table should produce no chunks")
	})

	t.Run("chunk boundaries are day-aligned to midnight UTC", func(t *testing.T) {
		// Use Sep 2025 — start mid-day to verify alignment snaps to midnight
		start := time.Date(2025, 9, 1, 14, 30, 0, 0, time.UTC)
		end := time.Date(2025, 9, 3, 14, 30, 0, 0, time.UTC)
		tables := []string{"transactions"}

		chunks, err := PreCreateChunks(ctx, pool, tables, start, end)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(chunks), 2, "need at least 2 chunks to verify alignment")

		// TimescaleDB aligns 1-day chunks to midnight UTC because:
		// - Internal formula: range_start = (pg_epoch_usec / interval_length) * interval_length
		// - The PG epoch (2000-01-01 00:00:00 UTC) is itself at midnight
		// - 1 day = 86400000000 µs divides evenly into the epoch difference
		for _, chunk := range chunks {
			utcStart := chunk.Start.UTC()
			assert.Equal(t, 0, utcStart.Hour(), "chunk %s start hour should be 0", chunk.Name)
			assert.Equal(t, 0, utcStart.Minute(), "chunk %s start minute should be 0", chunk.Name)
			assert.Equal(t, 0, utcStart.Second(), "chunk %s start second should be 0", chunk.Name)
		}

		// Each chunk's duration (End - Start) should equal exactly 1 day
		oneDayMicros := int64(24 * time.Hour / time.Microsecond)
		for i, chunk := range chunks {
			durationMicros := chunk.End.Sub(chunk.Start).Microseconds()
			assert.Equal(t, oneDayMicros, durationMicros,
				"chunk %d duration should be exactly 1 day", i)
		}
	})
}

// --- Test: DisableInsertAutovacuum / RestoreInsertAutovacuum (real DB) ---

func TestDisableInsertAutovacuum(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	t.Run("disables insert autovacuum", func(t *testing.T) {
		tables := []string{"transactions", "operations"}
		err := DisableInsertAutovacuum(ctx, pool, tables)
		require.NoError(t, err)

		for _, table := range tables {
			assert.True(t, hasReloption(t, ctx, pool, table, "autovacuum_vacuum_insert_threshold=-1"),
				"table %s should have insert threshold = -1", table)
		}
	})

	t.Run("nonexistent table returns error", func(t *testing.T) {
		err := DisableInsertAutovacuum(ctx, pool, []string{"nonexistent_table"})
		require.Error(t, err)
		assert.ErrorContains(t, err, "disabling insert autovacuum")
	})
}

func TestRestoreInsertAutovacuum(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	t.Run("restore removes threshold", func(t *testing.T) {
		tables := []string{"transactions"}

		// First disable
		err := DisableInsertAutovacuum(ctx, pool, tables)
		require.NoError(t, err)
		assert.True(t, hasReloption(t, ctx, pool, "transactions", "autovacuum_vacuum_insert_threshold=-1"))

		// Then restore
		err = RestoreInsertAutovacuum(ctx, pool, tables)
		require.NoError(t, err)
		assert.False(t, hasReloption(t, ctx, pool, "transactions", "autovacuum_vacuum_insert_threshold=-1"),
			"insert threshold should be removed after restore")
	})

	t.Run("nonexistent table returns error", func(t *testing.T) {
		err := RestoreInsertAutovacuum(ctx, pool, []string{"nonexistent_table"})
		require.Error(t, err)
		assert.ErrorContains(t, err, "restoring insert autovacuum")
	})
}

// --- Test: SetChunkLogged (real DB) ---

func TestSetChunkLogged(t *testing.T) {
	pool, ctx := openChunksTestPool(t)

	t.Run("converts UNLOGGED to LOGGED and re-enables autovacuum", func(t *testing.T) {
		// Use Oct 2025
		boundary := chunkRange{
			start: time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2025, 10, 2, 0, 0, 0, 0, time.UTC),
		}
		chunk, err := prepareNewChunk(ctx, pool, "transactions", boundary)
		require.NoError(t, err)
		require.NotNil(t, chunk)

		// Verify precondition: chunk is UNLOGGED with autovacuum disabled
		assert.Equal(t, "u", queryRelpersistence(t, ctx, pool, chunk.Name))
		assert.True(t, hasReloption(t, ctx, pool, chunk.Name, "autovacuum_enabled=false"))

		// Set chunk logged
		err = SetChunkLogged(ctx, pool, chunk.Name)
		require.NoError(t, err)

		// Verify: now LOGGED
		assert.Equal(t, "p", queryRelpersistence(t, ctx, pool, chunk.Name))

		// Verify: autovacuum re-enabled
		assert.True(t, hasReloption(t, ctx, pool, chunk.Name, "autovacuum_enabled=true"))
	})

	t.Run("nonexistent chunk returns error", func(t *testing.T) {
		err := SetChunkLogged(ctx, pool, "_timescaledb_internal.nonexistent_chunk")
		require.Error(t, err)
		assert.ErrorContains(t, err, "alter table")
	})
}
