// Tests for the Blend current-state wipe.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func newWipeFixture(t *testing.T) (context.Context, *pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	return ctx, pool, func() {
		pool.Close()
		dbt.Close()
	}
}

// currentStateTables lists every blend_* table in the schema, in filenode-scan
// order.
func currentStateTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name LIKE 'blend\_%'
		ORDER BY table_name
	`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, tables)
	return tables
}

func filenodes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables []string) map[string]uint32 {
	t.Helper()
	nodes := make(map[string]uint32, len(tables))
	for _, table := range tables {
		var node uint32
		require.NoError(t, pool.QueryRow(ctx, `SELECT pg_relation_filenode($1)`, table).Scan(&node))
		nodes[table] = node
	}
	return nodes
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n))
	return n
}

// TestWipeCurrentState pins what the Blend rebuild's wipe clears and what it
// must leave alone.
//
// Coverage is asserted by filenode rather than by seeding all eleven tables:
// TRUNCATE rewrites a table's underlying file, so a changed filenode proves the
// wipe reached that table, and an unchanged one proves it did not. That catches
// a current-state table added later and forgotten here — which seeded rows,
// listing only the tables the test itself knows about, would not.
func TestWipeCurrentState(t *testing.T) {
	ctx, pool, cleanup := newWipeFixture(t)
	defer cleanup()

	// Classification rows. Nothing rebuilds these, so the wipe must not touch
	// them: without the WASM-to-protocol mapping the re-migration has no
	// contracts to fold.
	_, err := pool.Exec(ctx, `INSERT INTO protocol_wasms (wasm_hash) VALUES ('\x01')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO protocol_contracts (contract_id, wasm_hash) VALUES ('\x02', '\x01')`)
	require.NoError(t, err)

	// A representative current-state row, so the assertion below is about rows
	// disappearing and not only about files being rewritten.
	_, err = pool.Exec(ctx, `INSERT INTO blend_pools (pool_contract_id) VALUES ('\x03')`)
	require.NoError(t, err)

	tables := currentStateTables(t, ctx, pool)
	protectedTables := []string{"protocol_wasms", "protocol_contracts"}
	before := filenodes(t, ctx, pool, tables)
	protectedBefore := filenodes(t, ctx, pool, protectedTables)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, blend.WipeCurrentState(ctx, tx))
	})

	assert.Zero(t, countRows(t, ctx, pool, "blend_pools"))
	assert.Equal(t, 1, countRows(t, ctx, pool, "protocol_wasms"))
	assert.Equal(t, 1, countRows(t, ctx, pool, "protocol_contracts"))

	after := filenodes(t, ctx, pool, tables)
	for _, table := range tables {
		assert.NotEqual(t, before[table], after[table], "%s is a current-state table the wipe left behind", table)
	}
	assert.Equal(t, protectedBefore, filenodes(t, ctx, pool, protectedTables), "the wipe must not touch classification tables")
}
