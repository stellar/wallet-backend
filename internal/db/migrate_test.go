package db

import (
	"context"
	"io/fs"
	"testing"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/db/migrations"
)

func TestMigrate_up_1(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	n, err := Migrate(ctx, dbt.DSN, migrate.Up, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	ids, err := QueryMany[struct {
		ID string `db:"id"`
	}](ctx, pool, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	wantIDs := []string{"2024-04-29.0-initial.sql"}
	var gotIDs []string
	for _, row := range ids {
		gotIDs = append(gotIDs, row.ID)
	}
	assert.Equal(t, wantIDs, gotIDs)
}

func TestMigrate_up_2_down_1(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	n, err := Migrate(ctx, dbt.DSN, migrate.Up, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	ids, err := QueryMany[struct {
		ID string `db:"id"`
	}](ctx, pool, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	wantIDs := []string{
		"2024-04-29.0-initial.sql",
		"2024-05-22.0-ingest.sql",
	}
	var gotIDs []string
	for _, row := range ids {
		gotIDs = append(gotIDs, row.ID)
	}
	assert.Equal(t, wantIDs, gotIDs)

	n, err = Migrate(ctx, dbt.DSN, migrate.Down, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	ids, err = QueryMany[struct {
		ID string `db:"id"`
	}](ctx, pool, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	gotIDs = nil
	for _, row := range ids {
		gotIDs = append(gotIDs, row.ID)
	}
	assert.Equal(t, []string{"2024-04-29.0-initial.sql"}, gotIDs)
}

func TestMigrate_upall_down_all(t *testing.T) {
	ctx := context.Background()
	db := dbtest.OpenWithoutMigrations(t)
	defer db.Close()

	pool, err := OpenDBConnectionPool(ctx, db.DSN)
	require.NoError(t, err)
	defer pool.Close()

	// Get number of files in the migrations directory
	var count int
	err = fs.WalkDir(migrations.FS, ".", func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if !d.IsDir() {
			count++
		}
		return nil
	})
	require.NoError(t, err)

	n, err := Migrate(ctx, db.DSN, migrate.Up, 0)
	require.NoError(t, err)
	require.Equal(t, count, n)

	row, err := QueryOne[struct {
		Count int `db:"count"`
	}](ctx, pool, "SELECT COUNT(*) AS count FROM gorp_migrations")
	require.NoError(t, err)
	assert.Equal(t, count, row.Count)

	n, err = Migrate(ctx, db.DSN, migrate.Down, count)
	require.NoError(t, err)
	require.Equal(t, count, n)

	row, err = QueryOne[struct {
		Count int `db:"count"`
	}](ctx, pool, "SELECT COUNT(*) AS count FROM gorp_migrations")
	require.NoError(t, err)
	assert.Equal(t, 0, row.Count)
}
