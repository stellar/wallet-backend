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
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	dbConnectionPool, err := OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	n, err := Migrate(ctx, dbt.DSN, migrate.Up, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	ids := []string{}
	err = dbConnectionPool.SelectContext(ctx, &ids, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	wantIDs := []string{"2024-04-29.0-initial.sql"}
	assert.Equal(t, wantIDs, ids)
}

func TestMigrate_up_2_down_1(t *testing.T) {
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	dbConnectionPool, err := OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	n, err := Migrate(ctx, dbt.DSN, migrate.Up, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	ids := []string{}
	err = dbConnectionPool.SelectContext(ctx, &ids, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	wantIDs := []string{
		"2024-04-29.0-initial.sql",
		"2024-05-22.0-ingest.sql",
	}
	assert.Equal(t, wantIDs, ids)

	n, err = Migrate(ctx, dbt.DSN, migrate.Down, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	err = dbConnectionPool.SelectContext(ctx, &ids, "SELECT id FROM gorp_migrations")
	require.NoError(t, err)
	wantIDs = []string{
		"2024-04-29.0-initial.sql",
	}
	assert.Equal(t, wantIDs, ids)
}

func TestMigrate_upall_down_all(t *testing.T) {
	db := dbtest.OpenWithoutMigrations(t)
	defer db.Close()

	dbConnectionPool, err := OpenDBConnectionPool(db.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

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

	ctx := context.Background()

	n, err := Migrate(ctx, db.DSN, migrate.Up, 0)
	require.NoError(t, err)
	require.Equal(t, count, n)

	var migrationsCount int
	err = dbConnectionPool.GetContext(ctx, &migrationsCount, "SELECT COUNT(*) FROM gorp_migrations")
	require.NoError(t, err)
	assert.Equal(t, count, migrationsCount)

	n, err = Migrate(ctx, db.DSN, migrate.Down, count)
	require.NoError(t, err)
	require.Equal(t, count, n)

	err = dbConnectionPool.GetContext(ctx, &migrationsCount, "SELECT COUNT(*) FROM gorp_migrations")
	require.NoError(t, err)
	assert.Equal(t, 0, migrationsCount)
}
