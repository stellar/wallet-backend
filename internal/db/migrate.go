package db

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/stdlib"
	migrate "github.com/rubenv/sql-migrate"

	"github.com/stellar/wallet-backend/internal/db/migrations"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Migrate(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) (int, error) {
	dbConnectionPool, err := OpenDBConnectionPool(databaseURL)
	if err != nil {
		return 0, fmt.Errorf("connecting to the database: %w", err)
	}
	defer utils.DeferredClose(ctx, dbConnectionPool, "closing dbConnectionPool in the Migrate function")

	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	db := stdlib.OpenDBFromPool(dbConnectionPool.Pool())

	appliedMigrationsCount, err := migrate.ExecMax(db, "postgres", m, direction, count)
	if err != nil {
		return appliedMigrationsCount, fmt.Errorf("applying migrations: %w", err)
	}
	return appliedMigrationsCount, nil
}
