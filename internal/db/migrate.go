package db

import (
	"context"
	"fmt"
	"net/http"

	migrate "github.com/rubenv/sql-migrate"

	"github.com/stellar/wallet-backend/internal/db/migrations"
	"github.com/stellar/wallet-backend/internal/db/migrations/protocols"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Migrate(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) (int, error) {
	dbConnectionPool, err := OpenDBConnectionPool(databaseURL)
	if err != nil {
		return 0, fmt.Errorf("connecting to the database: %w", err)
	}
	defer utils.DeferredClose(ctx, dbConnectionPool, "closing dbConnectionPool in the Migrate function")

	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	db, err := dbConnectionPool.SqlDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching sql.DB: %w", err)
	}

	appliedMigrationsCount, err := migrate.ExecMax(db, dbConnectionPool.DriverName(), m, direction, count)
	if err != nil {
		return appliedMigrationsCount, fmt.Errorf("applying migrations: %w", err)
	}
	return appliedMigrationsCount, nil
}

// RunProtocolMigrations executes all embedded protocol SQL files to register
// protocols in the database. Protocol SQL files are idempotent.
func RunProtocolMigrations(ctx context.Context, dbPool ConnectionPool) (int, error) {
	sqlDB, err := dbPool.SqlDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching sql.DB for protocol migrations: %w", err)
	}
	n, err := protocols.Run(ctx, sqlDB)
	if err != nil {
		return n, fmt.Errorf("running protocol migrations: %w", err)
	}
	return n, nil
}
