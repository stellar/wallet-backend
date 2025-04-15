package db

import (
	"context"
	"fmt"
	"net/http"

	migrate "github.com/rubenv/sql-migrate"

	"github.com/stellar/wallet-backend/internal/db/migrations"
)

func Migrate(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) (int, error) {
	dbConnectionPool, err := OpenDBConnectionPool(databaseURL)
	if err != nil {
		return 0, fmt.Errorf("connecting to the database: %w", err)
	}
	defer dbConnectionPool.Close()

	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	db, err := dbConnectionPool.SqlDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching sql.DB: %w", err)
	}

	appliedMigrationsCount, err := migrate.ExecMax(db, dbConnectionPool.DriverName(), m, direction, count)
	if err != nil {
		return 0, fmt.Errorf("applying migrations: %w", err)
	}
	return appliedMigrationsCount, nil
}
