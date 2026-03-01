package db

import (
	"context"
	"fmt"
	"net/http"

	migrate "github.com/rubenv/sql-migrate"

	"github.com/stellar/wallet-backend/internal/db/migrations"
)

func Migrate(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) (int, error) {
	pool, err := OpenDBConnectionPool(ctx, databaseURL)
	if err != nil {
		return 0, fmt.Errorf("connecting to the database: %w", err)
	}
	defer pool.Close()

	sqlDB := SQLDBFromPool(pool)
	defer sqlDB.Close() //nolint:errcheck // best-effort close of the sql.DB wrapper

	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}

	appliedMigrationsCount, err := migrate.ExecMax(sqlDB, "postgres", m, direction, count)
	if err != nil {
		return appliedMigrationsCount, fmt.Errorf("applying migrations: %w", err)
	}
	return appliedMigrationsCount, nil
}
