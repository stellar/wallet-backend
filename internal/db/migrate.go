package db

import (
	"context"
	"fmt"
	"net/http"

	migrate "github.com/rubenv/sql-migrate"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db/migrations"
	"github.com/stellar/wallet-backend/internal/db/migrations/protocols"
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

// RunProtocolMigrations executes all embedded protocol SQL files to register
// protocols in the database. Protocol SQL files are idempotent.
func RunProtocolMigrations(ctx context.Context, dbPool *pgxpool.Pool) (int, error) {
	sqlDB := SQLDBFromPool(dbPool)
	defer sqlDB.Close() //nolint:errcheck // best-effort close of the sql.DB wrapper

	n, err := protocols.Run(ctx, sqlDB)
	if err != nil {
		return n, fmt.Errorf("running protocol migrations: %w", err)
	}
	return n, nil
}
