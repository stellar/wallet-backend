package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stellar/go-stellar-sdk/support/log"
)

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
	MaxIdleDBConns    = 20              // Keep warm connections ready in the pool
	MaxDBConnLifetime = 5 * time.Minute // Recycle connections periodically
)

// Querier is the minimal interface shared by *pgxpool.Pool and pgx.Tx.
// It allows QueryOne/QueryMany to work with both pool and transaction.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Compile-time checks that *pgxpool.Pool and pgx.Tx satisfy Querier.
var _ Querier = (*pgxpool.Pool)(nil)
var _ Querier = (pgx.Tx)(nil)

func OpenDBConnectionPool(ctx context.Context, dataSourceName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("parsing DB connection string: %w", err)
	}
	cfg.MaxConns = MaxOpenDBConns
	cfg.MinConns = MaxIdleDBConns
	cfg.MaxConnLifetime = MaxDBConnLifetime
	cfg.MaxConnIdleTime = MaxDBConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating app DB connection pool: %w", err)
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging app DB connection pool: %w", err)
	}

	return pool, nil
}

// OpenDBConnectionPoolForBackfill creates a connection pool optimized for bulk insert operations.
// It configures session-level settings (synchronous_commit=off) via the connection
// string, which are applied to every new connection in the pool.
// This should ONLY be used for backfill instances, NOT for live ingestion.
func OpenDBConnectionPoolForBackfill(ctx context.Context, dataSourceName string) (*pgxpool.Pool, error) {
	// Append session parameters to connection string for automatic configuration.
	// URL-encoded: -c synchronous_commit=off
	backfillParams := "options=-c%20synchronous_commit%3Doff"

	separator := "?"
	if strings.Contains(dataSourceName, "?") {
		separator = "&"
	}
	backfillDSN := dataSourceName + separator + backfillParams

	cfg, err := pgxpool.ParseConfig(backfillDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing backfill DB connection string: %w", err)
	}
	cfg.MaxConns = MaxOpenDBConns
	cfg.MinConns = MaxIdleDBConns
	cfg.MaxConnLifetime = MaxDBConnLifetime
	cfg.MaxConnIdleTime = MaxDBConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating backfill DB connection pool: %w", err)
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging backfill DB connection pool: %w", err)
	}

	return pool, nil
}

// SqlDBFromPool returns a *sql.DB backed by the given pgx pool.
// This is only needed for libraries that require database/sql (e.g. sql-migrate).
func SqlDBFromPool(pool *pgxpool.Pool) *sql.DB {
	return stdlib.OpenDBFromPool(pool)
}

// ConfigureBackfillSession sets session_replication_role to 'replica' which disables FK constraint
// checking. This cannot be set via connection string and requires elevated privileges (superuser
// or replication role). Call this ONCE at backfill startup after creating the connection pool.
func ConfigureBackfillSession(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "SET session_replication_role = 'replica'")
	if err != nil {
		return fmt.Errorf("setting session_replication_role: %w", err)
	}
	return nil
}

// RunInTransaction runs the given atomic function in a pgx transaction.
// It automatically rolls back on error and commits on success.
func RunInTransaction(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	pgxTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning pgx transaction: %w", err)
	}

	defer func() {
		if err := pgxTx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Ctx(ctx).Errorf("error rolling back pgx transaction: %v", err)
		}
	}()

	if err := fn(pgxTx); err != nil {
		return fmt.Errorf("running atomic function in RunInTransaction: %w", err)
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("committing pgx transaction: %w", err)
	}

	return nil
}

// QueryOne executes a query and scans the single result row into T using pgx struct tag scanning.
// Returns pgx.ErrNoRows if no row is found.
func QueryOne[T any](ctx context.Context, q Querier, query string, args ...any) (T, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("executing query: %w", err)
	}
	result, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		var zero T
		return zero, err //nolint:wrapcheck // pgx.ErrNoRows must propagate unwrapped for errors.Is checks
	}
	return result, nil
}

// QueryMany executes a query and scans all result rows into []T using pgx struct tag scanning.
// Returns an empty slice (not nil) if no rows are found.
func QueryMany[T any](ctx context.Context, q Querier, query string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	results, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}
