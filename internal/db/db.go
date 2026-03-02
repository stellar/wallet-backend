package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
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
var (
	_ Querier = (*pgxpool.Pool)(nil)
	_ Querier = (pgx.Tx)(nil)
)

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
// Two session-level settings are applied to every connection in the pool:
//   - synchronous_commit=off (via DSN options, no privilege required)
//   - session_replication_role='replica' (via AfterConnect, disables FK constraint checks;
//     requires superuser or replication privilege on the DB user)
//
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

	// Set session_replication_role = 'replica' on every new connection to disable FK constraint
	// checking for faster bulk inserts. This must be done via AfterConnect because the setting
	// cannot be embedded in the DSN and pgxpool creates many connections — a one-shot pool.Exec
	// would only configure a single connection.
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET session_replication_role = 'replica'")
		if err != nil {
			return fmt.Errorf("setting session_replication_role on backfill connection: %w", err)
		}
		return nil
	}

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

// SQLDBFromPool returns a *sql.DB backed by the given pgx pool.
// This is only needed for libraries that require database/sql (e.g. sql-migrate).
func SQLDBFromPool(pool *pgxpool.Pool) *sql.DB {
	return stdlib.OpenDBFromPool(pool)
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

// QueryOne executes a query and scans the single result row into T.
// For struct types, it uses pgx named-field scanning (db tags).
// For scalar types (int, bool, string, etc.), it uses direct column scanning.
// Returns pgx.ErrNoRows if no row is found.
func QueryOne[T any](ctx context.Context, q Querier, query string, args ...any) (T, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("executing query: %w", err)
	}
	var scanner func(pgx.CollectableRow) (T, error)
	var t T
	if reflect.TypeOf(t).Kind() == reflect.Struct {
		scanner = pgx.RowToStructByNameLax[T]
	} else {
		scanner = pgx.RowTo[T]
	}
	result, err := pgx.CollectOneRow(rows, scanner)
	if err != nil {
		var zero T
		return zero, err //nolint:wrapcheck // pgx.ErrNoRows must propagate unwrapped for errors.Is checks
	}
	return result, nil
}

// QueryMany executes a query and scans all result rows into []T.
// For struct types, it uses pgx named-field scanning (db tags).
// For scalar types (int, bool, string, etc.), it uses direct column scanning.
// Returns an empty slice (not nil) if no rows are found.
func QueryMany[T any](ctx context.Context, q Querier, query string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	var scanner func(pgx.CollectableRow) (T, error)
	var t T
	if reflect.TypeOf(t).Kind() == reflect.Struct {
		scanner = pgx.RowToStructByNameLax[T]
	} else {
		scanner = pgx.RowTo[T]
	}
	results, err := pgx.CollectRows(rows, scanner)
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}
