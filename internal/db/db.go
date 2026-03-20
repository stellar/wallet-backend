package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stellar/go-stellar-sdk/support/log"
)

const (
	DefaultMaxConnIdleTime time.Duration = 10 * time.Second
	DefaultMaxConns        int32         = 10
	DefaultMinConns        int32         = 5
	DefaultMaxConnLifetime time.Duration = 5 * time.Minute
)

// PoolConfig holds configurable pgxpool settings. Zero values fall back to Default* constants.
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	QueryExecMode   pgx.QueryExecMode // 0 = pgx default (CacheStatement)
}

// DefaultPoolConfig returns a PoolConfig populated with the default values.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConns:        DefaultMaxConns,
		MinConns:        DefaultMinConns,
		MaxConnLifetime: DefaultMaxConnLifetime,
		MaxConnIdleTime: DefaultMaxConnIdleTime,
	}
}

// resolvePoolConfig returns the first config provided, or DefaultPoolConfig() if none.
func resolvePoolConfig(configs []PoolConfig) PoolConfig {
	if len(configs) > 0 {
		return configs[0]
	}
	return DefaultPoolConfig()
}

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

func OpenDBConnectionPool(ctx context.Context, dataSourceName string, poolConfigs ...PoolConfig) (*pgxpool.Pool, error) {
	poolCfg := resolvePoolConfig(poolConfigs)
	cfg, err := pgxpool.ParseConfig(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("parsing DB connection string: %w", err)
	}
	cfg.MaxConns = poolCfg.MaxConns
	cfg.MinConns = poolCfg.MinConns
	cfg.MaxConnLifetime = poolCfg.MaxConnLifetime
	cfg.MaxConnIdleTime = poolCfg.MaxConnIdleTime
	if poolCfg.QueryExecMode != 0 {
		cfg.ConnConfig.DefaultQueryExecMode = poolCfg.QueryExecMode
	}

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
	result, err := pgx.CollectOneRow(rows, rowScanner[T]())
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
	results, err := pgx.CollectRows(rows, rowScanner[T]())
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}

// QueryManyPtrs executes a query and scans all result rows into []*T.
// For struct types, it uses pgx named-field scanning (db tags).
// For scalar types (int, bool, string, etc.), it uses direct column scanning.
// Returns an empty slice (not nil) if no rows are found.
func QueryManyPtrs[T any](ctx context.Context, q Querier, query string, args ...any) ([]*T, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	results, err := pgx.CollectRows(rows, rowPtrScanner[T]())
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	if results == nil {
		results = []*T{}
	}
	return results, nil
}

func rowScanner[T any]() func(pgx.CollectableRow) (T, error) {
	var t T
	if typ := reflect.TypeOf(t); typ != nil && typ.Kind() == reflect.Struct && hasDBTags(typ) {
		return pgx.RowToStructByNameLax[T]
	}
	return pgx.RowTo[T]
}

func rowPtrScanner[T any]() func(pgx.CollectableRow) (*T, error) {
	var t T
	if typ := reflect.TypeOf(t); typ != nil && typ.Kind() == reflect.Struct && hasDBTags(typ) {
		return pgx.RowToAddrOfStructByNameLax[T]
	}
	return pgx.RowToAddrOf[T]
}

// hasDBTags reports whether the given struct type has any fields with a "db" tag,
// recursing into embedded (anonymous) struct fields. This distinguishes model structs
// (which use RowToStructByNameLax) from scalar-like structs such as time.Time and
// uuid.UUID (which use RowTo).
func hasDBTags(t reflect.Type) bool {
	for i := range t.NumField() {
		f := t.Field(i)
		if _, ok := f.Tag.Lookup("db"); ok {
			return true
		}
		if f.Anonymous && f.Type.Kind() == reflect.Struct && hasDBTags(f.Type) {
			return true
		}
	}
	return false
}
