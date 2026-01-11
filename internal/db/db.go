package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/stellar/go-stellar-sdk/support/log"
)

type ConnectionPool interface {
	SQLExecuter
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	Close() error
	Ping(ctx context.Context) error
	SqlDB(ctx context.Context) (*sql.DB, error)
	SqlxDB(ctx context.Context) (*sqlx.DB, error)
	PgxPool() *pgxpool.Pool
}

// Make sure *DBConnectionPoolImplementation implements DBConnectionPool:
var _ ConnectionPool = (*ConnectionPoolImplementation)(nil)

type ConnectionPoolImplementation struct {
	*sqlx.DB
	pgxPool *pgxpool.Pool
}

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
	MaxIdleDBConns    = 20              // Keep warm connections ready in the pool
	MaxDBConnLifetime = 5 * time.Minute // Recycle connections periodically
)

func OpenDBConnectionPool(dataSourceName string) (ConnectionPool, error) {
	sqlxDB, err := sqlx.Open("postgres", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error creating app DB connection pool: %w", err)
	}
	sqlxDB.SetConnMaxIdleTime(MaxDBConnIdleTime)
	sqlxDB.SetMaxOpenConns(MaxOpenDBConns)
	sqlxDB.SetMaxIdleConns(MaxIdleDBConns)
	sqlxDB.SetConnMaxLifetime(MaxDBConnLifetime)

	err = sqlxDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging app DB connection pool: %w", err)
	}

	// Create pgx pool for binary COPY operations
	pgxPool, err := pgxpool.New(context.Background(), dataSourceName)
	if err != nil {
		_ = sqlxDB.Close() //nolint:errcheck // Best effort cleanup; primary error is pgx pool creation
		return nil, fmt.Errorf("error creating pgx pool: %w", err)
	}

	return &ConnectionPoolImplementation{DB: sqlxDB, pgxPool: pgxPool}, nil
}

// OpenDBConnectionPoolForBackfill creates a connection pool optimized for bulk insert operations.
// It configures session-level settings (synchronous_commit=off) via the connection
// string, which are applied to every new connection in the pool.
// This should ONLY be used for backfill instances, NOT for live ingestion.
func OpenDBConnectionPoolForBackfill(dataSourceName string) (ConnectionPool, error) {
	// Append session parameters to connection string for automatic configuration.
	// URL-encoded: -c synchronous_commit=off
	backfillParams := "options=-c%20synchronous_commit%3Doff"

	separator := "?"
	if strings.Contains(dataSourceName, "?") {
		separator = "&"
	}
	backfillDSN := dataSourceName + separator + backfillParams

	sqlxDB, err := sqlx.Open("postgres", backfillDSN)
	if err != nil {
		return nil, fmt.Errorf("error creating backfill DB connection pool: %w", err)
	}
	sqlxDB.SetConnMaxIdleTime(MaxDBConnIdleTime)
	sqlxDB.SetMaxOpenConns(MaxOpenDBConns)
	sqlxDB.SetMaxIdleConns(MaxIdleDBConns)
	sqlxDB.SetConnMaxLifetime(MaxDBConnLifetime)

	err = sqlxDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging backfill DB connection pool: %w", err)
	}

	// Create pgx pool for binary COPY operations with backfill settings
	pgxPool, err := pgxpool.New(context.Background(), backfillDSN)
	if err != nil {
		_ = sqlxDB.Close() //nolint:errcheck // Best effort cleanup; primary error is pgx pool creation
		return nil, fmt.Errorf("error creating pgx pool for backfill: %w", err)
	}

	return &ConnectionPoolImplementation{DB: sqlxDB, pgxPool: pgxPool}, nil
}

// ConfigureBackfillSession sets session_replication_role to 'replica' which disables FK constraint
// checking. This cannot be set via connection string and requires elevated privileges (superuser
// or replication role). Call this ONCE at backfill startup after creating the connection pool.
func ConfigureBackfillSession(ctx context.Context, db ConnectionPool) error {
	_, err := db.ExecContext(ctx, "SET session_replication_role = 'replica'")
	if err != nil {
		return fmt.Errorf("setting session_replication_role: %w", err)
	}
	return nil
}

//nolint:wrapcheck // this is a thin layer on top of the sqlx.DB.BeginTxx method
func (db *ConnectionPoolImplementation) BeginTxx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	return db.DB.BeginTxx(ctx, opts)
}

//nolint:wrapcheck // this is a thin layer on top of the sqlx.DB.PingContext method
func (db *ConnectionPoolImplementation) Ping(ctx context.Context) error {
	return db.DB.PingContext(ctx)
}

func (db *ConnectionPoolImplementation) SqlDB(ctx context.Context) (*sql.DB, error) {
	return db.DB.DB, nil
}

func (db *ConnectionPoolImplementation) SqlxDB(ctx context.Context) (*sqlx.DB, error) {
	return db.DB, nil
}

func (db *ConnectionPoolImplementation) PgxPool() *pgxpool.Pool {
	return db.pgxPool
}

// Close closes both the sqlx and pgx pools.
//
//nolint:wrapcheck // this is a thin layer on top of the sqlx.DB.Close method
func (db *ConnectionPoolImplementation) Close() error {
	if db.pgxPool != nil {
		db.pgxPool.Close()
	}
	return db.DB.Close()
}

// Transaction is an interface that wraps the sqlx.Tx structs methods.
type Transaction interface {
	SQLExecuter
	Rollback() error
	Commit() error
}

// Make sure *sqlx.Tx implements DBTransaction:
var _ Transaction = (*sqlx.Tx)(nil)

// SQLExecuter is an interface that wraps the *sqlx.DB and *sqlx.Tx structs methods.
type SQLExecuter interface {
	DriverName() string
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	sqlx.PreparerContext
	sqlx.QueryerContext
	Rebind(query string) string
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// Make sure *sqlx.DB implements SQLExecuter:
var _ SQLExecuter = (*sqlx.DB)(nil)

// Make sure DBConnectionPool implements SQLExecuter:
var _ SQLExecuter = (ConnectionPool)(nil)

// Make sure *sqlx.Tx implements SQLExecuter:
var _ SQLExecuter = (*sqlx.Tx)(nil)

// Make sure DBTransaction implements SQLExecuter:
var _ SQLExecuter = (Transaction)(nil)

// RunInTransaction runs the given atomic function in an atomic database transaction and returns an error. Boilerplate
// code for database transactions.
func RunInTransaction(ctx context.Context, dbConnectionPool ConnectionPool, opts *sql.TxOptions, atomicFunction func(dbTx Transaction) error) error {
	// wrap the atomic function with a function that returns nil and an error so we can call RunInTransactionWithResult
	wrappedFunction := func(dbTx Transaction) (interface{}, error) {
		return nil, atomicFunction(dbTx)
	}

	_, err := RunInTransactionWithResult(ctx, dbConnectionPool, opts, wrappedFunction)
	return err
}

// RunInTransactionWithResult runs the given atomic function in an atomic database transaction and returns a result and
// an error. Boilerplate code for database transactions.
func RunInTransactionWithResult[T any](ctx context.Context, dbConnectionPool ConnectionPool, opts *sql.TxOptions, atomicFunction func(dbTx Transaction) (T, error)) (result T, err error) {
	dbTx, err := dbConnectionPool.BeginTxx(ctx, opts)
	if err != nil {
		return *new(T), fmt.Errorf("creating db transaction for RunInTransactionWithResult: %w", err)
	}

	defer func() {
		if err != nil {
			log.Ctx(ctx).Errorf("Rolling back transaction due to error: %v", err)
			errRollBack := dbTx.Rollback()
			if errRollBack != nil {
				log.Ctx(ctx).Errorf("Error in database transaction rollback: %v", errRollBack)
			}
		}
	}()

	result, err = atomicFunction(dbTx)
	if err != nil {
		return *new(T), fmt.Errorf("running atomic function in RunInTransactionWithResult: %w", err)
	}

	err = dbTx.Commit()
	if err != nil {
		return *new(T), fmt.Errorf("committing transaction in RunInTransactionWithResult: %w", err)
	}

	return result, nil
}

func RunInPgxTransaction(ctx context.Context, dbConnectionPool ConnectionPool, atomicFunction func(pgxTx pgx.Tx) error) error {
	pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning pgx transaction: %w", err)
	}

	defer func() {
		if err := pgxTx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Ctx(ctx).Errorf("error rolling back pgx transaction: %v", err)
		}
	}()

	if err := atomicFunction(pgxTx); err != nil {
		return fmt.Errorf("running atomic function in RunInPgxTransaction: %w", err)
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("committing pgx transaction: %w", err)
	}

	return nil
}
