package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/support/log"
)

type ConnectionPool interface {
	SQLExecuter
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	Close() error
	Ping(ctx context.Context) error
	SqlDB(ctx context.Context) (*sql.DB, error)
	SqlxDB(ctx context.Context) (*sqlx.DB, error)
}

// Make sure *DBConnectionPoolImplementation implements DBConnectionPool:
var _ ConnectionPool = (*ConnectionPoolImplementation)(nil)

type ConnectionPoolImplementation struct {
	*sqlx.DB
}

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
)

func OpenDBConnectionPool(dataSourceName string) (ConnectionPool, error) {
	sqlxDB, err := sqlx.Open("postgres", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error creating app DB connection pool: %w", err)
	}
	sqlxDB.SetConnMaxIdleTime(MaxDBConnIdleTime)
	sqlxDB.SetMaxOpenConns(MaxOpenDBConns)

	err = sqlxDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging app DB connection pool: %w", err)
	}

	return &ConnectionPoolImplementation{DB: sqlxDB}, nil
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
