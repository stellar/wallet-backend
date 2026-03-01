package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// ConnectionPool is the minimal interface for database access.
// All query execution goes through Pool() directly.
type ConnectionPool interface {
	Pool() *pgxpool.Pool
	Close() error
	Ping(ctx context.Context) error
}

// PgxPool is the primary ConnectionPool implementation wrapping a pgxpool.Pool.
type PgxPool struct {
	pool *pgxpool.Pool
}

// Make sure *PgxPool implements ConnectionPool:
var _ ConnectionPool = (*PgxPool)(nil)

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
	MaxIdleDBConns    = 20              // Keep warm connections ready in the pool
	MaxDBConnLifetime = 5 * time.Minute // Recycle connections periodically
)

func OpenDBConnectionPool(dataSourceName string) (ConnectionPool, error) {
	config, err := pgxpool.ParseConfig(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("parsing pgx pool config: %w", err)
	}
	config.MaxConns = int32(MaxOpenDBConns)
	config.MinConns = int32(MaxIdleDBConns)
	config.MaxConnIdleTime = MaxDBConnIdleTime
	config.MaxConnLifetime = MaxDBConnLifetime

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("error creating app DB connection pool: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging app DB connection pool: %w", err)
	}

	return &PgxPool{pool: pool}, nil
}

func (db *PgxPool) Pool() *pgxpool.Pool {
	return db.pool
}

//nolint:wrapcheck // thin wrapper
func (db *PgxPool) Ping(ctx context.Context) error {
	return db.pool.Ping(ctx)
}

func (db *PgxPool) Close() error {
	db.pool.Close()
	return nil
}

// RunInPgxTransaction runs the given atomic function in an atomic database transaction.
func RunInPgxTransaction(ctx context.Context, dbConnectionPool ConnectionPool, atomicFunction func(pgxTx pgx.Tx) error) error {
	pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
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
