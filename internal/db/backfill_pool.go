package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BackfillMaxPgxConns is the maximum number of pgx connections for the backfill pool.
// This replaces the dual-pool setup (~38 conns) with a single pgx pool (15 conns).
const BackfillMaxPgxConns int32 = 15

// BackfillConnectionPool is a pgx-only ConnectionPool implementation for backfill mode.
// It eliminates the sqlx pool entirely since backfill only needs pgx for its hot path
// (COPY inserts, cursor updates, compression).
// Must be created with OpenBackfillPgxPool — zero value is not usable.
type BackfillConnectionPool struct {
	pool *pgxpool.Pool
}

// Compile-time check that BackfillConnectionPool implements ConnectionPool.
var _ ConnectionPool = (*BackfillConnectionPool)(nil)

// OpenBackfillPgxPool creates a pgx-only connection pool optimized for backfill ingestion.
// It appends synchronous_commit=off to the DSN and configures each connection with
// session_replication_role='replica' to disable FK constraint checking.
func OpenBackfillPgxPool(ctx context.Context, dataSourceName string, maxConns int32) (ConnectionPool, error) {
	// Append synchronous_commit=off for faster writes (same as the old backfill pool)
	backfillParams := "options=-c%20synchronous_commit%3Doff"
	separator := "?"
	if strings.Contains(dataSourceName, "?") {
		separator = "&"
	}
	backfillDSN := dataSourceName + separator + backfillParams

	config, err := pgxpool.ParseConfig(backfillDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing backfill pgx pool config: %w", err)
	}
	config.MaxConns = maxConns

	// Set session_replication_role on every connection, fixing the bug where
	// the old code set it on sqlx but all inserts went through pgx.
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET session_replication_role = 'replica'")
		if err != nil {
			return fmt.Errorf("setting session_replication_role: %w", err)
		}
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("creating backfill pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging backfill pgx pool: %w", err)
	}

	return &BackfillConnectionPool{pool: pool}, nil
}

// Pool returns the underlying pgx connection pool.
func (b *BackfillConnectionPool) Pool() *pgxpool.Pool {
	return b.pool
}

// Close closes the pgx pool.
func (b *BackfillConnectionPool) Close() error {
	b.pool.Close()
	return nil
}

// Ping checks pool connectivity.
func (b *BackfillConnectionPool) Ping(ctx context.Context) error {
	//nolint:wrapcheck // thin wrapper
	return b.pool.Ping(ctx)
}
