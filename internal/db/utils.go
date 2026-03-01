package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AcquireAdvisoryLock attempt to acquire an advisory lock on the provided lockKey, returns true if acquired, or false
// not.
func AcquireAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, lockKey int) (bool, error) {
	var acquired bool
	sqlQuery := "SELECT pg_try_advisory_lock($1)"
	err := pool.QueryRow(ctx, sqlQuery, lockKey).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("querying pg_try_advisory_lock(%v): %w", lockKey, err)
	}
	return acquired, nil
}

// ReleaseAdvisoryLock releases an advisory lock on the provided lockKey.
func ReleaseAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, lockKey int) error {
	sqlQuery := "SELECT pg_advisory_unlock($1)"
	var released bool
	err := pool.QueryRow(ctx, sqlQuery, lockKey).Scan(&released)
	if err != nil {
		return fmt.Errorf("executing pg_advisory_unlock(%v): %w", lockKey, err)
	}
	return nil
}
