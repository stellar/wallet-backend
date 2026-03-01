package db

import (
	"context"
	"fmt"
)

// AcquireAdvisoryLock attempt to acquire an advisory lock on the provided lockKey, returns true if acquired, or false
// not.
func AcquireAdvisoryLock(ctx context.Context, dbConnectionPool ConnectionPool, lockKey int) (bool, error) {
	tssAdvisoryLockAcquired := false
	sqlQuery := "SELECT pg_try_advisory_lock($1)"
	err := dbConnectionPool.Pool().QueryRow(ctx, sqlQuery, lockKey).Scan(&tssAdvisoryLockAcquired)
	if err != nil {
		return false, fmt.Errorf("querying pg_try_advisory_lock(%v): %w", lockKey, err)
	}
	return tssAdvisoryLockAcquired, nil
}

// ReleaseAdvisoryLock releases an advisory lock on the provided lockKey.
func ReleaseAdvisoryLock(ctx context.Context, dbConnectionPool ConnectionPool, lockKey int) error {
	sqlQuery := "SELECT pg_advisory_unlock($1)"
	_, err := dbConnectionPool.Pool().Exec(ctx, sqlQuery, lockKey)
	if err != nil {
		return fmt.Errorf("executing pg_advisory_unlock(%v): %w", lockKey, err)
	}
	return nil
}
