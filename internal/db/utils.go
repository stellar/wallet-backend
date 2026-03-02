package db

import (
	"context"
	"fmt"
)

// AcquireAdvisoryLock attempt to acquire an advisory lock on the provided lockKey, returns true if acquired, or false
// not.
func AcquireAdvisoryLock(ctx context.Context, q Querier, lockKey int) (bool, error) {
	var acquired bool
	sqlQuery := "SELECT pg_try_advisory_lock($1)"
	err := q.QueryRow(ctx, sqlQuery, lockKey).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("querying pg_try_advisory_lock(%v): %w", lockKey, err)
	}
	return acquired, nil
}

// ReleaseAdvisoryLock releases an advisory lock on the provided lockKey.
func ReleaseAdvisoryLock(ctx context.Context, q Querier, lockKey int) error {
	sqlQuery := "SELECT pg_advisory_unlock($1)"
	var released bool
	err := q.QueryRow(ctx, sqlQuery, lockKey).Scan(&released)
	if err != nil {
		return fmt.Errorf("executing pg_advisory_unlock(%v): %w", lockKey, err)
	}
	return nil
}
