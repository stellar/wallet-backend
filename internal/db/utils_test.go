package db

import (
	"context"
	"crypto/rand"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func TestAdvisoryLockAndRelease(t *testing.T) {
	ctx := context.Background()
	// Creates a test database:
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	// Creates a database pool
	randBigInt, err := rand.Int(rand.Reader, big.NewInt(90000))
	require.NoError(t, err)
	lockKey := int(randBigInt.Int64())

	t.Run("lock_acquired_can_be_released_on_dbConnClose", func(t *testing.T) {
		pool1, err := OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)

		// Acquire on a pinned connection so the lock is tied to a specific session.
		conn1, err := pool1.Acquire(ctx)
		require.NoError(t, err)
		lockAcquired, err := AcquireAdvisoryLock(ctx, conn1, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired, "should be able to acquire the lock")

		// Create another database pool
		pool2, err := OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer pool2.Close()
		lockAcquired2, err := AcquireAdvisoryLock(ctx, pool2, lockKey)
		require.NoError(t, err)
		require.False(t, lockAcquired2, "should not be able to acquire the lock since its already been acquired by pool1")

		// Return and close — closing the underlying connection releases the advisory lock.
		conn1.Release()
		pool1.Close()
		time.Sleep(500 * time.Millisecond)

		// try to acquire the lock again
		lockAcquired2, err = AcquireAdvisoryLock(ctx, pool2, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired2, "should be able to acquire the lock since we called pool1.Close()")
	})

	t.Run("lock_acquired_can_be_released_on_ReleaseAdvisoryLock", func(t *testing.T) {
		pool1, err := OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer pool1.Close()

		// Acquire on a pinned connection so we can release on the same session.
		conn1, err := pool1.Acquire(ctx)
		require.NoError(t, err)
		defer conn1.Release()

		lockAcquired, err := AcquireAdvisoryLock(ctx, conn1, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired, "should be able to acquire the lock")

		// Create another database pool
		pool2, err := OpenDBConnectionPool(ctx, dbt.DSN)
		require.NoError(t, err)
		defer pool2.Close()
		lockAcquired2, err := AcquireAdvisoryLock(ctx, pool2, lockKey)
		require.NoError(t, err)
		require.False(t, lockAcquired2, "should not be able to acquire the lock since its already been acquired by pool1")

		// Release the lock on the same pinned connection that acquired it.
		err = ReleaseAdvisoryLock(ctx, conn1, lockKey)
		require.NoError(t, err)

		// try to acquire the lock again
		lockAcquired2, err = AcquireAdvisoryLock(ctx, pool2, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired2, "should be able to acquire the lock since we called ReleaseAdvisoryLock")
	})
}
