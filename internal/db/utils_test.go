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
		dbConnectionPool1, err := OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		lockAcquired, err := AcquireAdvisoryLock(ctx, dbConnectionPool1, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired, "should be able to acquire the lock")

		// Create another database pool
		dbConnectionPool2, err := OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool2.Close()
		lockAcquired2, err := AcquireAdvisoryLock(ctx, dbConnectionPool2, lockKey)
		require.NoError(t, err)
		require.False(t, lockAcquired2, "should not be able to acquire the lock since its already been acquired by dbConnectionPool1")

		// Close the original connection which releases the lock
		err = dbConnectionPool1.Close()
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// try to acquire the lock again
		lockAcquired2, err = AcquireAdvisoryLock(ctx, dbConnectionPool2, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired2, "should be able to acquire the lock since we called dbConnectionPool1.Close()")
	})

	t.Run("lock_acquired_can_be_released_on_ReleaseAdvisoryLock", func(t *testing.T) {
		dbConnectionPool1, err := OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool1.Close()
		lockAcquired, err := AcquireAdvisoryLock(ctx, dbConnectionPool1, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired, "should be able to acquire the lock")

		// Create another database pool
		dbConnectionPool2, err := OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool2.Close()
		lockAcquired2, err := AcquireAdvisoryLock(ctx, dbConnectionPool2, lockKey)
		require.NoError(t, err)
		require.False(t, lockAcquired2, "should not be able to acquire the lock since its already been acquired by dbConnectionPool1")

		// Release the lock
		err = ReleaseAdvisoryLock(ctx, dbConnectionPool1, lockKey)
		require.NoError(t, err)

		// try to acquire the lock again
		lockAcquired2, err = AcquireAdvisoryLock(ctx, dbConnectionPool2, lockKey)
		require.NoError(t, err)
		require.True(t, lockAcquired2, "should be able to acquire the lock since we called ReleaseAdvisoryLock")
	})
}
