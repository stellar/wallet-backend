package services

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
)

func TestMigrateAdvisoryLockID(t *testing.T) {
	assert.Equal(t, migrateAdvisoryLockID(lockScopeCurrentState, "SEP41"), migrateAdvisoryLockID(lockScopeCurrentState, "SEP41"),
		"the lock ID must be deterministic — every writer must derive the same key for a protocol")
	assert.NotEqual(t, migrateAdvisoryLockID(lockScopeCurrentState, "SEP41"), migrateAdvisoryLockID(lockScopeCurrentState, "BLEND"),
		"different protocols must map to different locks so their runs don't contend")
	assert.NotEqual(t, migrateAdvisoryLockID(lockScopeCurrentState, "SEP41"), migrateAdvisoryLockID(lockScopeHistory, "SEP41"),
		"the two strategies must map to different locks so a history run doesn't block a current-state run")
}

func TestAcquireMigrateLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbPool, _ := setupTestDB(t)

	// holdLock takes a protocol's lock on a raw connection, as a concurrent
	// migration or rebuild in another process would.
	holdLock := func(t *testing.T, scope, protocolID string) func() {
		t.Helper()
		conn, err := dbPool.Acquire(ctx)
		require.NoError(t, err)
		lockID := migrateAdvisoryLockID(scope, protocolID)
		acquired, err := db.AcquireAdvisoryLock(ctx, conn, lockID)
		require.NoError(t, err)
		require.True(t, acquired)
		return func() {
			require.NoError(t, db.ReleaseAdvisoryLock(context.Background(), conn, lockID))
			conn.Release()
		}
	}

	t.Run("acquire, refuse while held, release, reacquire", func(t *testing.T) {
		release, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err)

		_, err = acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.ErrorContains(t, err, "is held")

		release()

		release2, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err, "release must free the lock for the next run")
		release2()
	})

	t.Run("distinct protocols do not contend", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "SEP41")
		defer unhold()

		release, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"BLEND"})
		require.NoError(t, err)
		release()
	})

	t.Run("distinct scopes do not contend", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "SEP41")
		defer unhold()

		release, err := acquireMigrateLocks(ctx, dbPool, lockScopeHistory, []string{"SEP41"})
		require.NoError(t, err, "a held current-state lock must not block a history run for the same protocol")
		release()
	})

	t.Run("failing mid-list frees the locks already taken", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "BLEND")
		defer unhold()

		// SEP41 is acquired first, then BLEND fails: the whole call must fail
		// AND free SEP41, or an aborted multi-protocol run would wedge every
		// protocol before the held one.
		_, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41", "BLEND"})
		require.ErrorContains(t, err, `protocol "BLEND" is held`)

		release, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err, "the failed run must not leak SEP41's lock")
		release()
	})
}
