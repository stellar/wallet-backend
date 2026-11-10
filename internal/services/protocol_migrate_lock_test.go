package services

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
)

func TestMigrateAdvisoryLockID(t *testing.T) {
	// Golden values, not a self-comparison: the lock ID is a wire-level key.
	// Changing the prefix, the hash, or the cast lets a rolling deployment run
	// old and new binaries that take different locks for the same protocol and
	// wipe under each other. A failure here means the change is a breaking one.
	assert.Equal(t, 8825155305645790671, migrateAdvisoryLockID(lockScopeCurrentState, "SEP41"))
	assert.Equal(t, 3765383363378513446, migrateAdvisoryLockID(lockScopeHistory, "SEP41"))
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
		locks, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err)

		_, err = acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.ErrorContains(t, err, "is held")

		require.NoError(t, db.RunInTransaction(ctx, locks.conn, func(pgx.Tx) error { return nil }),
			"a live lock session must run a transaction")
		locks.release()

		locks2, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err, "release must free the lock for the next run")
		locks2.release()
	})

	t.Run("distinct protocols do not contend", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "SEP41")
		defer unhold()

		locks, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"BLEND"})
		require.NoError(t, err)
		locks.release()
	})

	t.Run("distinct scopes do not contend", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "SEP41")
		defer unhold()

		locks, err := acquireMigrateLocks(ctx, dbPool, lockScopeHistory, []string{"SEP41"})
		require.NoError(t, err, "a held current-state lock must not block a history run for the same protocol")
		locks.release()
	})

	t.Run("failing mid-list frees the locks already taken", func(t *testing.T) {
		unhold := holdLock(t, lockScopeCurrentState, "BLEND")
		defer unhold()

		// SEP41 is acquired first, then BLEND fails: the whole call must fail
		// AND free SEP41, or an aborted multi-protocol run would wedge every
		// protocol before the held one.
		_, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41", "BLEND"})
		require.ErrorContains(t, err, `protocol "BLEND" is held`)

		locks, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err, "the failed run must not leak SEP41's lock")
		locks.release()
	})

	t.Run("a transaction on the lock connection cannot commit after the session dies", func(t *testing.T) {
		locks, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err)
		defer locks.release()

		// Ending the lock session server-side is what a CNPG failover does: the
		// locks are gone, but this process has not seen the disconnect yet.
		var pid int
		require.NoError(t, locks.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
		_, err = dbPool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, locks.conn, func(pgx.Tx) error { return nil })
		require.Error(t, err, "a write fenced by the lock connection must fail once its session is gone")

		locks2, err := acquireMigrateLocks(ctx, dbPool, lockScopeCurrentState, []string{"SEP41"})
		require.NoError(t, err, "the dead session no longer holds the lock")
		locks2.release()
	})
}
