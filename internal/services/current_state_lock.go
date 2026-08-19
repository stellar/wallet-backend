package services

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
)

// currentStateAdvisoryLockID derives the per-protocol advisory lock key shared
// by every current-state writer that must be exclusive: the repair engine and
// the current-state migration. Live ingestion is deliberately not a party —
// repair is designed to run against it. Same fnv64a construction as the
// live-ingest lock (generateAdvisoryLockID).
func currentStateAdvisoryLockID(protocolID string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-current-state-" + protocolID))
	return int(h.Sum64())
}

// acquireCurrentStateLocks try-locks every protocol's current-state advisory
// lock on one dedicated connection held for the run, and returns the release
// function to defer. A held lock means another repair or current-state
// migration owns that protocol right now — the caller must not proceed.
//
// The lock covers only concurrent runs. The sequential hazard it cannot see:
// a failed current-state migration re-dispatched after a repair re-applies
// window deltas the repaired absolute value already contains. Operators
// restart such a migration from scratch instead of resuming it.
func acquireCurrentStateLocks(ctx context.Context, pool *pgxpool.Pool, protocolIDs []string) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring a connection for current-state locks: %w", err)
	}

	// destroyConn ends the session, which releases any locks already taken
	// server-side — no per-lock unwind bookkeeping. Release() instead would
	// hand a lock-holding connection back to the pool. Detached from ctx (a
	// shutdown signal cancels it first, and pgx refuses to run on a cancelled
	// context) with a finite deadline so a wedged session cannot block forever.
	destroyConn := func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockTimeout)
		defer cancel()
		if closeErr := conn.Hijack().Close(closeCtx); closeErr != nil {
			log.Ctx(ctx).Warnf("closing current-state lock connection: %v", closeErr)
		}
	}

	lockIDs := make([]int, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		lockID := currentStateAdvisoryLockID(pid)
		acquired, lockErr := db.AcquireAdvisoryLock(ctx, conn, lockID)
		if lockErr != nil {
			destroyConn()
			return nil, fmt.Errorf("acquiring current-state lock for protocol %q: %w", pid, lockErr)
		}
		if !acquired {
			destroyConn()
			return nil, fmt.Errorf("current-state lock for protocol %q is held: another repair or current-state migration is running", pid)
		}
		lockIDs = append(lockIDs, lockID)
	}

	release := func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockTimeout)
		defer cancel()
		for _, lockID := range lockIDs {
			if unlockErr := db.ReleaseAdvisoryLock(releaseCtx, conn, lockID); unlockErr != nil {
				log.Ctx(ctx).Errorf("releasing current-state lock, destroying connection to end its session: %v", unlockErr)
				destroyConn()
				return
			}
		}
		conn.Release()
	}
	return release, nil
}
