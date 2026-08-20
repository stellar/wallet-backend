package services

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
)

// Advisory-lock scopes serialize protocol-migrate runs per protocol and
// strategy: any current-state run (migration or rebuild) excludes other
// current-state runs for the same protocol, and likewise for history runs.
// Live ingestion is deliberately not a party — the per-ledger cursor CAS
// already decides which writer lands a given ledger.
const (
	lockScopeCurrentState = "current-state"
	lockScopeHistory      = "history"
)

// dedupePreservingOrder returns protocolIDs with duplicates removed, keeping
// first occurrences in order. Callers dedupe before locking so a repeated ID
// cannot double-acquire (and then double-release) its own lock.
func dedupePreservingOrder(protocolIDs []string) []string {
	seen := make(map[string]struct{}, len(protocolIDs))
	unique := make([]string, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		if _, dup := seen[pid]; !dup {
			seen[pid] = struct{}{}
			unique = append(unique, pid)
		}
	}
	return unique
}

// migrateAdvisoryLockID derives the per-protocol, per-scope advisory lock
// key. Same fnv64a construction as the live-ingest lock
// (generateAdvisoryLockID). The input string is the wire-level lock key —
// changing it silently stops excluding runs of older builds.
func migrateAdvisoryLockID(scope, protocolID string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-" + scope + "-" + protocolID))
	return int(h.Sum64())
}

// acquireMigrateLocks try-locks every protocol's advisory lock for the given
// scope on one dedicated connection held for the run, and returns the release
// function to defer. A held lock means another migration or rebuild of the
// same strategy owns that protocol right now — the caller must not proceed.
func acquireMigrateLocks(ctx context.Context, pool *pgxpool.Pool, scope string, protocolIDs []string) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring a connection for %s locks: %w", scope, err)
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
			log.Ctx(ctx).Warnf("closing %s lock connection: %v", scope, closeErr)
		}
	}

	lockIDs := make([]int, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		lockID := migrateAdvisoryLockID(scope, pid)
		acquired, lockErr := db.AcquireAdvisoryLock(ctx, conn, lockID)
		if lockErr != nil {
			destroyConn()
			return nil, fmt.Errorf("acquiring %s lock for protocol %q: %w", scope, pid, lockErr)
		}
		if !acquired {
			destroyConn()
			return nil, fmt.Errorf("%s lock for protocol %q is held: another migration or rebuild is running", scope, pid)
		}
		lockIDs = append(lockIDs, lockID)
	}

	release := func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockTimeout)
		defer cancel()
		for _, lockID := range lockIDs {
			if unlockErr := db.ReleaseAdvisoryLock(releaseCtx, conn, lockID); unlockErr != nil {
				log.Ctx(ctx).Errorf("releasing %s lock, destroying connection to end its session: %v", scope, unlockErr)
				destroyConn()
				return
			}
		}
		conn.Release()
	}
	return release, nil
}
