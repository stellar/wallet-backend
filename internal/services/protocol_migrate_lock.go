package services

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
)

// One advisory lock per (protocol, strategy): a current-state run excludes
// other current-state runs for the same protocol; likewise for history. Live
// ingestion takes no lock — the per-ledger cursor CAS already decides which
// writer lands each ledger.
const (
	lockScopeCurrentState = "current-state"
	lockScopeHistory      = "history"
)

// dedupePreservingOrder removes duplicate protocol IDs, keeping first
// occurrences in order, so a repeated ID cannot double-acquire its own lock.
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

// migrateAdvisoryLockID derives the advisory lock key for (scope, protocol).
// The input string is the wire-level key: changing it silently stops
// excluding runs of older builds.
func migrateAdvisoryLockID(scope, protocolID string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-" + scope + "-" + protocolID))
	return int(h.Sum64())
}

// acquireMigrateLocks try-locks every protocol's lock for the given scope on
// one dedicated connection and returns the release func to defer. A held lock
// means another run of the same strategy owns that protocol — do not proceed.
func acquireMigrateLocks(ctx context.Context, pool *pgxpool.Pool, scope string, protocolIDs []string) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring a connection for %s locks: %w", scope, err)
	}

	// destroyConn ends the session, which frees any locks already taken —
	// Release() would instead hand a lock-holding connection back to the
	// pool. Detached from ctx (pgx refuses cancelled contexts) with a finite
	// deadline so a wedged session cannot block forever.
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
