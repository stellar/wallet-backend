---
description: Live ingestion acquires PG advisory lock keyed on FNV-64a hash of "wallet-backend-ingest-<network>"; different networks get separate locks; crash = no commit = safe restart
type: decision
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, postgresql, concurrency, advisory-lock]
---

# Advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock

## Context

Running two live ingestion processes against the same database would cause duplicate ledger writes and cursor corruption. A distributed locking mechanism is needed that is automatically released on crash.

## Detail

`pg_try_advisory_lock(lockID)` is called at startup in `startLiveIngestion()`. The lock ID is a deterministic FNV-64a hash of the string `"wallet-backend-ingest-<network>"` (e.g. `"wallet-backend-ingest-testnet"`). Two consequences:

1. **Same-network exclusion**: only one live ingestion process per network can hold the lock at a time.
2. **Different-network isolation**: testnet and pubnet use different lock IDs and can run on the same DB host.

If the process crashes mid-ledger, no data is committed (PersistLedgerData runs in a single atomic DB transaction), and the advisory lock is automatically released by PostgreSQL when the session closes. The next restart re-acquires the lock and re-processes the same ledger.

## Implications

- Never run two `ingest` processes for the same network against the same DB — the second one will fail at startup with a clear error rather than silently corrupting data.
- Advisory locks are session-scoped: `pg_try_advisory_lock` (non-blocking) is used, not `pg_advisory_lock` (blocking). A failure to acquire returns an error immediately.

## Source

`internal/services/ingest_live.go:startLiveIngestion()` — advisory lock acquisition
`internal/services/ingest.go:generateAdvisoryLockID()` — FNV-64a hash computation

## Related

The advisory lock enables [[the database is the sole integration point between the serve and ingest processes]] to work safely — because all coordination goes through the DB, the lock ensures only one ingest process writes to it at a time, making the DB a reliable source of truth rather than a contention point.

relevant_notes:
  - "[[the database is the sole integration point between the serve and ingest processes]] — grounds this: the advisory lock is the safety mechanism that makes DB-only coordination viable; without it, multiple ingest writers would corrupt the single integration point"
