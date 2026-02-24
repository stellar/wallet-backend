---
context: FNV-64a hash of "wallet-backend-ingest-<network>" gives testnet/pubnet separate locks; uses pg_try_advisory_lock (non-blocking)
type: decision
status: current
subsystem: ingestion
areas: [ingestion, data-layer, postgresql]
created: 2026-02-24
---

# advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock

Live ingestion acquires a PostgreSQL advisory lock at startup using `pg_try_advisory_lock` with a deterministic FNV-64a hash of the string `"wallet-backend-ingest-<network>"`. The non-blocking variant means startup fails fast if another instance holds the lock rather than waiting indefinitely.

The FNV-64a hash of the network name is the key design choice here: testnet and pubnet get separate lock IDs, so running both networks simultaneously on the same database does not cause mutual exclusion. This is intentional — a shared database (staging environment, for example) can run both.

The advisory lock is held for the lifetime of the process. If the process crashes, PostgreSQL automatically releases the lock when the connection closes, allowing the next restart to acquire it immediately.

Since [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]], a crash-and-restart cycle is safe: the lock releases, the new process acquires it, and re-processes the last incomplete ledger from scratch.

This approach avoids the need for an external distributed lock (e.g., Redis) or a lease table — the database connection that holds the ingest advisory lock IS the lock.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — the advisory lock works because crash recovery is safe; together they form the concurrency safety model

Areas:
- [[entries/ingestion]]
