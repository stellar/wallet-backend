---
context: BackfillDBConnectionPool appends synchronous_commit=off to the DATABASE_URL before opening sqlx.DB; live pool does not; this means backfill gets async WAL for all connections automatically
type: insight
created: 2026-02-24
---

# backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints

`synchronous_commit = off` is a PostgreSQL session parameter that allows the server to acknowledge a transaction commit before the WAL record is flushed to disk. This trades a small durability window (up to `wal_writer_delay`, typically ~200ms) for substantially higher write throughput — no client waits for disk I/O per transaction.

For historical backfill, this trade-off is acceptable: if the process crashes, the gap detection mechanism will re-identify and re-process incomplete ledgers. The data is re-derivable from the Stellar history archive.

The implementation appends `?options=-c synchronous_commit%3Doff` (or equivalent) directly to the DATABASE_URL before opening the `sqlx.DB` pool. This means every connection in the pool inherits `synchronous_commit=off` as a session default from the moment it is opened. There is no need to issue `SET synchronous_commit = off` before each query or transaction.

Contrast with `session_replication_role = replica` (also used for backfill), which cannot be set via connection string and requires an explicit `SET` command after each connection is established — handled by `ConfigureBackfillSession()`.

The live ingestion pool does not set `synchronous_commit=off`. Live ingestion writes into the main operational dataset where crash recovery requires the atomic guarantees of synchronous WAL — a crash without this cannot be cleanly recovered by gap detection alone.

---

Relevant Notes:
- [[ConfigureBackfillSession disables FK constraint checking via session_replication_role replica because it cannot be set at connection-string level]] — the complementary backfill session configuration that must use a different mechanism
- [[dual connection pool uses sqlx for struct scanning and pgxpool for binary COPY because they serve different wire protocol needs]] — the pool architecture in which this setting lives

Areas:
- [[entries/data-layer]]
