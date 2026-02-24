---
context: key-value table with upsert semantics; keys are cursor names (latestLedgerCursor, oldestLedgerCursor); IngestStoreModel wraps all access; Prometheus metrics are derived, not authoritative
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# ingest_store key-value table is the sole source of truth for cursor positions

All ingestion cursor state is stored in the `ingest_store` table — a simple key-value store with upsert semantics. `IngestStoreModel` is the only access path to this table.

The four operations on cursor state:

| Method | SQL | Used by |
|--------|-----|---------|
| `Get(cursorName)` | `SELECT value WHERE key = $1` | Startup: check first run, load cursor |
| `Update(cursorName, ledger)` | `INSERT ... ON CONFLICT DO UPDATE SET value = $2` | Live: advance `latestLedgerCursor` |
| `UpdateMin(cursorName, ledger)` | `UPDATE SET value = LEAST(value::integer, $2)` | Backfill: move `oldestLedgerCursor` backward |
| `GetLedgerGaps()` | Window function on `transactions` | Historical backfill: find gaps |

**Prometheus metrics are derived state**, not authoritative. On startup, cursor values are loaded from the DB into Prometheus. During live processing, metrics are updated periodically (every `oldestLedgerSyncInterval` = 100 ledgers for oldest, every ledger for latest). If the process crashes and restarts, it re-reads from the DB — the metrics restart from accurate values.

This design avoids split-brain between in-memory state and database state. If you need to know where ingestion actually is, query `ingest_store` directly. The Prometheus metrics are for observability, not correctness.

The upsert pattern for `Update()` (`INSERT ... ON CONFLICT DO UPDATE`) means the cursor can be written to even on first run without prior initialization — if no row exists, it is created.

---

A related job, `reconcile_oldest_cursor`, runs on the same cadence as the chunk interval (1 day). Its purpose: after TimescaleDB drops a chunk via retention policy, the `oldestLedgerCursor` value in `ingest_store` may reference ledgers that no longer exist. The reconcile job re-derives the actual oldest available ledger by querying `MIN(ledger_sequence)` from `transactions`, then updates `oldestLedgerCursor` to match. Without this job, the cursor would point to data that has been dropped, causing confusion in gap detection and oldest-ledger reporting. The cadence alignment with chunk interval ensures the cursor is reconciled no later than one chunk interval after a retention drop.

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[UpdateMin cursor pattern ensures oldest cursor only moves backward not forward]] — the UpdateMin operation detail
- [[oldest ledger sync interval decouples live and backfill cursor tracking]] — the sync interval that derives Prometheus metrics from DB state

Areas:
- [[entries/ingestion]]
