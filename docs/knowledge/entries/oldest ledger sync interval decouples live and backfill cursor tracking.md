---
context: Live process reads oldestLedgerCursor from DB every 100 ledgers (oldestLedgerSyncInterval); picks up progress from concurrent backfill without tight coupling
type: pattern
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# oldest ledger sync interval decouples live and backfill cursor tracking

Live ingestion and backfill can run concurrently — live follows the current chain tip while backfill fills historical gaps. They must both track the `oldestLedgerCursor` (the earliest ledger with complete data), but they must do so without tight coupling.

The decoupling mechanism: every `oldestLedgerSyncInterval` (100) ledgers processed by live ingestion, it reads `oldestLedgerCursor` from the database and syncs the value into Prometheus metrics. This picks up any progress made by a concurrently running backfill job.

Live ingestion does NOT hold a lock on the cursor — it just periodically reads what backfill has written. The interval of 100 ledgers (≈ 8 minutes at normal Stellar cadence of 5s/ledger) is a deliberate trade-off between freshness of the Prometheus metric and the cost of a database read per ledger.

Backfill writes to `oldestLedgerCursor` via `UpdateMin()` — only moving it backward (to earlier ledgers), never forward. Live ingestion moves `latestLedgerCursor` forward. The two cursors track orthogonal directions.

The practical implication: the Prometheus metric for `oldestLedgerCursor` in the live process lags actual backfill progress by up to 100 live ledgers ≈ 8 minutes. For monitoring purposes, the database value is always authoritative.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[UpdateMin cursor pattern ensures oldest cursor only moves backward not forward]] — the DB operation that backfill uses to write progress
- [[ingest_store key-value table is the sole source of truth for cursor positions]] — where cursor state lives

Areas:
- [[entries/ingestion]]
