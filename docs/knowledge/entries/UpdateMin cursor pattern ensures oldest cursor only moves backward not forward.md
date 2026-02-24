---
context: SQL: UPDATE ingest_store SET value = LEAST(value::integer, $2); backfill batches can complete out-of-order; LEAST ensures cursor reflects actual oldest ledger regardless of completion order
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# UpdateMin cursor pattern ensures oldest cursor only moves backward not forward

The `IngestStoreModel.UpdateMin()` method updates the `oldestLedgerCursor` using PostgreSQL's `LEAST()` function:

```sql
UPDATE ingest_store SET value = LEAST(value::integer, $2) WHERE key = $1
```

This ensures the cursor can only move to smaller (older) ledger numbers, never to larger ones.

**Why this matters for parallel backfill:** Backfill batches process in parallel and can complete in any order. If batch 5 (ledgers 1250-1500) completes before batch 3 (ledgers 750-1000), a naive update would advance the cursor to ledger 1500 even though ledgers 750-1000 aren't ingested yet. `UpdateMin()` prevents this: the cursor stays at its initial value until a batch with a lower ledger range updates it, and it can only move downward to reflect contiguous coverage from the oldest ingested point.

**The invariant maintained:** `oldestLedgerCursor` always points to the earliest ledger number for which we have complete, committed data. It is never an optimistic claim about future completions.

**For live ingestion**: Uses a different operation (`Update()`) for `latestLedgerCursor` — simple upsert with the new ledger number, which can only advance forward.

The asymmetry between `Update()` (forward-only, for latest cursor) and `UpdateMin()` (backward-only, for oldest cursor) mirrors the semantic difference: latest ingestion is always sequential, oldest ingestion grows incrementally inward from the beginning.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[ingest_store key-value table is the sole source of truth for cursor positions]] — the table this operates on
- [[oldest ledger sync interval decouples live and backfill cursor tracking]] — how live ingestion consumes this cursor value
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — cursor not advancing is what gap detection detects

Areas:
- [[entries/ingestion]]
