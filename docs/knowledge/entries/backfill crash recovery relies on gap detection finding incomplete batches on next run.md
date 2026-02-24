---
context: Batches atomically update oldestCursor only on success; failed batches leave gaps; GetLedgerGaps finds them on next run; catchup failure does NOT advance latestLedgerCursor
type: insight
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# backfill crash recovery relies on gap detection finding incomplete batches on next run

Backfill's crash recovery strategy differs from live ingestion's. Rather than tracking explicit failure state, it relies on the invariant that completed batches durably record their progress, and gap detection finds anything that isn't recorded.

**Historical backfill:** Each batch's final flush atomically updates the `oldestCursor` via `UpdateMin()`. If a batch crashes mid-processing, its cursor update never happens. On the next backfill run, `GetLedgerGaps()` finds the missing ledger range and creates new batches to cover it.

**Catchup mode:** Even more conservative — if any single batch fails, the entire catchup fails and `latestLedgerCursor` is NOT advanced. The next restart sees the same gap as before and retries the entire catchup range from scratch.

The practical implication: historical backfill is partially resumable (completed batches are durable, only failed batches retry). Catchup is fully idempotent but not resumable (any failure restarts the whole catchup range).

**What this means for operations:** A partial backfill run is safe to interrupt. You can stop a historical backfill mid-run, and the next run will automatically pick up from where progress was made. There is no cleanup required. However, repeatedly failing catchup will keep retrying the same range — investigate the root cause rather than expecting eventual success from retries alone.

Since [[catchup mode atomicity vs per-batch durability trade-off]], this all-or-nothing failure mode is the cost of catchup's atomicity benefit.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — live ingestion's crash recovery model
- [[backfill uses gap detection via window function on transactions ledger_number]] — the mechanism that finds incomplete batches
- [[UpdateMin cursor pattern ensures oldest cursor only moves backward not forward]] — the cursor operation that records batch completion

Areas:
- [[entries/ingestion]]
