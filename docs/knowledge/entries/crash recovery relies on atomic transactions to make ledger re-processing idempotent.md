---
context: No partial ledger state is possible; advisory lock prevents racing instances; on restart simply re-process from cursor+1; works for both live and backfill
type: insight
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# crash recovery relies on atomic transactions to make ledger re-processing idempotent

The ingestion pipeline's crash recovery strategy is deliberately simple: it relies on atomic transactions so that re-processing a ledger after a crash is always safe.

For live ingestion:
- On restart, reads `latestLedgerCursor` and resumes from `cursor + 1`
- Since each ledger's data is committed in a single atomic transaction (including the cursor advance as the final step), a crash mid-ledger leaves the cursor unchanged
- The same ledger is re-processed from scratch — this is safe because no partial state was committed

For backfill:
- Each batch atomically updates `oldestCursor` with its final flush
- If a batch crashes mid-processing, its cursor is not updated
- Gap detection on the next run finds the incomplete batch range and reprocesses it

The two safety mechanisms that make this work together:
1. **Atomic transactions** — no partial ledger state can exist in the database
2. **Advisory lock** — prevents two instances from processing the same ledger simultaneously (which would cause duplicate data)

The alternative design — tracking mid-ledger processing state with resumption points — would add significant complexity. The trade-off is that each restart re-processes the last incomplete ledger fully rather than resuming mid-way, which is cheap (one ledger ≈ a few hundred milliseconds of processing).

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[live ingestion persists each ledger in a single atomic database transaction]] — the atomicity guarantee
- [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — the concurrency safety
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — backfill's variant of crash recovery

Areas:
- [[entries/ingestion]]
