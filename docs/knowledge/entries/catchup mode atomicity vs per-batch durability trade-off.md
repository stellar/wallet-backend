---
context: Catchup: all-or-nothing final transaction (atomic, not incrementally resumable); historical backfill: per-batch cursor update (partially resumable, but individual batch granularity)
type: insight
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# catchup mode atomicity vs per-batch durability trade-off

The two backfill modes make opposite trade-offs on the spectrum between atomicity and incremental durability:

**Catchup mode** (all-or-nothing):
- All parallel batch results are collected in memory
- A single final database transaction applies everything: `processBatchChanges()` + `latestLedgerCursor` update
- **Benefit:** The cursor jump is atomic — `latestLedgerCursor` either advances fully to the new tip or doesn't advance at all
- **Cost:** Any batch failure aborts the entire catchup; not incrementally resumable

**Historical backfill** (per-batch durability):
- Each batch atomically updates `oldestCursor` with its final flush
- Completed batches are durable even if later batches fail
- **Benefit:** Partial progress is preserved; failed backfills don't lose completed work
- **Cost:** The oldest cursor advances incrementally across batches, not atomically as a whole

The design choice reflects the semantic difference between the two modes:
- Catchup's purpose is to advance `latestLedgerCursor` to a specific point so live ingestion can resume. An intermediate cursor position would leave live ingestion resuming mid-catchup, which is not a meaningful state.
- Historical backfill's purpose is to fill gaps over a potentially large ledger range. Incremental durability matters because a large backfill job can fail partway and should not restart from zero.

The tension becomes acute for very long catchup scenarios (service down for hours, hundreds of thousands of ledgers to catch up). In that case, the all-or-nothing behavior of catchup is costly. The designed remedy is the `catchupThreshold` — once you're far enough behind, the system arguably should treat the catchup as a historical backfill rather than a "live is paused" scenario.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup threshold triggers parallel backfill instead of sequential catchup after restart]] — the threshold that decides which mode to use
- [[catchup mode collects BatchChanges and merges them in a single atomic transaction at end]] — catchup's specific implementation
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — historical backfill's recovery model

Areas:
- [[entries/ingestion]]
