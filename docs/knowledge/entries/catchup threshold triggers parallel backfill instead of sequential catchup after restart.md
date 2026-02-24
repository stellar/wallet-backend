---
context: Default threshold is 100 ledgers; below threshold uses sequential live processing; above threshold switches to parallel BackfillModeCatchup then jumps startLedger to networkLatest+1
type: insight
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# catchup threshold triggers parallel backfill instead of sequential catchup after restart

When live ingestion restarts, it compares `networkLatest - latestLedgerCursor` against `CatchupThreshold` (default: 100). If the service is behind by fewer than 100 ledgers, it resumes sequential live processing from where it left off. If it is behind by 100 or more, it switches to a parallel backfill run in `BackfillModeCatchup` mode before resuming live processing.

The threshold exists because sequential catch-up at one ledger per iteration is too slow when the service has been down for an extended period (minutes, hours). Parallel backfill (multiple goroutines, each with their own backend) can catch up much faster.

After catchup completes, `startLedger` is set to `networkLatest + 1` — the cursor jumps past the range that was just backfilled. This means the live loop then starts fresh at the current network tip rather than re-processing the catchup range.

Since [[catchup mode atomicity vs per-batch durability trade-off]], catchup mode collects all batch changes and applies them in a single atomic transaction at the end. If any catchup batch fails, the entire catchup fails and `latestLedgerCursor` is NOT advanced. The next restart will attempt catchup again from the same position.

The practical implication: a service that restarts frequently within short windows (e.g., rolling deploys with < 100 ledger gap ≈ < 8 minutes) will always use sequential catch-up. A service that was down for a significant period will automatically use the faster parallel path.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup mode atomicity vs per-batch durability trade-off]] — catchup makes the all-or-nothing trade-off
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — historical backfill mode behaves differently than catchup on failure

Areas:
- [[entries/ingestion]]
