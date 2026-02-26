---
description: If networkLatest - cursor >= CatchupThreshold (default 100), startBackfilling in BackfillModeCatchup; avoids hours of sequential catch-up after a restart gap; configurable per deployment
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, backfill, catchup, performance, configuration]
---

# Catchup threshold triggers parallel backfill instead of sequential processing when service falls behind

## Context

When the ingestion service restarts after a gap (maintenance, crash), it may be many ledgers behind the network tip. Sequential live ingestion processes one ledger at a time — falling 1000 ledgers behind would take ~20 minutes to catch up at normal throughput.

## Detail

In `startLiveIngestion()`, after loading the cursor, the service calls `rpcService.GetHealth()` to get the current network ledger. If:

```
networkLatestLedger - latestLedgerCursor >= CatchupThreshold (default: 100)
```

Then instead of sequential processing, it calls `startBackfilling(ctx, cursor+1, networkLatest, BackfillModeCatchup)`. After the catchup completes (all batches parallel-processed and merged in a single atomic DB transaction), `startLedger` advances to `networkLatest + 1` and sequential live ingestion resumes.

`CatchupThreshold` is configurable via `--catchup-threshold`. Setting it to 0 would always use sequential processing regardless of lag; setting it to 1 would always use catchup mode on any gap.

## Implications

- The default 100 ledger threshold (approximately 8 minutes of Stellar time) means brief restarts won't trigger catchup mode — only longer gaps do.
- Catchup mode uses parallel backfill workers, so catch-up time scales with `--backfill-workers` (default: NumCPU). More workers = faster catchup.
- If catchup fails (any batch fails), the `latestLedgerCursor` is NOT advanced — the next restart will attempt catchup again for the same range.

## Source

`internal/services/ingest_live.go:startLiveIngestion()` — catchup threshold check
`internal/services/ingest_backfill.go:startBackfilling()` — BackfillModeCatchup path

## Related

Once catchup mode is triggered, [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] does NOT run — catchup mode commits all changes in a single atomic transaction after all batches complete rather than progressively compressing during the run.

relevant_notes:
  - "[[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] — contrasts with this: progressive recompression applies only to historical backfill mode, not to the catchup path this entry describes"
