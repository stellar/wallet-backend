---
description: Batch 0 must complete before any compression; watermark advances as contiguous batches finish; final verification pass catches boundary chunks; only applies to historical backfill mode
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, backfill, timescaledb, compression, performance]
---

# Progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete

## Context

Historical backfill inserts data uncompressed (faster writes). After all batches complete, TimescaleDB can compress old chunks. But waiting for all batches to finish before compressing wastes time — chunks from early batches can be compressed while later batches are still running.

## Detail

The `progressiveRecompressor` runs concurrently with the backfill parallel worker pool. It compresses TimescaleDB chunks as contiguous completed batches form a prefix:

1. Batch 0 must complete first (establishes the starting watermark).
2. As batches 0, 1, 2, 3... complete in contiguous order (no gaps), the watermark advances.
3. Chunks whose time range falls entirely before the watermark become eligible for compression.
4. After all backfill batches complete, a final verification pass runs to catch any boundary chunks that weren't compressed during the progressive pass.

This applies only to historical backfill mode. Catchup mode (`BackfillModeCatchup`) does not use progressive recompression — it commits all changes in a single atomic transaction after all batches complete.

## Implications

- Non-contiguous batch completion (batch 0 done, batch 2 done, batch 1 still running) stalls the watermark until batch 1 finishes — preventing premature compression of data that might be written by batch 1.
- The recompressor is transparent to the backfill logic — it observes batch completion events.
- If a backfill is interrupted mid-run, some chunks will be uncompressed. The next backfill run will resume and compress remaining chunks on completion.

## Source

`internal/services/ingest_backfill.go:progressiveRecompressor`

## Related

Progressive recompression is possible only because [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — the reduced durability allows fast parallel writes that produce compressible chunks worth optimizing.

This applies only to historical backfill, not to the path triggered by [[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] — catchup commits atomically at the end rather than progressively compressing as batches complete.

relevant_notes:
  - "[[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — grounds this: the reduced-durability parallel write pattern creates the uncompressed chunks that progressive recompression targets"
  - "[[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] — contrasts with this: catchup mode does not use progressive recompression; compression only applies to historical backfill mode"
