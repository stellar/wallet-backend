---
context: backfillBatchSize=250 (ledgers per parallel batch); backfillDBInsertBatchSize=50 (flush-to-DB frequency within a batch); inner batch size bounds peak memory in IndexerBuffer
type: insight
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# backfill batch size and DB insert batch size are separate concerns for memory bounding

Backfill has two size parameters that are often conflated but serve distinct purposes:

**`backfillBatchSize` (default: 250):** Controls how many ledgers are in each parallel batch assigned to a worker goroutine. This is the unit of work distribution — the `pond.Pool` splits the total ledger range into 250-ledger chunks, each assigned to one goroutine.

**`backfillDBInsertBatchSize` (default: 50):** Controls how often a worker goroutine flushes accumulated data to the database within its 250-ledger batch. After processing 50 ledgers, the worker calls `flushBatchBufferWithRetry()` and then `buffer.Clear()` to free the accumulated buffer state.

The inner batch size directly bounds peak memory usage. Without it, a 250-ledger batch would accumulate all state changes, all transaction data, and all XDR structs for all 250 ledgers in the `IndexerBuffer` before any writes. With the 50-ledger flush interval, the buffer peak is bounded to ~50 ledgers' worth of data.

The cursor is NOT updated during mid-batch flushes (no-op for cursor in `flushBatchBufferWithRetry`). Only the final flush at the end of the 250-ledger batch updates the `oldestCursor` via `UpdateMin()`. This preserves the atomic-completion invariant for gap detection.

Tuning guidance: increasing `backfillDBInsertBatchSize` allows more data to accumulate before flushing (fewer DB round-trips, higher throughput) at the cost of higher memory usage. Decreasing it reduces memory at the cost of more frequent writes.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill]] — what happens when the buffer is flushed
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — why mid-batch flushes don't update the cursor

Areas:
- [[entries/ingestion]]
