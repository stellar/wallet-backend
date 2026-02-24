---
context: Both modes call processLedger() → Indexer.ProcessLedgerTransactions(); difference is only in ledger source (LedgerBackend) and result persistence (PersistLedgerData vs flushBatchBuffer)
type: insight
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# live ingestion and backfill share a common processLedger path through the Indexer

Despite having different startup logic, different worker pool configurations, different persistence strategies, and different cursor management, live ingestion and backfill share the exact same core processing path:

```
getLedgerWithRetry() → processLedger() → Indexer.ProcessLedgerTransactions()
```

The `processLedger()` function is called identically in both `ingestLiveLedgers()` and the backfill batch loop. It takes a `LedgerCloseMeta` and routes it through the Indexer's processor fan-out, returning an `IndexerBuffer` with the processed results.

**What differs between modes:**
- **Source:** live uses an unbounded `PrepareRange` on one backend; backfill uses a bounded range per batch with per-goroutine backends
- **Persistence:** live calls `PersistLedgerData()` (full atomic transaction); backfill calls `flushBatchBufferWithRetry()` (batched flush, cursor deferred)
- **Cursor updates:** live advances `latestLedgerCursor` per ledger; backfill uses `UpdateMin` on `oldestLedgerCursor` at batch end

**What is identical:** the entire Indexer processing path — all nine processors, all buffer accumulation, all deduplication, all state change production. A bug in processor logic affects both modes equally. A fix to the Indexer applies to both modes simultaneously.

This shared path means the Indexer is the natural seam for testing and mocking. Unit tests that exercise `processLedger()` cover both live and backfill behavior.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — what processLedger delegates to
- [[live ingestion persists each ledger in a single atomic database transaction]] — the live persistence path
- [[backfill batch size and DB insert batch size are separate concerns for memory bounding]] — the backfill persistence path

Areas:
- [[entries/ingestion]]
