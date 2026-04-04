# Backfill Parallel Fetcher Design

**Date**: 2026-04-03
**Status**: Approved
**Problem**: The backfill pipeline achieves 60 ledgers/sec, bottlenecked by a single dispatcher goroutine calling `GetLedger` sequentially at 16.38ms/ledger.

## Context

The current backfill pipeline uses `optimizedStorageBackend` (forked from the SDK's `BufferedStorageBackend`) which was designed for sequential, single-consumer access. It internally uses:
- 15 S3 download workers that fetch files out-of-order
- A **priority queue** to re-order decoded batches
- A `batchQueue` that delivers batches in strict sequence
- A `GetLedger(seq)` API that enforces sequential consumption

The dispatcher goroutine sits on top, calling `GetLedger` in a loop and forwarding individual `LedgerCloseMeta` entries to `ledgerCh`. This sequential bottleneck limits throughput to ~61 ledgers/sec regardless of how many process/flush workers are configured.

**Key insight**: For backfill, ordering is unnecessary. Process workers handle ledgers independently. The watermark tracker handles out-of-order flushes. The priority queue and sequential `GetLedger` contract are pure overhead.

## Design: Backfill S3 Fetcher

Replace both the `optimizedStorageBackend` and the dispatcher with a new `backfillFetcher` component that downloads S3 files and pushes individual `LedgerCloseMeta` entries directly to `ledgerCh`.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                  backfillFetcher                     │
│                                                      │
│  taskCh (file-start sequences for the gap range)     │
│    ↓                                                 │
│  N S3 Workers ──download+decode──→ unpack batch      │
│                                     ↓                │
│                              push each LCM to        │
│                              external ledgerCh       │
└──────────────────────────┬──────────────────────────┘
                           │
                     ledgerCh (shared)
                           │
                    process workers (Stage 2)
```

### What's Removed (vs current pipeline)

| Component | Purpose | Why Unnecessary |
|-----------|---------|-----------------|
| Priority queue + lock | Re-order out-of-order S3 downloads | Process workers don't need order |
| `batchQueue` channel | Buffer ordered batches for `GetLedger` | No intermediate buffering needed |
| `GetLedger` sequential contract | Deliver one ledger at a time in order | Workers push directly to `ledgerCh` |
| `nextLedger` tracking | Enforce sequential consumption | No sequential contract |
| Dispatcher goroutine (`runDispatcher`) | Bridge backend → `ledgerCh` | Workers push directly |

### `backfillFetcher` Struct

```go
// backfillFetcher downloads S3 ledger files in parallel and fans out
// individual LedgerCloseMeta entries directly to an external channel.
// No ordering guarantees — designed for backfill where consumers
// handle ledgers independently.
type backfillFetcher struct {
    dataStore datastore.DataStore
    schema    datastore.DataStoreSchema
    config    BackfillFetcherConfig

    taskCh   chan uint32              // file-start sequences for workers
    ledgerCh chan<- xdr.LedgerCloseMeta // external channel owned by caller

    ctx    context.Context
    cancel context.CancelCauseFunc
    wg     sync.WaitGroup
}

type BackfillFetcherConfig struct {
    NumWorkers uint32
    RetryLimit uint32
    RetryWait  time.Duration
    GapStart   uint32
    GapEnd     uint32
}
```

### File Location

`internal/ingest/backfill_fetcher.go` — co-located with `storage_backend.go`.

### Worker Logic

Each worker:
1. Reads a file-start sequence from `taskCh`
2. Downloads and stream-decodes the S3 file (same `downloadAndDecode` logic as today)
3. Iterates over `batch.LedgerCloseMetas`
4. For each `LedgerCloseMeta` in the gap range, pushes to `ledgerCh`
5. Repeats until `taskCh` is closed or context cancelled

Retry logic: Same exponential backoff as `storageBuffer.downloadAndStore` — retries transient S3 errors, hard-fails on missing files in bounded ranges.

### Task Generation

A goroutine (or the `Run` method itself) computes file-start boundaries:
```go
for seq := schema.GetSequenceNumberStartBoundary(gap.GapStart); seq <= gap.GapEnd; seq += schema.LedgersPerFile {
    taskCh <- seq
}
close(taskCh)
```

This is simpler than the current `pushTaskQueue` replenishment pattern because the range is bounded and known up-front.

### Ledger Filtering

Each S3 file may contain ledgers outside the gap range (e.g., first/last file). Workers filter:
```go
for _, lcm := range batch.LedgerCloseMetas {
    seq := lcm.LedgerSequence()
    if seq < gap.GapStart || seq > gap.GapEnd {
        continue
    }
    ledgerCh <- lcm
}
```

### Stats Collection

Workers track fetch duration and channel wait times (same as current `backfillWorkerStats`). Stats are returned via a channel for aggregation into the gap summary log.

## Pipeline Integration

### Changes to `processGap`

**Before** (current):
```go
backend, err := m.ledgerBackendFactory(gapCtx)
backend.PrepareRange(gapCtx, BoundedRange(gap.GapStart, gap.GapEnd))
// ...
dispatcherStats := m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)
```

**After**:
```go
fetcher := ingest.NewBackfillFetcher(ingest.BackfillFetcherConfig{...}, dataStore, schema, ledgerCh)
fetcherStats := fetcher.Run(gapCtx, gapCancel)
// fetcher.Run closes ledgerCh when done (same contract as runDispatcher)
```

### New Dependencies on `ingestService`

The service needs access to `dataStore` and `schema` (currently encapsulated inside the backend). Options:
1. Store them on `ingestService` during construction (passed from `ingest.Configs`)
2. Create a `BackfillFetcherFactory` function (similar to `ledgerBackendFactory`)

Option 2 is cleaner — avoids exposing datastore internals to the service layer.

### New Config Field

`BackfillFetchWorkers` (default: 15) — number of S3 download goroutines. Exposed as a CLI flag.

### What Stays the Same

- `optimizedStorageBackend` — unchanged, still used for live ingestion
- `runProcessWorkers` — unchanged, reads from same `ledgerCh`
- `runFlushWorkers` — unchanged
- `backfillWatermark` — unchanged, handles out-of-order flushes
- `getLedgerWithRetry` — kept for live ingestion path
- `ledgerBackendFactory` — kept for `fetchLedgerCloseTime` (chunk pre-creation)
- `fetchLedgerCloseTime` — unchanged (only fetches 2 boundary ledgers)

### What's Removed

- `runDispatcher` — replaced by `backfillFetcher`
- Backend creation in `processGap` — replaced by fetcher creation

## Expected Performance

### Current Bottleneck
Single dispatcher: 61 ledgers/sec fetch → 60 ledgers/sec pipeline.

### After Change
S3 workers push directly. With 15 workers, fetch capacity is hundreds of ledgers/sec. **Bottleneck shifts to process workers** (CPU-bound XDR parsing at ~119ms/ledger).

### Scaling Targets

| Fetch Workers | Process Workers | Flush Workers | DB Conns Needed | Expected Throughput |
|--------------|----------------|---------------|-----------------|-------------------|
| 15 | 8 (current) | 2 (current) | 15 | ~67/sec (process-limited) |
| 15 | 16 | 4 | 25 | ~130/sec |
| 15 | 24 | 5 | 30 | ~200/sec |
| 15 | 32 | 8 | 45 | ~260/sec |

**2-4× improvement** depending on process/flush worker scaling.

## Verification

1. **Unit tests**: Test `backfillFetcher` with mock `DataStore` — verify all ledgers in gap range are pushed to `ledgerCh`, out-of-range ledgers are filtered, retries work, context cancellation stops workers.
2. **Integration**: Run backfill on a small gap (1000 ledgers) and verify:
   - All ledgers ingested (no gaps in DB)
   - Watermark cursor reaches gap end
   - Gap summary log shows fetch stats
3. **Performance**: Run on the same gap range (61602559-61637119) and compare ledgers/sec.
4. **Live ingestion unaffected**: Run `make unit-test` — no changes to live path.
