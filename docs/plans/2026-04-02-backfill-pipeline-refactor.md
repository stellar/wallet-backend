# Backfill Pipeline Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the batch-per-backend backfill with a 3-stage pipelined architecture (dispatcher → process workers → flush workers) for maximum throughput.

**Architecture:** Single `optimizedStorageBackend` per gap, fan-out to N process workers via bounded channel, M flush workers with 5 parallel COPYs each, watermark-based cursor tracking. Gaps processed sequentially.

**Tech Stack:** Go channels, `sync.WaitGroup`, `errgroup`, `pond.Pool` (shared indexer pool), `pgxpool`

**Design Doc:** `docs/plans/2026-04-02-backfill-pipeline-refactor-design.md`

---

## Task 1: Add New Config Fields to `ingestService`

**Files:**
- Modify: `internal/services/ingest.go:46-79` (IngestServiceConfig)
- Modify: `internal/services/ingest.go:99-121` (ingestService struct)
- Modify: `internal/services/ingest.go:123-161` (NewIngestService)

**Step 1: Add config fields**

Add new fields to `IngestServiceConfig`:

```go
// === Backfill Tuning ===
BackfillWorkers           int
BackfillBatchSize         int
BackfillDBInsertBatchSize int
BackfillProcessWorkers    int // NEW: Stage 2 workers (default: NumCPU)
BackfillFlushWorkers      int // NEW: Stage 3 flush workers (default: 4)
BackfillLedgerChanSize    int // NEW: ledgerCh buffer size (default: 256)
BackfillFlushChanSize     int // NEW: flushCh buffer size (default: 8)
```

**Step 2: Add fields to `ingestService` struct**

Replace `backfillPool`, `backfillBatchSize`, `backfillDBInsertBatchSize` with:

```go
// Backfill pipeline config
backfillProcessWorkers    int
backfillFlushWorkers      int
backfillFlushBatchSize    uint32
backfillLedgerChanSize    int
backfillFlushChanSize     int
```

Note: `backfillBatchSize` (ledgers per backend range) is no longer needed since we use 1 backend per gap. `backfillDBInsertBatchSize` is renamed to `backfillFlushBatchSize` for clarity.

**Step 3: Update `NewIngestService`**

Remove the `backfillPool` creation. Apply defaults for new fields:

```go
processWorkers := cfg.BackfillProcessWorkers
if processWorkers <= 0 {
    processWorkers = runtime.NumCPU()
}
flushWorkers := cfg.BackfillFlushWorkers
if flushWorkers <= 0 {
    flushWorkers = 4
}
flushBatchSize := cfg.BackfillDBInsertBatchSize
if flushBatchSize <= 0 {
    flushBatchSize = 100
}
ledgerChanSize := cfg.BackfillLedgerChanSize
if ledgerChanSize <= 0 {
    ledgerChanSize = 256
}
flushChanSize := cfg.BackfillFlushChanSize
if flushChanSize <= 0 {
    flushChanSize = 8
}
```

Remove `backfillPool` from struct initialization. Remove pool metrics registration for "backfill" pool. Add the new fields to the struct literal.

**Step 4: Fix all compilation errors**

Any code referencing `m.backfillPool`, `m.backfillBatchSize`, or `m.backfillDBInsertBatchSize` will break. The backfill file will be rewritten in later tasks, but fix any references in `ingest.go` and test files to compile.

**Step 5: Run tests**

Run: `go build ./internal/services/...`
Expected: Compiles successfully (tests may fail — that's OK, we're rewriting the backfill file next)

**Step 6: Commit**

```bash
git add internal/services/ingest.go
git commit -m "Refactor ingestService config for pipeline-based backfill"
```

---

## Task 2: Implement the Watermark Tracker

**Files:**
- Create: `internal/services/backfill_watermark.go`
- Create: `internal/services/backfill_watermark_test.go`

This is a standalone component with no dependencies on the pipeline, so we build and test it first.

**Step 1: Write the failing tests**

Create `internal/services/backfill_watermark_test.go`:

```go
package services

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func Test_backfillWatermark_MarkFlushed_sequential(t *testing.T) {
    w := newBackfillWatermark(100, 109) // 10 ledgers: 100-109

    // Flush ledgers 100-104
    advanced := w.MarkFlushed([]uint32{100, 101, 102, 103, 104})
    assert.True(t, advanced)
    assert.Equal(t, uint32(104), w.Cursor())

    // Flush ledgers 105-109
    advanced = w.MarkFlushed([]uint32{105, 106, 107, 108, 109})
    assert.True(t, advanced)
    assert.Equal(t, uint32(109), w.Cursor())
    assert.True(t, w.Complete())
}

func Test_backfillWatermark_MarkFlushed_outOfOrder(t *testing.T) {
    w := newBackfillWatermark(100, 109)

    // Flush ledgers 105-109 first — cursor can't advance
    advanced := w.MarkFlushed([]uint32{105, 106, 107, 108, 109})
    assert.False(t, advanced)
    assert.Equal(t, uint32(0), w.Cursor()) // no contiguous range from start

    // Flush ledgers 100-104 — cursor jumps to 109
    advanced = w.MarkFlushed([]uint32{100, 101, 102, 103, 104})
    assert.True(t, advanced)
    assert.Equal(t, uint32(109), w.Cursor())
    assert.True(t, w.Complete())
}

func Test_backfillWatermark_MarkFlushed_withGap(t *testing.T) {
    w := newBackfillWatermark(100, 109)

    // Flush 100-102
    w.MarkFlushed([]uint32{100, 101, 102})
    assert.Equal(t, uint32(102), w.Cursor())

    // Flush 105-107 (skip 103-104)
    advanced := w.MarkFlushed([]uint32{105, 106, 107})
    assert.False(t, advanced) // cursor stuck at 102

    // Flush 103-104 — cursor jumps to 107
    advanced = w.MarkFlushed([]uint32{103, 104})
    assert.True(t, advanced)
    assert.Equal(t, uint32(107), w.Cursor())
    assert.False(t, w.Complete()) // 108, 109 still missing
}

func Test_backfillWatermark_singleLedger(t *testing.T) {
    w := newBackfillWatermark(500, 500)

    advanced := w.MarkFlushed([]uint32{500})
    assert.True(t, advanced)
    assert.Equal(t, uint32(500), w.Cursor())
    assert.True(t, w.Complete())
}

func Test_backfillWatermark_emptyFlush(t *testing.T) {
    w := newBackfillWatermark(100, 109)

    advanced := w.MarkFlushed([]uint32{})
    assert.False(t, advanced)
    assert.Equal(t, uint32(0), w.Cursor())
}
```

**Step 2: Run tests to verify they fail**

Run: `go test -v ./internal/services -run Test_backfillWatermark -timeout 30s`
Expected: FAIL — `newBackfillWatermark` not defined

**Step 3: Implement the watermark tracker**

Create `internal/services/backfill_watermark.go`:

```go
package services

import "sync"

// backfillWatermark tracks which ledgers in a gap have been flushed to DB,
// and reports the highest contiguous flushed ledger for cursor updates.
// Thread-safe: multiple flush workers call MarkFlushed concurrently.
type backfillWatermark struct {
    mu       sync.Mutex
    flushed  []bool // indexed by (ledgerSeq - startLedger)
    start    uint32 // first ledger in gap
    end      uint32 // last ledger in gap
    cursor   uint32 // highest contiguous flushed ledger (0 = none)
    scanFrom uint32 // optimization: resume scan from last cursor position
}

// newBackfillWatermark creates a watermark tracker for the gap [start, end].
func newBackfillWatermark(start, end uint32) *backfillWatermark {
    return &backfillWatermark{
        flushed:  make([]bool, end-start+1),
        start:    start,
        end:      end,
        scanFrom: start,
    }
}

// MarkFlushed records the given ledger sequences as flushed and advances the
// cursor if possible. Returns true if the cursor advanced.
func (w *backfillWatermark) MarkFlushed(ledgers []uint32) bool {
    w.mu.Lock()
    defer w.mu.Unlock()

    for _, seq := range ledgers {
        if seq >= w.start && seq <= w.end {
            w.flushed[seq-w.start] = true
        }
    }

    // Advance cursor from last known position
    oldCursor := w.cursor
    for w.scanFrom <= w.end && w.flushed[w.scanFrom-w.start] {
        w.cursor = w.scanFrom
        w.scanFrom++
    }

    return w.cursor != oldCursor
}

// Cursor returns the highest contiguous flushed ledger, or 0 if none.
func (w *backfillWatermark) Cursor() uint32 {
    w.mu.Lock()
    defer w.mu.Unlock()
    return w.cursor
}

// Complete returns true if all ledgers in the gap have been flushed.
func (w *backfillWatermark) Complete() bool {
    w.mu.Lock()
    defer w.mu.Unlock()
    return w.cursor == w.end
}
```

**Step 4: Run tests to verify they pass**

Run: `go test -v ./internal/services -run Test_backfillWatermark -timeout 30s`
Expected: PASS (all 5 tests)

**Step 5: Commit**

```bash
git add internal/services/backfill_watermark.go internal/services/backfill_watermark_test.go
git commit -m "Add backfill watermark tracker for contiguous cursor advancement"
```

---

## Task 3: Implement the `flushItem` Type and Flush Worker

**Files:**
- Create: `internal/services/backfill_flush.go`
- Create: `internal/services/backfill_flush_test.go`

The flush worker receives filled buffers and writes them to DB with parallel COPYs. Tested with mocks.

**Step 1: Write the flush item type and flush function**

Create `internal/services/backfill_flush.go`:

```go
package services

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/stellar/go-stellar-sdk/support/log"
    "golang.org/x/sync/errgroup"

    "github.com/stellar/wallet-backend/internal/indexer"
    "github.com/stellar/wallet-backend/internal/metrics"
)

// flushItem is the unit of work sent from process workers to flush workers.
// It contains a filled IndexerBuffer and the ledger sequences it covers,
// so the watermark tracker knows which ledgers were persisted.
type flushItem struct {
    Buffer  *indexer.IndexerBuffer
    Ledgers []uint32
}

// runFlushWorkers starts M flush worker goroutines that read from flushCh,
// write data to DB via parallel COPYs, and report success to the watermark.
// Returns when flushCh is closed and all in-flight flushes complete.
func (m *ingestService) runFlushWorkers(
    ctx context.Context,
    flushCh <-chan flushItem,
    watermark *backfillWatermark,
    numWorkers int,
) {
    var wg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for item := range flushCh {
                if ctx.Err() != nil {
                    return
                }
                if err := m.flushBufferWithRetry(ctx, item.Buffer); err != nil {
                    log.Ctx(ctx).Errorf("Flush worker %d: failed to flush %d ledgers: %v",
                        workerID, len(item.Ledgers), err)
                    continue
                }
                if advanced := watermark.MarkFlushed(item.Ledgers); advanced {
                    cursor := watermark.Cursor()
                    if err := m.updateOldestCursor(ctx, cursor); err != nil {
                        log.Ctx(ctx).Warnf("Flush worker %d: failed to update cursor to %d: %v",
                            workerID, cursor, err)
                    }
                }
            }
        }(i)
    }
    wg.Wait()
}

// flushBufferWithRetry persists a buffer's data to DB via 5 parallel COPYs
// with exponential backoff retry.
func (m *ingestService) flushBufferWithRetry(ctx context.Context, buffer *indexer.IndexerBuffer) error {
    var lastErr error
    for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
        select {
        case <-ctx.Done():
            return fmt.Errorf("context cancelled: %w", ctx.Err())
        default:
        }

        txs := buffer.GetTransactions()
        if _, _, err := m.insertAndUpsertParallel(ctx, txs, buffer); err != nil {
            lastErr = err
            m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush").Inc()

            backoff := time.Duration(1<<attempt) * time.Second
            if backoff > maxIngestProcessedDataRetryBackoff {
                backoff = maxIngestProcessedDataRetryBackoff
            }
            log.Ctx(ctx).Warnf("Error flushing buffer (attempt %d/%d): %v, retrying in %v...",
                attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

            select {
            case <-ctx.Done():
                return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
            case <-time.After(backoff):
            }
            continue
        }
        return nil
    }
    m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("batch_flush").Inc()
    return lastErr
}
```

**Step 2: Run compilation check**

Run: `go build ./internal/services/...`
Expected: Compiles (depends on Task 1 being done first)

**Step 3: Commit**

```bash
git add internal/services/backfill_flush.go
git commit -m "Add flush workers with parallel COPY and retry for backfill pipeline"
```

---

## Task 4: Implement the 3-Stage Pipeline Orchestrator

**Files:**
- Rewrite: `internal/services/ingest_backfill.go`

This is the core refactor. Replace all existing backfill code (except `calculateBackfillGaps` which is kept as-is) with the 3-stage pipeline.

**Step 1: Rewrite `ingest_backfill.go`**

The file should contain:

1. **`startBackfilling`** — orchestrator: calculate gaps, process each gap sequentially
2. **`calculateBackfillGaps`** — KEEP AS-IS (lines 100-157 of current file)
3. **`processGap`** — sets up backend, launches 3 stages, waits for completion
4. **`runDispatcher`** — Stage 1: sequential GetLedger, sends to ledgerCh
5. **`runProcessWorkers`** — Stage 2: N workers pull from ledgerCh, process, send to flushCh

```go
package services

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
    "github.com/stellar/go-stellar-sdk/support/log"
    "github.com/stellar/go-stellar-sdk/xdr"

    "github.com/stellar/wallet-backend/internal/data"
    "github.com/stellar/wallet-backend/internal/indexer"
)

// startBackfilling processes ledgers in the specified range, identifying gaps
// and processing them sequentially via a 3-stage pipeline.
func (m *ingestService) startBackfilling(ctx context.Context, startLedger, endLedger uint32) error {
    if startLedger > endLedger {
        return fmt.Errorf("start ledger cannot be greater than end ledger")
    }

    latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
    if err != nil {
        return fmt.Errorf("getting latest ledger cursor: %w", err)
    }

    if endLedger > latestIngestedLedger {
        return fmt.Errorf("end ledger %d cannot be greater than latest ingested ledger %d for backfilling", endLedger, latestIngestedLedger)
    }

    gaps, err := m.calculateBackfillGaps(ctx, startLedger, endLedger)
    if err != nil {
        return fmt.Errorf("calculating backfill gaps: %w", err)
    }
    if len(gaps) == 0 {
        log.Ctx(ctx).Infof("No gaps to backfill in range [%d - %d]", startLedger, endLedger)
        return nil
    }

    overallStart := time.Now()
    for i, gap := range gaps {
        log.Ctx(ctx).Infof("Processing gap %d/%d [%d - %d]", i+1, len(gaps), gap.GapStart, gap.GapEnd)
        if err := m.processGap(ctx, gap); err != nil {
            log.Ctx(ctx).Errorf("Gap %d/%d [%d - %d] failed: %v", i+1, len(gaps), gap.GapStart, gap.GapEnd, err)
            // Continue to next gap — partial progress is preserved via watermark
            continue
        }
    }

    log.Ctx(ctx).Infof("Backfilling completed in %v: %d gaps", time.Since(overallStart), len(gaps))
    return nil
}

// calculateBackfillGaps determines which ledger ranges need to be backfilled.
// KEEP EXISTING IMPLEMENTATION (lines 100-157 of current file) — copy as-is.

// processGap runs the 3-stage pipeline for a single contiguous gap.
func (m *ingestService) processGap(ctx context.Context, gap data.LedgerRange) error {
    gapCtx, gapCancel := context.WithCancelCause(ctx)
    defer gapCancel(nil)

    // Stage 0: Create backend for the full gap range
    backend, err := m.ledgerBackendFactory(gapCtx)
    if err != nil {
        return fmt.Errorf("creating ledger backend: %w", err)
    }
    defer func() {
        if closeErr := backend.Close(); closeErr != nil {
            log.Ctx(ctx).Warnf("Error closing ledger backend for gap [%d-%d]: %v",
                gap.GapStart, gap.GapEnd, closeErr)
        }
    }()

    ledgerRange := ledgerbackend.BoundedRange(gap.GapStart, gap.GapEnd)
    if err := backend.PrepareRange(gapCtx, ledgerRange); err != nil {
        return fmt.Errorf("preparing backend range [%d-%d]: %w", gap.GapStart, gap.GapEnd, err)
    }

    // Create channels
    ledgerCh := make(chan xdr.LedgerCloseMeta, m.backfillLedgerChanSize)
    flushCh := make(chan flushItem, m.backfillFlushChanSize)

    // Create watermark tracker
    watermark := newBackfillWatermark(gap.GapStart, gap.GapEnd)

    var pipelineWg sync.WaitGroup

    // Stage 3: Flush workers (start first so they're ready when data arrives)
    pipelineWg.Add(1)
    go func() {
        defer pipelineWg.Done()
        m.runFlushWorkers(gapCtx, flushCh, watermark, m.backfillFlushWorkers)
    }()

    // Stage 2: Process workers
    pipelineWg.Add(1)
    go func() {
        defer pipelineWg.Done()
        defer close(flushCh) // signal flush workers when all processing is done
        m.runProcessWorkers(gapCtx, gapCancel, ledgerCh, flushCh)
    }()

    // Stage 1: Dispatcher (runs on this goroutine)
    m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)

    // Wait for pipeline to drain
    pipelineWg.Wait()

    // Check if pipeline was cancelled due to error
    if cause := context.Cause(gapCtx); cause != nil && cause != context.Canceled {
        return fmt.Errorf("pipeline failed: %w", cause)
    }

    // Log final state
    cursor := watermark.Cursor()
    total := gap.GapEnd - gap.GapStart + 1
    if watermark.Complete() {
        log.Ctx(ctx).Infof("Gap [%d-%d] complete: %d ledgers", gap.GapStart, gap.GapEnd, total)
    } else {
        log.Ctx(ctx).Warnf("Gap [%d-%d] partial: cursor at %d of %d", gap.GapStart, gap.GapEnd, cursor, gap.GapEnd)
    }

    return nil
}

// runDispatcher is Stage 1: sequentially fetches ledgers from the backend
// and sends them to ledgerCh. Closes ledgerCh when done or on error.
func (m *ingestService) runDispatcher(
    ctx context.Context,
    cancel context.CancelCauseFunc,
    backend ledgerbackend.LedgerBackend,
    gap data.LedgerRange,
    ledgerCh chan<- xdr.LedgerCloseMeta,
) {
    defer close(ledgerCh)

    for seq := gap.GapStart; seq <= gap.GapEnd; seq++ {
        if ctx.Err() != nil {
            return
        }

        lcm, err := m.getLedgerWithRetry(ctx, backend, seq)
        if err != nil {
            cancel(fmt.Errorf("fetching ledger %d: %w", seq, err))
            return
        }

        select {
        case ledgerCh <- lcm:
        case <-ctx.Done():
            return
        }
    }
}

// runProcessWorkers is Stage 2: N workers pull ledgers from ledgerCh,
// process them into IndexerBuffers, and send filled buffers to flushCh.
func (m *ingestService) runProcessWorkers(
    ctx context.Context,
    cancel context.CancelCauseFunc,
    ledgerCh <-chan xdr.LedgerCloseMeta,
    flushCh chan<- flushItem,
) {
    var wg sync.WaitGroup
    for i := 0; i < m.backfillProcessWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            m.processWorkerLoop(ctx, cancel, ledgerCh, flushCh)
        }()
    }
    wg.Wait()
}

// processWorkerLoop is the main loop for a single process worker.
func (m *ingestService) processWorkerLoop(
    ctx context.Context,
    cancel context.CancelCauseFunc,
    ledgerCh <-chan xdr.LedgerCloseMeta,
    flushCh chan<- flushItem,
) {
    buffer := indexer.NewIndexerBuffer()
    var ledgers []uint32

    flush := func() {
        if len(ledgers) == 0 {
            return
        }
        item := flushItem{Buffer: buffer, Ledgers: ledgers}
        select {
        case flushCh <- item:
        case <-ctx.Done():
            return
        }
        buffer = indexer.NewIndexerBuffer()
        ledgers = nil
    }

    for lcm := range ledgerCh {
        if ctx.Err() != nil {
            return
        }

        if err := m.processLedger(ctx, lcm, buffer); err != nil {
            cancel(fmt.Errorf("processing ledger %d: %w", lcm.LedgerSequence(), err))
            return
        }
        ledgers = append(ledgers, lcm.LedgerSequence())

        if uint32(len(ledgers)) >= m.backfillFlushBatchSize {
            flush()
        }
    }

    // Flush remaining
    flush()
}
```

**Step 2: Remove old types and functions**

Delete from the file:
- `BackfillBatch` type
- `BackfillResult` type
- `analyzeBatchResults` function
- `splitGapsIntoBatches` method
- `processBackfillBatchesParallel` method
- `processSingleBatch` method
- `setupBatchBackend` method
- `flushBatchBufferWithRetry` method
- `processLedgersInBatch` method
- `updateOldestCursor` method — KEEP this, it's used by flush workers
- `progressiveRecompressor` type and all its methods

**Step 3: Verify compilation**

Run: `go build ./internal/services/...`
Expected: Compiles. Some tests will fail (addressed in Task 5).

**Step 4: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Rewrite backfill as 3-stage pipeline: dispatcher, process workers, flush workers"
```

---

## Task 5: Update Tests

**Files:**
- Rewrite: `internal/services/ingest_backfill_test.go`
- Modify: `internal/services/ingest_test.go` (remove/update tests for deleted functions)

**Step 1: Identify tests to remove from `ingest_test.go`**

The following tests reference deleted functions and must be removed or updated:
- `Test_ingestService_splitGapsIntoBatches` — DELETE (function removed)
- `Test_ingestService_setupBatchBackend` — DELETE (function removed)
- `Test_ingestService_processSingleBatch` — DELETE (function removed)
- `Test_ingestService_startBackfilling_HistoricalMode_MixedResults` — REWRITE for new pipeline
- `Test_ingestService_startBackfilling_HistoricalMode_AllBatchesFail_CursorUnchanged` — REWRITE for new pipeline
- `Test_ingestProcessedDataWithRetry` — UPDATE to test `flushBufferWithRetry`

Keep unchanged:
- `Test_ingestService_calculateBackfillGaps` — KEEP (function unchanged)
- `Test_ingestService_updateOldestCursor` — KEEP (function unchanged)

**Step 2: Replace `ingest_backfill_test.go`**

Delete all progressive recompressor tests. The watermark tests are already in `backfill_watermark_test.go` (Task 2).

The file can either be deleted entirely or repurposed for pipeline integration tests if needed.

**Step 3: Update test service construction**

All tests that construct `ingestService` need to use the new config fields instead of the old ones. Replace:
```go
BackfillBatchSize:      10,
BackfillDBInsertBatchSize: 50,
```
With:
```go
BackfillProcessWorkers: 2,
BackfillFlushWorkers:   1,
BackfillDBInsertBatchSize: 50,
BackfillLedgerChanSize: 10,
BackfillFlushChanSize:  2,
```

**Step 4: Run all tests**

Run: `go test -v ./internal/services/... -timeout 3m`
Expected: PASS — all remaining tests pass, deleted tests are gone

**Step 5: Commit**

```bash
git add internal/services/ingest_backfill_test.go internal/services/ingest_test.go
git commit -m "Update tests for pipeline-based backfill refactor"
```

---

## Task 6: Clean Up Unused References

**Files:**
- Modify: `internal/services/ingest.go` (remove unused imports, fields)
- Possibly modify: any other files that reference removed types (`BackfillBatch`, `BackfillResult`, `progressiveRecompressor`)

**Step 1: Search for references to removed types**

Search for: `BackfillBatch`, `BackfillResult`, `progressiveRecompressor`, `analyzeBatchResults`, `splitGapsIntoBatches`, `backfillPool`

Remove all dead references.

**Step 2: Remove `backfillPool` from metrics**

The "backfill" pool metrics registration in `NewIngestService` should be removed since the pond pool is gone.

**Step 3: Remove unused imports**

After removing all dead code, clean up imports across modified files.

**Step 4: Run linter and tests**

Run: `make tidy && go test -v -race ./internal/services/... -timeout 3m`
Expected: PASS, no lint errors

**Step 5: Commit**

```bash
git add -u
git commit -m "Remove unused backfill types and pool references"
```

---

## Task 7: Connection Pool Validation

**Files:**
- Modify: `internal/services/ingest.go` (add validation in `NewIngestService`)

**Step 1: Add pool size validation**

In `NewIngestService`, after computing `flushWorkers`, validate:

```go
// Each flush worker runs 5 parallel COPYs, each needing its own connection.
requiredConns := int32(flushWorkers*5 + 5) // 5 headroom for cursor updates etc.
maxConns := cfg.Models.DB.Config().MaxConns
if maxConns > 0 && maxConns < requiredConns {
    return nil, fmt.Errorf(
        "pgxpool max connections (%d) too low for %d flush workers (need at least %d: %d workers * 5 COPYs + 5 headroom)",
        maxConns, flushWorkers, requiredConns, flushWorkers)
}
```

**Step 2: Write test for validation**

Add to `ingest_test.go`:

```go
func Test_NewIngestService_poolValidation(t *testing.T) {
    // Test that NewIngestService returns error when pool is too small
    // for the configured flush workers.
    // Use a real dbtest pool with known MaxConns.
}
```

**Step 3: Run tests**

Run: `go test -v ./internal/services -run Test_NewIngestService -timeout 30s`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/services/ingest.go internal/services/ingest_test.go
git commit -m "Validate connection pool size against flush worker count at startup"
```

---

## Task 8: Final Verification

**Step 1: Run full lint and test suite**

Run: `make tidy && make check && make unit-test`
Expected: All pass

**Step 2: Verify no references to removed code**

Run: `grep -r "progressiveRecompressor\|BackfillBatch\|BackfillResult\|splitGapsIntoBatches\|backfillPool\|analyzeBatchResults" internal/services/`
Expected: No matches (except possibly in comments or the design doc)

**Step 3: Commit any fixes**

If any issues found, fix and commit.

---

## Summary of Files Changed

| File | Action |
|------|--------|
| `internal/services/ingest.go` | Modify: config fields, remove pool, add validation |
| `internal/services/ingest_backfill.go` | Rewrite: 3-stage pipeline |
| `internal/services/backfill_watermark.go` | Create: watermark tracker |
| `internal/services/backfill_watermark_test.go` | Create: watermark tests |
| `internal/services/backfill_flush.go` | Create: flush workers |
| `internal/services/ingest_backfill_test.go` | Rewrite: remove recompressor tests |
| `internal/services/ingest_test.go` | Modify: update/remove tests for deleted functions |

## Execution Order

Tasks 1-3 can be done somewhat independently but should be committed in order. Task 4 depends on Tasks 1-3. Tasks 5-7 depend on Task 4. Task 8 is the final check.

```
Task 1 (config) → Task 2 (watermark) → Task 3 (flush) → Task 4 (pipeline) → Task 5 (tests) → Task 6 (cleanup) → Task 7 (validation) → Task 8 (verify)
```
