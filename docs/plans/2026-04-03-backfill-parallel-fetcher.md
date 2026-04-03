# Backfill Parallel Fetcher Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the single-threaded dispatcher + `optimizedStorageBackend` bottleneck with a `backfillFetcher` that downloads S3 files in parallel and pushes `LedgerCloseMeta` directly to `ledgerCh`, eliminating the priority queue and sequential `GetLedger` overhead.

**Architecture:** New `backfillFetcher` struct in `internal/ingest/` with N worker goroutines that download+decode S3 files and fan out individual ledgers to an external channel. A `BackfillFetcherFactory` closure (created in `setupDeps`) is passed through `IngestServiceConfig` to `ingestService`, replacing the backend+dispatcher in `processGap`. The existing `optimizedStorageBackend` stays untouched for live ingestion.

**Tech Stack:** Go concurrency (goroutines, channels, sync.WaitGroup), `datastore.DataStore` (S3 client), `compressxdr.NewXDRDecoder` (zstd+XDR stream decode), `xdr.LedgerCloseMetaBatch`

**Design doc:** `docs/plans/2026-04-03-backfill-parallel-fetcher-design.md`

---

## Task 1: Create `backfillFetcher` with tests

**Files:**
- Create: `internal/ingest/backfill_fetcher.go`
- Create: `internal/ingest/backfill_fetcher_test.go`

### Step 1: Write the failing test

Create `internal/ingest/backfill_fetcher_test.go`. Test that a fetcher delivers all ledgers in a gap range to `ledgerCh`, unordered.

```go
package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackfillFetcher_DeliversAllLedgers(t *testing.T) {
	// Setup: mock datastore returns batches of 10 ledgers per file.
	// Gap: ledgers 100-129 = 3 files (100-109, 110-119, 120-129).
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	// Register 3 files covering ledgers 100-129
	for fileStart := uint32(100); fileStart < 130; fileStart += ledgersPerFile {
		batch := makeBatch(fileStart, fileStart+ledgersPerFile-1)
		objectKey := schema.GetObjectKeyFromSequenceNumber(fileStart)
		ds.On("GetFile", mock.Anything, objectKey).Return(
			newBatchReader(batch), nil,
		).Once()
	}

	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 3,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
		GapStart:   100,
		GapEnd:     129,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)
	defer fetchCancel(nil)

	stats := fetcher.Run(fetchCtx, fetchCancel)

	// ledgerCh should be closed by Run
	var received []uint32
	for lcm := range ledgerCh {
		received = append(received, lcm.LedgerSequence())
	}

	// All 30 ledgers delivered (order doesn't matter)
	assert.Len(t, received, 30)
	assert.Equal(t, 30, stats.fetchCount)

	// Verify all sequences present
	seqSet := make(map[uint32]bool)
	for _, seq := range received {
		seqSet[seq] = true
	}
	for seq := uint32(100); seq <= 129; seq++ {
		assert.True(t, seqSet[seq], "missing ledger %d", seq)
	}
}
```

Note: This test uses `testSchema` and `mockDataStore` from `storage_backend_test.go`. You will need a helper `newBatchReader` that encodes a `LedgerCloseMetaBatch` through `compressxdr` and returns an `io.ReadCloser`. Model it after how the existing tests create mock S3 responses.

### Step 2: Run test to verify it fails

```bash
go test -v ./internal/ingest/ -run TestBackfillFetcher_DeliversAllLedgers -timeout 30s
```

Expected: FAIL — `NewBackfillFetcher` not defined.

### Step 3: Write minimal `backfillFetcher` implementation

Create `internal/ingest/backfill_fetcher.go`:

```go
// backfillFetcher downloads S3 ledger files in parallel and fans out
// individual LedgerCloseMeta entries directly to an external channel.
// No ordering guarantees — designed for backfill where consumers
// handle ledgers independently and the watermark tracker handles
// out-of-order flushes.
package ingest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// BackfillFetcherConfig configures the parallel S3 fetcher for backfill.
type BackfillFetcherConfig struct {
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration
	GapStart   uint32
	GapEnd     uint32
}

// BackfillFetchStats accumulates timing from all fetch workers.
// Returned by Run for gap summary log aggregation.
type BackfillFetchStats struct {
	FetchCount   int
	FetchTotal   time.Duration
	ChannelWait  map[string]time.Duration // key: "channel:direction"
}

// backfillFetcher downloads S3 ledger files in parallel and pushes
// individual LedgerCloseMeta entries to an external channel.
type backfillFetcher struct {
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema
	config    BackfillFetcherConfig

	ledgerCh chan<- xdr.LedgerCloseMeta // external, caller-owned
}

// NewBackfillFetcher creates a fetcher that will push ledgers from the
// configured gap range to ledgerCh. Call Run to start workers.
func NewBackfillFetcher(
	config BackfillFetcherConfig,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
	ledgerCh chan<- xdr.LedgerCloseMeta,
) *backfillFetcher {
	return &backfillFetcher{
		dataStore: ds,
		schema:    schema,
		config:    config,
		ledgerCh:  ledgerCh,
	}
}

// Run starts fetch workers, waits for them to complete, then closes ledgerCh.
// Returns aggregated fetch stats for the gap summary log.
// On error, calls cancel with the cause; the caller checks context.Cause.
func (f *backfillFetcher) Run(ctx context.Context, cancel context.CancelCauseFunc) *BackfillFetchStats {
	defer close(f.ledgerCh)

	// Compute all file-start sequences up-front (bounded range).
	startBoundary := f.schema.GetSequenceNumberStartBoundary(f.config.GapStart)
	endBoundary := f.schema.GetSequenceNumberStartBoundary(f.config.GapEnd)

	taskCh := make(chan uint32, f.config.NumWorkers)

	// Seed tasks in a goroutine to avoid blocking if taskCh fills.
	go func() {
		defer close(taskCh)
		for seq := startBoundary; seq <= endBoundary; seq += f.schema.LedgersPerFile {
			select {
			case taskCh <- seq:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect per-worker stats.
	statsCh := make(chan *BackfillFetchStats, f.config.NumWorkers)

	var wg sync.WaitGroup
	for i := uint32(0); i < f.config.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := f.worker(ctx, cancel, taskCh)
			statsCh <- stats
		}()
	}
	wg.Wait()
	close(statsCh)

	// Aggregate stats from all workers.
	agg := &BackfillFetchStats{ChannelWait: make(map[string]time.Duration)}
	for ws := range statsCh {
		agg.FetchCount += ws.FetchCount
		agg.FetchTotal += ws.FetchTotal
		for k, v := range ws.ChannelWait {
			agg.ChannelWait[k] += v
		}
	}
	return agg
}

// worker processes file-start sequences from taskCh until the channel
// closes or the context is cancelled.
func (f *backfillFetcher) worker(ctx context.Context, cancel context.CancelCauseFunc, taskCh <-chan uint32) *BackfillFetchStats {
	stats := &BackfillFetchStats{ChannelWait: make(map[string]time.Duration)}

	for sequence := range taskCh {
		if ctx.Err() != nil {
			return stats
		}

		fetchStart := time.Now()
		batch, err := f.downloadWithRetry(ctx, cancel, sequence)
		fetchDur := time.Since(fetchStart)
		if err != nil {
			return stats // cancel already called by downloadWithRetry
		}
		stats.FetchCount++
		stats.FetchTotal += fetchDur

		// Fan out individual ledgers to ledgerCh, filtering to gap range.
		for i := range batch.LedgerCloseMetas {
			lcm := batch.LedgerCloseMetas[i]
			seq := uint32(lcm.LedgerSequence())
			if seq < f.config.GapStart || seq > f.config.GapEnd {
				continue
			}

			sendStart := time.Now()
			select {
			case f.ledgerCh <- lcm:
				stats.ChannelWait[fmt.Sprintf("%s:%s", "ledger", "send")] += time.Since(sendStart)
			case <-ctx.Done():
				return stats
			}
		}
	}
	return stats
}

// downloadWithRetry downloads and decodes a single S3 file with retry.
// On permanent failure, calls cancel and returns the error.
func (f *backfillFetcher) downloadWithRetry(ctx context.Context, cancel context.CancelCauseFunc, sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	for attempt := uint32(0); attempt <= f.config.RetryLimit; attempt++ {
		batch, err := f.downloadAndDecode(ctx, sequence)
		if err != nil {
			if ctx.Err() != nil {
				return xdr.LedgerCloseMetaBatch{}, ctx.Err()
			}
			if os.IsNotExist(err) {
				cancel(fmt.Errorf("ledger file for sequence %d not found: %w", sequence, err))
				return xdr.LedgerCloseMetaBatch{}, err
			}
			if attempt == f.config.RetryLimit {
				cancel(fmt.Errorf("downloading ledger file for sequence %d: maximum retries (%d) exceeded: %w",
					sequence, f.config.RetryLimit, err))
				return xdr.LedgerCloseMetaBatch{}, err
			}
			log.WithField("sequence", sequence).WithError(err).
				Warnf("Backfill fetch error (attempt %d/%d), retrying...", attempt+1, f.config.RetryLimit)
			if !sleepWithContext(ctx, f.config.RetryWait) {
				return xdr.LedgerCloseMetaBatch{}, ctx.Err()
			}
			continue
		}
		return batch, nil
	}
	// Unreachable, but Go requires a return.
	return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("unreachable: retry loop exited for sequence %d", sequence)
}

// downloadAndDecode fetches and stream-decodes a single S3 file.
// Same streaming optimization as storageBuffer.downloadAndDecode.
func (f *backfillFetcher) downloadAndDecode(ctx context.Context, sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	objectKey := f.schema.GetObjectKeyFromSequenceNumber(sequence)
	reader, err := f.dataStore.GetFile(ctx, objectKey)
	if err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("fetching ledger file %s: %w", objectKey, err)
	}
	defer reader.Close() //nolint:errcheck

	var batch xdr.LedgerCloseMetaBatch
	decoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &batch)
	if _, err = decoder.ReadFrom(reader); err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("decoding ledger file %s: %w", objectKey, err)
	}
	return batch, nil
}
```

### Step 4: Run test to verify it passes

```bash
go test -v ./internal/ingest/ -run TestBackfillFetcher -timeout 30s
```

Expected: PASS. You may need to add the `newBatchReader` test helper that compresses a batch so `mockDataStore.GetFile` can return it. Look at how the existing `storage_backend_test.go` mocks S3 responses — if it doesn't have one, create a helper that uses `compressxdr.NewXDREncoder` to write a batch to a buffer and returns `io.NopCloser(bytes.NewReader(buf))`.

### Step 5: Add edge-case tests

Add to `backfill_fetcher_test.go`:

1. **Partial first/last file filtering** — gap 105-124 with `LedgersPerFile=10` should only deliver ledgers 105-124 (not 100-104 or 125-129).
2. **Context cancellation** — cancel context mid-fetch, verify workers exit without deadlock.
3. **Retry on transient error** — mock `GetFile` to fail once then succeed, verify retry works.
4. **Missing file (NotExist)** — mock `GetFile` to return `os.ErrNotExist`, verify `cancel` is called.

### Step 6: Run all tests

```bash
go test -v ./internal/ingest/ -timeout 60s
```

Expected: All pass, including existing `storage_backend_test.go` tests (unchanged).

### Step 7: Commit

```bash
git add internal/ingest/backfill_fetcher.go internal/ingest/backfill_fetcher_test.go
git commit -m "feat: add backfillFetcher for parallel S3 ledger fetching

Replaces the sequential dispatcher + optimizedStorageBackend bottleneck
in the backfill pipeline. Downloads S3 files with N parallel workers and
pushes individual LedgerCloseMeta entries directly to ledgerCh, removing
the priority queue and sequential GetLedger overhead."
```

---

## Task 2: Create `BackfillFetcherFactory` and wire through config

**Files:**
- Modify: `internal/ingest/ingest.go:142-229` (setupDeps + new factory)
- Modify: `internal/ingest/ledger_backend.go:27-57` (extract dataStore/schema creation)
- Modify: `internal/services/ingest.go:45-81` (IngestServiceConfig)
- Modify: `internal/services/ingest.go:101-125` (ingestService struct)
- Modify: `internal/services/ingest.go:127-177` (NewIngestService)
- Modify: `cmd/ingest.go:61-100` (new CLI flag)

### Step 1: Extract dataStore and schema creation

In `internal/ingest/ledger_backend.go`, extract the datastore+schema creation from `newDatastoreLedgerBackend` into a shared function so both the legacy backend and the new fetcher can use it:

```go
// newDatastoreResources creates the DataStore client and loads the schema
// from the S3 manifest. Shared by both optimizedStorageBackend (live) and
// backfillFetcher (backfill).
func newDatastoreResources(ctx context.Context, configPath string, networkPassphrase string) (
	datastore.DataStore, datastore.DataStoreSchema, ledgerbackend.BufferedStorageBackendConfig, error,
) {
	storageBackendConfig, err := loadDatastoreBackendConfig(configPath)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("loading datastore config: %w", err)
	}
	storageBackendConfig.DataStoreConfig.NetworkPassphrase = networkPassphrase

	ds, err := datastore.NewDataStore(ctx, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("creating datastore: %w", err)
	}

	schema, err := datastore.LoadSchema(ctx, ds, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("loading datastore schema: %w", err)
	}

	return ds, schema, storageBackendConfig.BufferedStorageBackendConfig, nil
}
```

Refactor `newDatastoreLedgerBackend` to use it:

```go
func newDatastoreLedgerBackend(ctx context.Context, datastoreConfigPath string, networkPassphrase string) (ledgerbackend.LedgerBackend, error) {
	ds, schema, bufConfig, err := newDatastoreResources(ctx, datastoreConfigPath, networkPassphrase)
	if err != nil {
		return nil, err
	}

	ledgerBackend, err := newOptimizedStorageBackend(bufConfig, ds, schema)
	if err != nil {
		return nil, fmt.Errorf("creating optimized storage backend: %w", err)
	}

	log.Infof("Using optimized storage backend with buffer size %d, %d workers",
		bufConfig.BufferSize, bufConfig.NumWorkers)
	return ledgerBackend, nil
}
```

### Step 2: Define the factory type and add to config

In `internal/services/ingest.go`, add the factory type alongside the existing `LedgerBackendFactory`:

```go
// BackfillFetcherFactory creates a backfillFetcher that pushes ledgers to ledgerCh.
// Each gap gets its own fetcher. The factory encapsulates dataStore/schema creation.
type BackfillFetcherFactory func(ctx context.Context, cancel context.CancelCauseFunc, config ingest.BackfillFetcherConfig, ledgerCh chan<- xdr.LedgerCloseMeta) *ingest.BackfillFetcher
```

Wait — we should avoid the import cycle (`services` importing `ingest`). The `BackfillFetcherConfig` and `BackfillFetchStats` types live in `ingest` package. The service layer already imports `ingest` types indirectly via `indexer`. Let me check...

Actually, `internal/services/` does NOT import `internal/ingest/` currently — the `ledgerBackendFactory` returns `ledgerbackend.LedgerBackend` (from the SDK). We need a factory that returns a runner interface to avoid the import cycle.

**Better approach**: Define an interface in `services` that the fetcher satisfies:

```go
// BackfillFetcher runs parallel S3 download workers and pushes ledgers to a channel.
type BackfillFetcher interface {
	// Run starts workers, blocks until done, closes ledgerCh, returns stats.
	Run(ctx context.Context, cancel context.CancelCauseFunc) *BackfillFetchStats
}

// BackfillFetchStats holds aggregated timing from fetch workers.
type BackfillFetchStats struct {
	FetchCount  int
	FetchTotal  time.Duration
	ChannelWait map[string]time.Duration
}

// BackfillFetcherFactory creates a fetcher for a specific gap.
type BackfillFetcherFactory func(ctx context.Context, config BackfillFetcherConfig, ledgerCh chan<- xdr.LedgerCloseMeta) BackfillFetcher

// BackfillFetcherConfig configures a single fetcher instance.
type BackfillFetcherConfig struct {
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration
	GapStart   uint32
	GapEnd     uint32
}
```

Then move the config/stats types from `ingest` to `services` (or define them in `services` and have the `ingest` fetcher return the `services` type — but that's also a cycle).

**Cleanest approach**: Keep `BackfillFetcherConfig` and `BackfillFetchStats` in the `ingest` package. Define the factory in `ingest` package too. The services package imports `ingest` just like it imports `indexer`. Check if this import already exists or is safe:

Look at imports in `internal/services/ingest.go` — it imports `indexer` already:
```go
"github.com/stellar/wallet-backend/internal/indexer"
```

Adding `"github.com/stellar/wallet-backend/internal/ingest"` should be fine as long as `ingest` doesn't import `services`. Let me verify — `ingest/ingest.go` imports `services` at line 207 (`services.NewIngestService`). **There IS a cycle**: `ingest → services` already exists, so `services → ingest` would create a cycle.

**Resolution**: The factory is a closure created in `setupDeps` (in the `ingest` package). It returns an interface defined in `services`. The stats type needs to live somewhere both can see — either a shared package or just use a generic struct.

**Simplest solution**: Use the same pattern as `LedgerBackendFactory` — return a function type that returns a concrete runner. The runner is a `func() *backfillWorkerStats` (which already exists in services). The factory closure captures `dataStore` and `schema` from `setupDeps`.

```go
// In services/ingest.go:
// BackfillFetcherFactory creates a fetcher for a gap and returns a Run function.
// The Run function starts workers, blocks until done, closes ledgerCh, and returns
// fetch stats compatible with the gap summary log.
type BackfillFetcherFactory func(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	gapStart, gapEnd uint32,
	ledgerCh chan<- xdr.LedgerCloseMeta,
) *backfillWorkerStats
```

This avoids any new types crossing the package boundary — `backfillWorkerStats` already exists in `services`, and `xdr.LedgerCloseMeta` is from the SDK. The factory is a closure that internally creates a `backfillFetcher` (in `ingest` package) and adapts its stats to `*backfillWorkerStats`.

Add to `IngestServiceConfig` (after line 80):

```go
// === Backfill Tuning ===
BackfillProcessWorkers    int
BackfillFlushWorkers      int
BackfillDBInsertBatchSize int
BackfillLedgerChanSize    int
BackfillFlushChanSize     int
BackfillFetcherFactory    BackfillFetcherFactory // NEW: parallel S3 fetcher for backfill
BackfillFetchWorkers      int                    // NEW: S3 download goroutines (default: 15)
```

Add to `ingestService` struct (after line 123):

```go
backfillFetcherFactory BackfillFetcherFactory
backfillFetchWorkers   int
```

Assign in `NewIngestService` (after line 174):

```go
backfillFetcherFactory: cfg.BackfillFetcherFactory,
backfillFetchWorkers:   cfg.BackfillFetchWorkers,
```

### Step 3: Create the factory in `setupDeps`

In `internal/ingest/ingest.go`, after the existing `ledgerBackendFactory` (line 205), add:

```go
// Create factory for the parallel backfill fetcher.
// Each gap gets its own fetcher with fresh S3 connections.
var backfillFetcherFactory services.BackfillFetcherFactory
if cfg.LedgerBackendType == LedgerBackendTypeDatastore {
	backfillFetcherFactory = func(
		ctx context.Context,
		cancel context.CancelCauseFunc,
		gapStart, gapEnd uint32,
		ledgerCh chan<- xdr.LedgerCloseMeta,
	) *services.BackfillWorkerStats {
		ds, schema, _, err := newDatastoreResources(ctx, cfg.DatastoreConfigPath, cfg.NetworkPassphrase)
		if err != nil {
			cancel(fmt.Errorf("creating datastore resources: %w", err))
			return &services.BackfillWorkerStats{}
		}

		fetcher := NewBackfillFetcher(BackfillFetcherConfig{
			NumWorkers: uint32(cfg.BackfillFetchWorkers),
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
			GapStart:   gapStart,
			GapEnd:     gapEnd,
		}, ds, schema, ledgerCh)

		fetchStats := fetcher.Run(ctx, cancel)

		// Adapt ingest.BackfillFetchStats → services.backfillWorkerStats
		ws := &services.BackfillWorkerStats{}
		// ... map fields
		return ws
	}
}
```

**Wait** — this requires `services.BackfillWorkerStats` to be exported, and creates a `services → ingest` import path issue. Let me reconsider.

**Final, cleanest approach**: Make the factory return a **function** — no cross-package types needed:

```go
// In services/ingest.go:
// BackfillFetcherRunner starts fetch workers and returns when done.
// It must close ledgerCh before returning.
// Returns (fetchCount, fetchTotal, channelWait).
type BackfillFetcherRunner func(ctx context.Context, cancel context.CancelCauseFunc) (int, time.Duration, map[string]time.Duration)

// BackfillFetcherFactory creates a runner for a specific gap.
type BackfillFetcherFactory func(gapStart, gapEnd uint32, ledgerCh chan<- xdr.LedgerCloseMeta) BackfillFetcherRunner
```

In `setupDeps`, the factory closure creates the fetcher and returns a runner that adapts the results:

```go
backfillFetcherFactory := func(gapStart, gapEnd uint32, ledgerCh chan<- xdr.LedgerCloseMeta) services.BackfillFetcherRunner {
	return func(ctx context.Context, cancel context.CancelCauseFunc) (int, time.Duration, map[string]time.Duration) {
		ds, schema, _, err := newDatastoreResources(ctx, cfg.DatastoreConfigPath, cfg.NetworkPassphrase)
		if err != nil {
			cancel(fmt.Errorf("creating datastore resources: %w", err))
			return 0, 0, nil
		}
		fetcher := NewBackfillFetcher(BackfillFetcherConfig{
			NumWorkers: uint32(cfg.BackfillFetchWorkers),
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
			GapStart:   gapStart,
			GapEnd:     gapEnd,
		}, ds, schema, ledgerCh)
		stats := fetcher.Run(ctx, cancel)
		return stats.FetchCount, stats.FetchTotal, stats.ChannelWait
	}
}
```

No new cross-package types — only primitives and `map[string]time.Duration`.

### Step 4: Add CLI flag for `BackfillFetchWorkers`

In `cmd/ingest.go`, add after the `backfill-flush-chan-size` flag (after line 100):

```go
{
	Name:        "backfill-fetch-workers",
	Usage:       "Number of parallel S3 download workers in the backfill fetcher. Each worker downloads and decodes one file at a time.",
	OptType:     types.Int,
	ConfigKey:   &cfg.BackfillFetchWorkers,
	FlagDefault: 15,
	Required:    false,
},
```

Add `BackfillFetchWorkers int` to `ingest.Configs` struct (after line 86).

### Step 5: Run existing tests

```bash
go test -v ./internal/ingest/ -timeout 60s
go test -v ./internal/services/ -timeout 3m
```

Expected: All existing tests pass. The new factory is nil for non-datastore backends and for tests that don't set it.

### Step 6: Commit

```bash
git add internal/ingest/ledger_backend.go internal/ingest/ingest.go internal/services/ingest.go cmd/ingest.go
git commit -m "feat: wire BackfillFetcherFactory through config pipeline

Extract newDatastoreResources from newDatastoreLedgerBackend for reuse.
Add BackfillFetcherFactory and BackfillFetchWorkers to IngestServiceConfig.
Add --backfill-fetch-workers CLI flag (default: 15)."
```

---

## Task 3: Replace `runDispatcher` with `backfillFetcher` in `processGap`

**Files:**
- Modify: `internal/services/ingest_backfill.go:152-291` (processGap)
- Modify: `internal/services/ingest_backfill.go:296-332` (runDispatcher — remove)

### Step 1: Modify `processGap` to use the fetcher

In `processGap`, replace the backend creation + dispatcher with the fetcher factory.

**Remove** (lines 161-174):
```go
backend, err := m.ledgerBackendFactory(gapCtx)
// ... PrepareRange, defer Close
```

**Remove** (line 237):
```go
dispatcherStats := m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)
```

**Replace with**:
```go
// Stage 1: parallel S3 fetcher (replaces dispatcher + backend)
var fetchCount int
var fetchTotal time.Duration
var fetchChannelWait map[string]time.Duration

pipelineWg.Add(1)
go func() {
	defer pipelineWg.Done()
	runner := m.backfillFetcherFactory(gap.GapStart, gap.GapEnd, ledgerCh)
	fetchCount, fetchTotal, fetchChannelWait = runner(gapCtx, gapCancel)
}()
```

Note: The fetcher's `Run` closes `ledgerCh`, which is the same contract as `runDispatcher` (which calls `defer close(ledgerCh)` at line 303). The process workers' `for lcm := range ledgerCh` loop exits when `ledgerCh` closes, then they close `flushCh`.

**Update the stats aggregation** (lines 248-257). Replace:
```go
gapStats.mergeWorker(dispatcherStats)
```

With:
```go
// Merge fetcher stats into gap stats
gapStats.fetchCount += fetchCount
gapStats.fetchTotal += fetchTotal
for k, v := range fetchChannelWait {
	gapStats.channelWait[k] += v
}
```

### Step 2: Remove `runDispatcher`

Delete the `runDispatcher` method (lines 296-332 in `ingest_backfill.go`). It's no longer called.

Also remove the `getLedgerWithRetry` import if it's only used by the dispatcher — but check first: it's also used by live ingestion, so likely keep it.

### Step 3: Run tests

```bash
go test -v ./internal/services/ -timeout 3m
```

Expected: Tests pass. Any test that mocks `ledgerBackendFactory` for backfill scenarios will need updating to mock `backfillFetcherFactory` instead.

### Step 4: Commit

```bash
git add internal/services/ingest_backfill.go
git commit -m "feat: replace dispatcher with parallel backfillFetcher in processGap

Remove runDispatcher and backend creation from processGap.
Use BackfillFetcherFactory to create a parallel S3 fetcher per gap.
Workers push LedgerCloseMeta directly to ledgerCh, eliminating the
priority queue and sequential GetLedger bottleneck."
```

---

## Task 4: Add Prometheus metrics to the fetcher

**Files:**
- Modify: `internal/ingest/backfill_fetcher.go` (add metrics observations)
- Modify: `internal/ingest/backfill_fetcher.go` (accept metrics in config or via option)

### Step 1: Add metrics to fetcher worker

The existing dispatcher observes two metrics:
- `PhaseDuration.WithLabelValues("backfill_fetch").Observe(fetchDur.Seconds())`
- `BackfillChannelWait.WithLabelValues("ledger", "send").Observe(sendDur.Seconds())`

Since the fetcher lives in `internal/ingest/` (not `services/`), it can't directly access `appMetrics`. Two options:
1. Pass the specific metric observers as function callbacks
2. Pass the full `metrics.Ingestion` struct

Use option 1 — add optional observer callbacks to the config:

```go
type BackfillFetcherConfig struct {
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration
	GapStart   uint32
	GapEnd     uint32
	// Optional Prometheus observers (nil-safe — no metrics if nil).
	OnFetchDuration   func(seconds float64)
	OnChannelWait     func(channel, direction string, seconds float64)
}
```

In the worker, after recording stats:

```go
if f.config.OnFetchDuration != nil {
	f.config.OnFetchDuration(fetchDur.Seconds())
}
// ... and for channel wait:
if f.config.OnChannelWait != nil {
	f.config.OnChannelWait("ledger", "send", sendDur.Seconds())
}
```

Wire in `setupDeps` when creating the factory:

```go
OnFetchDuration: func(s float64) {
	m.Ingestion.PhaseDuration.WithLabelValues("backfill_fetch").Observe(s)
},
OnChannelWait: func(ch, dir string, s float64) {
	m.Ingestion.BackfillChannelWait.WithLabelValues(ch, dir).Observe(s)
},
```

### Step 2: Run tests

```bash
go test -v ./internal/ingest/ -run TestBackfillFetcher -timeout 30s
```

Expected: Pass (callbacks are nil in tests — no metrics observed).

### Step 3: Commit

```bash
git add internal/ingest/backfill_fetcher.go internal/ingest/ingest.go
git commit -m "feat: add Prometheus metric callbacks to backfillFetcher

Observe backfill_fetch phase duration and ledger channel send wait
via optional callbacks, wired to the existing Prometheus metrics."
```

---

## Task 5: Update channel utilization sampler

**Files:**
- Modify: `internal/services/ingest_backfill.go:192-217` (sampler goroutine)

### Step 1: Review sampler

The sampler currently samples `ledgerCh` and `flushCh` fill ratios. With the fetcher owning `ledgerCh` production, the sampler still works — it just reads `len(ledgerCh)/cap(ledgerCh)`. No code changes needed for the sampler itself.

However, the sampler is started **before** the fetcher, and the fetcher closes `ledgerCh`. After close, `len(ledgerCh)` returns 0 and `cap(ledgerCh)` is still valid — so reading fill ratio of a closed channel is safe in Go.

**No changes needed.** Move to next task.

### Step 2: Commit (skip — no changes)

---

## Task 6: Clean up and verify

**Files:**
- All modified files

### Step 1: Run linting

```bash
make tidy
make check
```

Fix any issues.

### Step 2: Run full unit tests

```bash
make unit-test
```

Expected: All pass.

### Step 3: Run backfill on a small gap

```bash
go run main.go ingest --mode=backfill --start-ledger=61602559 --end-ledger=61603559 --backfill-fetch-workers=15 --backfill-process-workers=8 --backfill-flush-workers=2 --log-level=debug
```

Verify:
- Gap completes without errors
- Summary log shows fetch stats (fetch count, fetch total, channel wait)
- No missing ledgers in DB

### Step 4: Run performance comparison

Run on the same 34,561-ledger gap and compare:
- Previous: 60 ledgers/sec
- Expected: >60 ledgers/sec (process-worker-limited, ~67 with 8 workers)

Then scale up process workers:
```bash
--backfill-process-workers=16 --backfill-flush-workers=4
```

Expected: ~130 ledgers/sec.

### Step 5: Final commit

```bash
git add -A
git commit -m "chore: clean up after backfill parallel fetcher integration"
```
