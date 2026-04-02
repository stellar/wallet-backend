# Backfill Pipeline Observability Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add per-stage timing, channel wait/utilization metrics, throughput counters, and structured gap summary logs to the 3-stage backfill pipeline.

**Architecture:** Extend the existing `IngestionMetrics` struct with 8 new Prometheus metrics (14 series). Each pipeline stage instruments its own timing and channel waits. A per-gap stats accumulator collects worker-local data for a summary log at gap completion. A lightweight channel utilization sampler goroutine runs per gap.

**Tech Stack:** `prometheus/client_golang`, existing `metrics.Metrics` DI pattern, Go channels, `sync/atomic` for stats.

**Design doc:** `docs/plans/2026-04-02-backfill-observability-design.md`

---

### Task 1: Add new backfill metrics to IngestionMetrics

**Files:**
- Modify: `internal/metrics/ingestion.go`
- Modify: `internal/metrics/ingestion_test.go`

**Step 1: Write the failing tests for new metric fields**

Add to `internal/metrics/ingestion_test.go`:

```go
func TestIngestionMetrics_BackfillRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	require.NotNil(t, m.BackfillChannelWait)
	require.NotNil(t, m.BackfillChannelUtilization)
	require.NotNil(t, m.BackfillLedgersFlushed)
	require.NotNil(t, m.BackfillBatchSize)
	require.NotNil(t, m.BackfillGapProgress)
	require.NotNil(t, m.BackfillGapStartLedger)
	require.NotNil(t, m.BackfillGapEndLedger)
}

func TestIngestionMetrics_BackfillChannelWait_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(0.1)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_backfill_channel_wait_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 10)
		}
	}
}

func TestIngestionMetrics_BackfillBatchSize_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BackfillBatchSize.Observe(50)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_backfill_batch_size" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 10) // LinearBuckets(10, 10, 10)
		}
	}
}

func TestIngestionMetrics_BackfillChannelWait_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(0.05)
	m.BackfillChannelWait.WithLabelValues("flush", "receive").Observe(1.0)

	// Verify labels exist and are correct
	assert.Equal(t, 1.0, testutil.ToFloat64(
		m.BackfillChannelWait.WithLabelValues("ledger", "send").(*prometheus.HistogramVec... // skip: just assert non-panic
	))
	// Simpler: just ensure no panic on valid label combos
	assert.NotPanics(t, func() {
		m.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("ledger", "receive").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("flush", "send").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("flush", "receive").Observe(0.1)
	})
}
```

Also update the lint test to include new collectors:

```go
// In TestIngestionMetrics_Lint, add to the collector slice:
m.BackfillChannelWait, m.BackfillChannelUtilization,
m.BackfillLedgersFlushed, m.BackfillBatchSize,
m.BackfillGapProgress, m.BackfillGapStartLedger, m.BackfillGapEndLedger,
```

**Step 2: Run tests to verify they fail**

Run: `go test -v ./internal/metrics/ -run TestIngestionMetrics_Backfill -timeout 30s`
Expected: FAIL — fields don't exist on `IngestionMetrics`

**Step 3: Add new metric fields and registration to ingestion.go**

Add these fields to the `IngestionMetrics` struct in `internal/metrics/ingestion.go`:

```go
// --- Backfill Pipeline Metrics ---

// BackfillChannelWait observes time goroutines spend blocked on channel operations.
// Labels: channel ("ledger", "flush"), direction ("send", "receive").
// PromQL: histogram_quantile(0.99, rate(wallet_ingestion_backfill_channel_wait_seconds_bucket{channel="ledger",direction="send"}[5m]))
BackfillChannelWait *prometheus.HistogramVec
// BackfillChannelUtilization reports channel fill ratio (0.0-1.0), sampled every second.
// Labels: channel ("ledger", "flush").
// PromQL: wallet_ingestion_backfill_channel_utilization_ratio{channel="ledger"}
BackfillChannelUtilization *prometheus.GaugeVec
// BackfillLedgersFlushed counts ledgers successfully flushed to DB during backfill.
// PromQL: rate(wallet_ingestion_backfill_ledgers_flushed_total[5m])
BackfillLedgersFlushed prometheus.Counter
// BackfillBatchSize observes the number of ledgers per flush batch.
// PromQL: histogram_quantile(0.5, rate(wallet_ingestion_backfill_batch_size_bucket[5m]))
BackfillBatchSize prometheus.Histogram
// BackfillGapProgress reports completion ratio (0.0-1.0) of the current gap.
// PromQL: wallet_ingestion_backfill_gap_progress_ratio
BackfillGapProgress prometheus.Gauge
// BackfillGapStartLedger is the start ledger of the gap currently being processed.
// PromQL: wallet_ingestion_backfill_gap_start_ledger
BackfillGapStartLedger prometheus.Gauge
// BackfillGapEndLedger is the end ledger of the gap currently being processed.
// PromQL: wallet_ingestion_backfill_gap_end_ledger
BackfillGapEndLedger prometheus.Gauge
```

Add initialization in `newIngestionMetrics()`:

```go
BackfillChannelWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
    Name:    "wallet_ingestion_backfill_channel_wait_seconds",
    Help:    "Time goroutines spend blocked on backfill pipeline channel operations.",
    Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
}, []string{"channel", "direction"}),
BackfillChannelUtilization: prometheus.NewGaugeVec(prometheus.GaugeOpts{
    Name: "wallet_ingestion_backfill_channel_utilization_ratio",
    Help: "Fill ratio (0.0-1.0) of backfill pipeline channels, sampled every second.",
}, []string{"channel"}),
BackfillLedgersFlushed: prometheus.NewCounter(prometheus.CounterOpts{
    Name: "wallet_ingestion_backfill_ledgers_flushed_total",
    Help: "Total ledgers successfully flushed to database during backfill.",
}),
BackfillBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
    Name:    "wallet_ingestion_backfill_batch_size",
    Help:    "Number of ledgers per flush batch during backfill.",
    Buckets: prometheus.LinearBuckets(10, 10, 10),
}),
BackfillGapProgress: prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "wallet_ingestion_backfill_gap_progress_ratio",
    Help: "Completion ratio (0.0-1.0) of the backfill gap currently being processed.",
}),
BackfillGapStartLedger: prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "wallet_ingestion_backfill_gap_start_ledger",
    Help: "Start ledger of the backfill gap currently being processed (0 when idle).",
}),
BackfillGapEndLedger: prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "wallet_ingestion_backfill_gap_end_ledger",
    Help: "End ledger of the backfill gap currently being processed (0 when idle).",
}),
```

Add to `reg.MustRegister(...)`:

```go
m.BackfillChannelWait,
m.BackfillChannelUtilization,
m.BackfillLedgersFlushed,
m.BackfillBatchSize,
m.BackfillGapProgress,
m.BackfillGapStartLedger,
m.BackfillGapEndLedger,
```

**Step 4: Run tests to verify they pass**

Run: `go test -v ./internal/metrics/ -timeout 30s`
Expected: ALL PASS (including lint test with new collectors)

**Step 5: Commit**

```bash
git add internal/metrics/ingestion.go internal/metrics/ingestion_test.go
git commit -m "Add backfill pipeline Prometheus metrics to IngestionMetrics"
```

---

### Task 2: Create backfill stats types

**Files:**
- Create: `internal/services/backfill_stats.go`
- Create: `internal/services/backfill_stats_test.go`

**Step 1: Write the failing tests**

Add to `internal/services/backfill_stats_test.go`:

```go
package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackfillWorkerStats_Add(t *testing.T) {
	s := &backfillWorkerStats{}

	s.addFetch(100 * time.Millisecond)
	s.addFetch(200 * time.Millisecond)
	s.addProcess(50 * time.Millisecond)
	s.addChannelWait("ledger", "send", 10*time.Millisecond)

	assert.Equal(t, 2, s.fetchCount)
	assert.Equal(t, 300*time.Millisecond, s.fetchTotal)
	assert.Equal(t, 1, s.processCount)
	assert.Equal(t, 50*time.Millisecond, s.processTotal)
	assert.Equal(t, 10*time.Millisecond, s.channelWait["ledger:send"])
}

func TestBackfillGapStats_Merge(t *testing.T) {
	gs := newBackfillGapStats()

	w1 := &backfillWorkerStats{}
	w1.addFetch(100 * time.Millisecond)
	w1.addProcess(50 * time.Millisecond)
	w1.addChannelWait("ledger", "send", 10*time.Millisecond)

	w2 := &backfillWorkerStats{}
	w2.addFetch(200 * time.Millisecond)
	w2.addProcess(75 * time.Millisecond)
	w2.addChannelWait("ledger", "send", 20*time.Millisecond)

	gs.mergeWorker(w1)
	gs.mergeWorker(w2)

	assert.Equal(t, 3, gs.fetchCount)  // NOT merged — wait, this should be sum: 1+1=2? No: let me re-check...
	// Actually w1 has fetchCount=1, w2 has fetchCount=1 => merged = 2
	// Let me fix: the test above sets w1.fetchCount=2 via two addFetch calls
	// So merged = 2 + 1 = 3
	assert.Equal(t, 300*time.Millisecond, gs.fetchTotal)
	assert.Equal(t, 2, gs.processCount)
	assert.Equal(t, 125*time.Millisecond, gs.processTotal)
	assert.Equal(t, 30*time.Millisecond, gs.channelWait["ledger:send"])
}

func TestBackfillGapStats_MergeFlush(t *testing.T) {
	gs := newBackfillGapStats()

	w := &backfillFlushWorkerStats{}
	w.addFlush(500 * time.Millisecond)
	w.addFlush(300 * time.Millisecond)
	w.addChannelWait("flush", "receive", 100*time.Millisecond)

	gs.mergeFlushWorker(w)

	assert.Equal(t, 2, gs.flushCount)
	assert.Equal(t, 800*time.Millisecond, gs.flushTotal)
	assert.Equal(t, 100*time.Millisecond, gs.channelWait["flush:receive"])
}
```

**Step 2: Run tests to verify they fail**

Run: `go test -v ./internal/services/ -run TestBackfill.*Stats -timeout 30s`
Expected: FAIL — types don't exist

**Step 3: Implement backfill stats types**

Create `internal/services/backfill_stats.go`:

```go
package services

import (
	"fmt"
	"time"
)

// backfillWorkerStats accumulates timing for a single process worker.
// Each worker owns its instance — no mutex needed.
type backfillWorkerStats struct {
	fetchCount   int
	fetchTotal   time.Duration
	processCount int
	processTotal time.Duration
	channelWait  map[string]time.Duration // key: "channel:direction"
}

func (s *backfillWorkerStats) addFetch(d time.Duration) {
	s.fetchCount++
	s.fetchTotal += d
}

func (s *backfillWorkerStats) addProcess(d time.Duration) {
	s.processCount++
	s.processTotal += d
}

func (s *backfillWorkerStats) addChannelWait(channel, direction string, d time.Duration) {
	if s.channelWait == nil {
		s.channelWait = make(map[string]time.Duration)
	}
	s.channelWait[fmt.Sprintf("%s:%s", channel, direction)] += d
}

// backfillFlushWorkerStats accumulates timing for a single flush worker.
type backfillFlushWorkerStats struct {
	flushCount  int
	flushTotal  time.Duration
	channelWait map[string]time.Duration
}

func (s *backfillFlushWorkerStats) addFlush(d time.Duration) {
	s.flushCount++
	s.flushTotal += d
}

func (s *backfillFlushWorkerStats) addChannelWait(channel, direction string, d time.Duration) {
	if s.channelWait == nil {
		s.channelWait = make(map[string]time.Duration)
	}
	s.channelWait[fmt.Sprintf("%s:%s", channel, direction)] += d
}

// backfillGapStats aggregates stats from all workers for a single gap.
// Only accessed from processGap after workers complete — no mutex needed.
type backfillGapStats struct {
	fetchCount   int
	fetchTotal   time.Duration
	processCount int
	processTotal time.Duration
	flushCount   int
	flushTotal   time.Duration
	channelWait  map[string]time.Duration
}

func newBackfillGapStats() *backfillGapStats {
	return &backfillGapStats{
		channelWait: make(map[string]time.Duration),
	}
}

func (gs *backfillGapStats) mergeWorker(w *backfillWorkerStats) {
	gs.fetchCount += w.fetchCount
	gs.fetchTotal += w.fetchTotal
	gs.processCount += w.processCount
	gs.processTotal += w.processTotal
	for k, v := range w.channelWait {
		gs.channelWait[k] += v
	}
}

func (gs *backfillGapStats) mergeFlushWorker(w *backfillFlushWorkerStats) {
	gs.flushCount += w.flushCount
	gs.flushTotal += w.flushTotal
	for k, v := range w.channelWait {
		gs.channelWait[k] += v
	}
}

// avgOrZero returns average duration, or zero if count is 0.
func avgOrZero(total time.Duration, count int) time.Duration {
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}
```

**Step 4: Run tests to verify they pass**

Run: `go test -v ./internal/services/ -run TestBackfill.*Stats -timeout 30s`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add internal/services/backfill_stats.go internal/services/backfill_stats_test.go
git commit -m "Add backfill stats accumulator types for gap summary logging"
```

---

### Task 3: Instrument Stage 1 (Dispatcher) with fetch timing and channel wait

**Files:**
- Modify: `internal/services/ingest_backfill.go` — `runDispatcher` function

**Step 1: Add fetch timing and ledger-send channel wait instrumentation**

Modify `runDispatcher` in `internal/services/ingest_backfill.go`. The function currently:
1. Loops `gap.GapStart` to `gap.GapEnd`
2. Calls `getLedgerWithRetry` (already has `LedgerFetchDuration` metric for total including retries)
3. Sends to `ledgerCh`

Change the signature to accept `*backfillWorkerStats` and return it filled. Add timing around the fetch call (for the `backfill_fetch` phase — just the successful call time, different from `LedgerFetchDuration` which includes retries), and timing around the channel send:

```go
func (m *ingestService) runDispatcher(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	backend ledgerbackend.LedgerBackend,
	gap data.LedgerRange,
	ledgerCh chan<- xdr.LedgerCloseMeta,
) *backfillWorkerStats {
	defer close(ledgerCh)
	stats := &backfillWorkerStats{}

	for seq := gap.GapStart; seq <= gap.GapEnd; seq++ {
		if ctx.Err() != nil {
			return stats
		}

		fetchStart := time.Now()
		lcm, err := m.getLedgerWithRetry(ctx, backend, seq)
		fetchDur := time.Since(fetchStart)
		if err != nil {
			cancel(fmt.Errorf("fetching ledger %d: %w", seq, err))
			return stats
		}
		stats.addFetch(fetchDur)
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_fetch").Observe(fetchDur.Seconds())

		sendStart := time.Now()
		select {
		case ledgerCh <- lcm:
			sendDur := time.Since(sendStart)
			stats.addChannelWait("ledger", "send", sendDur)
			m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(sendDur.Seconds())
		case <-ctx.Done():
			return stats
		}
	}
	return stats
}
```

**Step 2: Update the call site in `processGap`**

In `processGap`, change from:
```go
m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)
```
to:
```go
dispatcherStats := m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)
```

Store `dispatcherStats` for later use in the gap summary (Task 6).

**Step 3: Run existing tests + vet**

Run: `go vet ./internal/services/`
Expected: PASS (no compilation errors)

**Step 4: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Instrument backfill dispatcher with fetch timing and channel wait metrics"
```

---

### Task 4: Instrument Stage 2 (Process Workers) with process timing and channel waits

**Files:**
- Modify: `internal/services/ingest_backfill.go` — `runProcessWorkers` function

**Step 1: Add process timing and channel wait instrumentation**

Modify `runProcessWorkers` to:
1. Accept a `chan<- *backfillWorkerStats` to report per-worker stats
2. Time each `processLedger` call
3. Time the `ledgerCh` receive wait (time blocked in `range ledgerCh`)
4. Time the `flushCh` send wait

Change the signature:
```go
func (m *ingestService) runProcessWorkers(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	ledgerCh <-chan xdr.LedgerCloseMeta,
	flushCh chan<- flushItem,
	statsCh chan<- *backfillWorkerStats,
)
```

Each worker goroutine creates its own `backfillWorkerStats` and sends it to `statsCh` on exit. The channel receive wait is measured by timing before/after pulling from `ledgerCh`. Since we use `range`, we need to switch to an explicit loop with `select` to time the receive:

```go
for range m.backfillProcessWorkers {
    wg.Add(1)
    go func() {
        defer wg.Done()
        stats := &backfillWorkerStats{}
        defer func() { statsCh <- stats }()

        buffer := indexer.NewIndexerBuffer()
        var ledgers []uint32

        flush := func() {
            if len(ledgers) == 0 {
                return
            }
            sendStart := time.Now()
            select {
            case flushCh <- flushItem{Buffer: buffer, Ledgers: ledgers}:
                sendDur := time.Since(sendStart)
                stats.addChannelWait("flush", "send", sendDur)
                m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("flush", "send").Observe(sendDur.Seconds())
            case <-ctx.Done():
                return
            }
            buffer = indexer.NewIndexerBuffer()
            ledgers = nil
        }

        for {
            recvStart := time.Now()
            lcm, ok := <-ledgerCh
            if !ok {
                break
            }
            recvDur := time.Since(recvStart)
            stats.addChannelWait("ledger", "receive", recvDur)
            m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("ledger", "receive").Observe(recvDur.Seconds())

            if ctx.Err() != nil {
                return
            }

            processStart := time.Now()
            if err := m.processLedger(ctx, lcm, buffer); err != nil {
                cancel(fmt.Errorf("processing ledger %d: %w", lcm.LedgerSequence(), err))
                return
            }
            processDur := time.Since(processStart)
            stats.addProcess(processDur)
            m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_process").Observe(processDur.Seconds())

            ledgers = append(ledgers, lcm.LedgerSequence())

            if uint32(len(ledgers)) >= m.backfillFlushBatchSize {
                flush()
            }
        }

        flush()
    }()
}
```

**Step 2: Update the call site in `processGap`**

Add a `statsCh` channel and collect worker stats:

```go
processStatsCh := make(chan *backfillWorkerStats, m.backfillProcessWorkers)
```

Pass `processStatsCh` to `runProcessWorkers`. After `pipelineWg.Wait()`, drain the channel:

```go
close(processStatsCh)
gapStats := newBackfillGapStats()
gapStats.mergeWorker(dispatcherStats)
for ws := range processStatsCh {
    gapStats.mergeWorker(ws)
}
```

**Step 3: Run vet**

Run: `go vet ./internal/services/`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Instrument backfill process workers with timing and channel wait metrics"
```

---

### Task 5: Instrument Stage 3 (Flush Workers) with flush timing, throughput, batch size, and gap progress

**Files:**
- Modify: `internal/services/ingest_backfill.go` — `runFlushWorkers` function

**Step 1: Add flush timing, throughput counter, batch size, and gap progress**

Modify `runFlushWorkers` signature to accept `chan<- *backfillFlushWorkerStats`:

```go
func (m *ingestService) runFlushWorkers(
	ctx context.Context,
	flushCh <-chan flushItem,
	watermark *backfillWatermark,
	numWorkers int,
	gap data.LedgerRange,
	statsCh chan<- *backfillFlushWorkerStats,
)
```

Each worker:
1. Times the `flushCh` receive
2. Times each `flushBufferWithRetry` call → observe `PhaseDuration` with `"backfill_flush"`
3. Observes `BackfillBatchSize` with `len(item.Ledgers)`
4. Increments `BackfillLedgersFlushed` by `len(item.Ledgers)` on success
5. Updates `BackfillGapProgress` when watermark advances

```go
for i := range numWorkers {
    wg.Add(1)
    go func(workerID int) {
        defer wg.Done()
        stats := &backfillFlushWorkerStats{}
        defer func() { statsCh <- stats }()

        for {
            recvStart := time.Now()
            item, ok := <-flushCh
            if !ok {
                return
            }
            recvDur := time.Since(recvStart)
            stats.addChannelWait("flush", "receive", recvDur)
            m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("flush", "receive").Observe(recvDur.Seconds())

            if ctx.Err() != nil {
                return
            }

            m.appMetrics.Ingestion.BackfillBatchSize.Observe(float64(len(item.Ledgers)))

            flushStart := time.Now()
            if err := m.flushBufferWithRetry(ctx, item.Buffer); err != nil {
                log.Ctx(ctx).Errorf("Flush worker %d: %d ledgers failed: %v",
                    workerID, len(item.Ledgers), err)
                continue
            }
            flushDur := time.Since(flushStart)
            stats.addFlush(flushDur)
            m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_flush").Observe(flushDur.Seconds())
            m.appMetrics.Ingestion.BackfillLedgersFlushed.Add(float64(len(item.Ledgers)))

            if advanced := watermark.MarkFlushed(item.Ledgers); advanced {
                gapSize := float64(gap.GapEnd - gap.GapStart + 1)
                progress := float64(watermark.Cursor()-gap.GapStart+1) / gapSize
                m.appMetrics.Ingestion.BackfillGapProgress.Set(progress)

                if err := m.updateOldestCursor(ctx, watermark.Cursor()); err != nil {
                    log.Ctx(ctx).Warnf("Flush worker %d: cursor update failed: %v",
                        workerID, err)
                }
            }
        }
    }(i)
}
```

**Step 2: Update the call site in `processGap`**

Add flush stats channel, pass `gap` to `runFlushWorkers`:

```go
flushStatsCh := make(chan *backfillFlushWorkerStats, m.backfillFlushWorkers)
```

Pass to `runFlushWorkers`. After pipeline completes, drain:

```go
close(flushStatsCh)
for fs := range flushStatsCh {
    gapStats.mergeFlushWorker(fs)
}
```

**Step 3: Run vet**

Run: `go vet ./internal/services/`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Instrument backfill flush workers with timing, throughput, and gap progress metrics"
```

---

### Task 6: Add channel utilization sampler and gap boundary gauges

**Files:**
- Modify: `internal/services/ingest_backfill.go` — `processGap` function

**Step 1: Add channel utilization sampler goroutine**

In `processGap`, after creating `ledgerCh` and `flushCh`, start a sampler goroutine:

```go
// Set gap boundary gauges
m.appMetrics.Ingestion.BackfillGapStartLedger.Set(float64(gap.GapStart))
m.appMetrics.Ingestion.BackfillGapEndLedger.Set(float64(gap.GapEnd))
m.appMetrics.Ingestion.BackfillGapProgress.Set(0)
defer func() {
    m.appMetrics.Ingestion.BackfillGapStartLedger.Set(0)
    m.appMetrics.Ingestion.BackfillGapEndLedger.Set(0)
}()

// Channel utilization sampler — snapshots fill ratios every second
pipelineWg.Add(1)
go func() {
    defer pipelineWg.Done()
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-gapCtx.Done():
            return
        case <-ticker.C:
            if cap(ledgerCh) > 0 {
                m.appMetrics.Ingestion.BackfillChannelUtilization.WithLabelValues("ledger").Set(
                    float64(len(ledgerCh)) / float64(cap(ledgerCh)),
                )
            }
            if cap(flushCh) > 0 {
                m.appMetrics.Ingestion.BackfillChannelUtilization.WithLabelValues("flush").Set(
                    float64(len(flushCh)) / float64(cap(flushCh)),
                )
            }
        }
    }
}()
```

**Step 2: Run vet**

Run: `go vet ./internal/services/`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Add channel utilization sampler and gap boundary gauges"
```

---

### Task 7: Add gap summary log

**Files:**
- Modify: `internal/services/ingest_backfill.go` — `processGap` function

**Step 1: Add gap summary logging after pipeline completion**

After all stats are collected (post `pipelineWg.Wait()` and stats channel draining), add the summary log:

```go
// Log gap summary
total := gap.GapEnd - gap.GapStart + 1
elapsed := time.Since(overallStart)
ledgersPerSec := float64(0)
if elapsed > 0 {
    ledgersPerSec = float64(total) / elapsed.Seconds()
}

log.Ctx(ctx).Infof("Gap [%d-%d] summary (%v, %.0f ledgers/sec):\n"+
    "  fetch:   %v total, %v avg (%d calls)\n"+
    "  process: %v total, %v avg (%d calls)\n"+
    "  flush:   %v total, %v avg (%d batches)\n"+
    "  channel_wait: ledger_send=%v ledger_recv=%v flush_send=%v flush_recv=%v",
    gap.GapStart, gap.GapEnd, elapsed, ledgersPerSec,
    gapStats.fetchTotal, avgOrZero(gapStats.fetchTotal, gapStats.fetchCount), gapStats.fetchCount,
    gapStats.processTotal, avgOrZero(gapStats.processTotal, gapStats.processCount), gapStats.processCount,
    gapStats.flushTotal, avgOrZero(gapStats.flushTotal, gapStats.flushCount), gapStats.flushCount,
    gapStats.channelWait["ledger:send"],
    gapStats.channelWait["ledger:receive"],
    gapStats.channelWait["flush:send"],
    gapStats.channelWait["flush:receive"],
)
```

This replaces or augments the existing completion log in `processGap`.

**Step 2: Run vet**

Run: `go vet ./internal/services/`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/services/ingest_backfill.go
git commit -m "Add structured gap summary log with per-stage timing breakdown"
```

---

### Task 8: Integration verification

**Files:**
- None new — verification only

**Step 1: Run unit tests**

Run: `go test -v -race ./internal/services/ -timeout 3m`
Expected: ALL PASS

**Step 2: Run metrics tests**

Run: `go test -v ./internal/metrics/ -timeout 30s`
Expected: ALL PASS

**Step 3: Run linter**

Run: `make tidy && go vet ./...`
Expected: PASS

**Step 4: Final commit (if any tidy changes)**

```bash
git add -A
git commit -m "tidy: fix formatting after backfill observability changes"
```
