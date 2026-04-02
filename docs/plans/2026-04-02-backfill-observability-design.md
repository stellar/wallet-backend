# Backfill Pipeline Observability Design

**Date:** 2026-04-02
**Status:** Approved
**Goal:** Add per-stage timing, bottleneck identification, and throughput metrics to the 3-stage backfill pipeline (dispatcher -> process -> flush).

## Problem

The backfill pipeline in `internal/services/ingest_backfill.go` has minimal observability. Only `RetriesTotal` and `RetryExhaustionsTotal` with `"batch_flush"` labels exist. There is no visibility into:

- How much time each pipeline stage takes
- Whether workers are blocked waiting on channels (backpressure)
- What the throughput rate is
- Whether batches are full or partial

Without this, optimization is guesswork.

## Approach

**Approach B: Prometheus metrics + structured gap summary logs.** Uses the existing `IngestionMetrics` struct and custom registry. No new dependencies.

## New Prometheus Metrics (8 total, 14 series)

### 1. Per-Stage Durations — Reuse existing `PhaseDuration` HistogramVec

Add 3 new phase labels to the existing `wallet_ingestion_phase_duration_seconds`:

| Phase Label | Stage | Measures |
|---|---|---|
| `backfill_fetch` | Stage 1 | Time for a single successful ledger fetch (excludes retry overhead) |
| `backfill_process` | Stage 2 | Time to process one ledger into an IndexerBuffer |
| `backfill_flush` | Stage 3 | Time for one `flushBufferWithRetry` call |

### 2. Channel Wait Time Histogram (1 new metric, 4 series)

```
wallet_ingestion_backfill_channel_wait_seconds {channel, direction}
```

- **Type:** Histogram
- **Labels:** `channel` (`"ledger"`, `"flush"`) x `direction` (`"send"`, `"receive"`)
- **Buckets:** `[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30]`
- **Diagnostic:** High send wait = downstream bottleneck. High receive wait = upstream bottleneck.

### 3. Channel Utilization Gauges (1 new metric, 2 series)

```
wallet_ingestion_backfill_channel_utilization_ratio {channel}
```

- **Type:** GaugeVec
- **Labels:** `channel` (`"ledger"`, `"flush"`)
- **Sampled:** Every 1 second by a goroutine scoped to the gap's context
- **Range:** 0.0 (empty) to 1.0 (full)
- **Diagnostic:** Sustained 1.0 = downstream can't keep up. Sustained 0.0 = upstream is slow.

### 4. Backfill Throughput Counter (1 new metric, 1 series)

```
wallet_ingestion_backfill_ledgers_flushed_total
```

- **Type:** Counter
- **Incremented:** By batch size when flush worker completes
- **Query:** `rate(...[5m])` for ledgers/sec

### 5. Backfill Batch Size Histogram (1 new metric, 1 series)

```
wallet_ingestion_backfill_batch_size
```

- **Type:** Histogram
- **Buckets:** `LinearBuckets(10, 10, 10)` -> 10, 20, ..., 100
- **Diagnostic:** Most batches at 100 = healthy. Many partial batches = process workers starved.

### 6. Gap Progress Gauge (1 new metric, 1 series)

```
wallet_ingestion_backfill_gap_progress_ratio
```

- **Type:** Gauge
- **Updated:** When watermark advances, set to `(cursor - start) / (end - start)`
- **Diagnostic:** Steadily climbing = healthy. Plateau = stalled pipeline.

### 7. Gap Boundary Gauges (2 new metrics, 2 series)

```
wallet_ingestion_backfill_gap_start_ledger
wallet_ingestion_backfill_gap_end_ledger
```

- **Type:** Gauge
- **Set:** When gap processing begins; reset to 0 when done
- **Diagnostic:** Shows which ledger range is active for dashboard correlation.

### Cardinality Summary

| Metric | Series |
|---|---|
| PhaseDuration (3 new labels) | 3 |
| channel_wait_seconds | 4 |
| channel_utilization_ratio | 2 |
| backfill_ledgers_flushed_total | 1 |
| backfill_batch_size | 1 |
| backfill_gap_progress_ratio | 1 |
| gap_start/end_ledger | 2 |
| **Total** | **14** |

## Structured Logging

### Gap Summary Log

Emitted at gap completion by `processGap`. Each pipeline stage accumulates local stats (no contention), reported at shutdown:

```
Gap [1000-50000] complete in 2m34s:
  fetch:   1m12s total (avg 1.4ms/ledger)
  process: 48s total (avg 0.98ms/ledger)
  flush:   34s total (avg 340ms/batch, 145 batches)
  channel_wait: ledger_send=2.1s ledger_recv=45s flush_send=12s flush_recv=0.3s
  throughput: 318 ledgers/sec
```

### Implementation Pattern

A `backfillStats` struct with per-worker accumulators:

- Each worker goroutine maintains a local `backfillWorkerStats` (no mutex needed)
- Workers report stats through a channel or at function return
- `processGap` aggregates all worker stats into the summary log

## Channel Utilization Sampler

A lightweight goroutine started per gap:

- Ticks every 1 second
- Reads `len(ledgerCh)/cap(ledgerCh)` and `len(flushCh)/cap(flushCh)`
- Sets the gauge values
- Exits when the gap context is cancelled

Since Go channels expose `len()` and `cap()` as non-blocking reads, this has negligible overhead.

## Bottleneck Identification Cheat Sheet

| Symptom | Bottleneck | Action |
|---|---|---|
| `ledger/send` wait high, `ledger/receive` wait low | Stage 2 (process) | Increase `backfillProcessWorkers` |
| `flush/send` wait high, `flush/receive` wait low | Stage 3 (flush/DB) | Increase `backfillFlushWorkers` or tune DB |
| `ledger/receive` high, `flush/receive` high | Stage 1 (fetch/RPC) | RPC is slow; nothing to tune internally |
| `ledger` utilization ~1.0 | Stage 2 can't keep up | More process workers or increase `backfillLedgerChanSize` |
| `flush` utilization ~1.0 | Stage 3 can't keep up | More flush workers or increase `backfillFlushChanSize` |
| Batch sizes mostly partial | Process workers starved | Stage 1 fetch is the bottleneck |

## Files Modified

| File | Changes |
|---|---|
| `internal/metrics/ingestion.go` | Add 8 new metric fields + registration |
| `internal/services/ingest_backfill.go` | Instrument all 3 stages, add channel sampler, add stats aggregation, add gap summary log |
| `internal/services/backfill_stats.go` | New file: `backfillStats` and `backfillWorkerStats` types |

## Non-Goals

- OpenTelemetry tracing
- Per-ledger log lines (too noisy)
- Alerting rules (separate concern, can be added to Grafana later)
