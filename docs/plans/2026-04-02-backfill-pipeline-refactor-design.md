# Backfill Pipeline Refactor: High-Throughput 3-Stage Design

**Date:** 2026-04-02
**Branch:** optimize-backfill-code
**Scope:** `internal/services/ingest_backfill.go` — complete refactor

## Problem

The current backfill implementation creates one `optimizedStorageBackend` per batch, processes ledgers sequentially within each batch, and performs stop-the-world DB flushes. These design choices leave significant throughput on the table:

- Redundant S3 backends for contiguous ranges within the same gap
- No pipelining between ledger processing and DB writes
- Single-writer flushes underutilize TimescaleDB's chunk-parallel write capability

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Backend per gap | 1 backend per gap | Avoids redundant S3 connections; `optimizedStorageBackend` already handles sequential prefetch internally |
| Gap concurrency | Sequential | Typically 1 large gap in practice; adding a semaphore later is trivial |
| Processing parallelism | Fan-out via bounded channel | Idiomatic Go pipeline; backpressure automatic via bounded channels |
| Indexer pool | Shared pool, per-worker groups | `pond.NewGroupContext` allows concurrent groups on the same pool; natural concurrency bound |
| Flush parallelism | M concurrent flush workers, 5 parallel COPYs each | TimescaleDB docs recommend parallel COPY; different time ranges hit different chunks, minimizing lock contention |
| Transaction wrapping | None (fire-and-forget COPYs) | Backfill is idempotent via UniqueViolation handling; atomicity buys nothing |
| Cursor management | Watermark tracker (bitmap) | Flush order is non-deterministic; watermark advances cursor only when contiguous range is safe |
| Error: flush failure | Retry with exponential backoff | Transient DB issues (load, connections) are common and self-resolving |
| Error: download/process | Fail-fast, cancel gap context | Indicates bad data or config; won't self-resolve |
| Progressive recompressor | Removed | Will be re-added in a separate PR |

## Architecture

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BackfillProcessWorkers` | `runtime.NumCPU()` | Stage 2 processing workers per gap |
| `BackfillFlushWorkers` | 4 | Stage 3 concurrent flush workers |
| `BackfillFlushBatchSize` | 100 | Ledgers accumulated per buffer before flush |
| `BackfillLedgerChannelSize` | 256 | Bounded channel: dispatcher to process workers |
| `BackfillFlushChannelSize` | 8 | Bounded channel: process workers to flush workers |

### Startup Validation

```
requiredConns = BackfillFlushWorkers * 5 + 5 (headroom)
if pgxpool.MaxConns < requiredConns → error
```

### Pipeline Flow (per gap, gaps processed sequentially)

```
  1 Backend (optimizedStorageBackend for full gap range)
       |
       v
  1 Dispatcher goroutine
  calls GetLedger(seq++) sequentially
       |
       | ledgerCh (bounded, size 256)
       v
  N Process Workers (default: NumCPU)
  +-------------------------------------+
  | Each worker:                        |
  |  - Owns its own IndexerBuffer      |
  |  - Pulls ledgers from ledgerCh     |
  |  - Calls processLedger() using     |
  |    shared indexer pool for tx       |
  |    parallelism within each ledger  |
  |  - After FlushBatchSize ledgers,   |
  |    sends buffer + ledger set to    |
  |    flushCh                          |
  |  - Creates fresh buffer, continues |
  +-------------------------------------+
       |
       | flushCh (bounded, size 8)
       v
  M Flush Workers (default: 4)
  +-------------------------------------+
  | Each flush:                         |
  |  - 5 parallel COPYs via errgroup   |
  |    (transactions, tx_accounts,     |
  |    operations, op_accounts,        |
  |    state_changes)                  |
  |  - Each COPY: own pool conn,      |
  |    synchronous_commit=off          |
  |  - UniqueViolation = success       |
  |  - Retry with exponential backoff  |
  |  - On success: report ledger set   |
  |    to watermark tracker            |
  +-------------------------------------+
       |
       v
  Watermark Tracker
  +-------------------------------------+
  |  - Bitmap of flushed ledger seqs   |
  |  - Advances cursor to highest      |
  |    contiguous flushed ledger       |
  |  - Updates cursor in DB            |
  +-------------------------------------+
```

### Backpressure

All automatic via bounded channels — no manual flow control:

- DB slow -> flushCh fills -> workers block on send -> ledgerCh fills -> dispatcher blocks -> backend pauses S3 downloads
- Processing slow -> ledgerCh stays full -> dispatcher blocks -> backend paces itself

### Error Handling

- **Flush failure:** retry with exponential backoff (max 10 attempts, capped at 30s). After exhaustion, cancel gap context.
- **Download/processing failure:** fail-fast, cancel gap context immediately. All in-flight work for the gap drains.
- **Gap isolation:** each gap has its own context. A failed gap does not prevent subsequent gaps from running.
- **Partial progress:** watermark tracker records which ledgers were successfully flushed. On restart, `calculateBackfillGaps` detects remaining gaps and only re-processes what's missing.

### Watermark Tracker Detail

The watermark tracker replaces per-batch cursor updates. Since M flush workers process buffers concurrently, and each buffer contains non-contiguous ledgers (workers pull from a shared channel), flush completion order is non-deterministic.

The tracker maintains a bitmap indexed by ledger sequence. When a flush worker reports success, it marks those ledger sequences as done. The tracker scans forward from the current cursor position to find the highest contiguous flushed ledger and updates the DB cursor to that value.

Cursor updates are batched (not per-flush) to avoid excessive DB writes.

### Connection Budget

At peak throughput with default config:

| Consumer | Connections |
|----------|-------------|
| Flush workers (4 x 5 COPYs) | 20 |
| Watermark cursor updates | 1 |
| Backend (S3, not DB) | 0 |
| Headroom | 4 |
| **Total** | **25** |

Pool max connections should be >= 25 for default config.

## What's NOT In Scope

- **Progressive recompressor** — removed entirely, will be re-added in a separate PR
- **Upsert operations** — skipped in backfill (balance tables represent current state, not historical)
- **Gap concurrency** — gaps run sequentially; concurrency can be added later via semaphore if needed

## Key Changes from Current Implementation

| Current | New |
|---------|-----|
| 1 backend per batch | 1 backend per gap |
| Independent batch workers (pond pool) | 3-stage pipeline with channels |
| Stop-the-world flush within batch | Decoupled flush workers, processing continues |
| 1 flush at a time per batch | M concurrent flush workers |
| Wrapping transaction around COPYs | Fire-and-forget parallel COPYs |
| Per-batch cursor update | Watermark-based contiguous cursor |
| Progressive recompressor integrated | Removed (separate PR) |
