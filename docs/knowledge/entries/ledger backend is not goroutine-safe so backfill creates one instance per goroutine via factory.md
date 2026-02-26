---
description: LedgerBackendFactory closure creates a new backend per goroutine in pond.Pool during backfill; avoids mutex on hot path; live ingestion uses a single instance for sequential processing
type: pattern
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, backfill, concurrency, factory-pattern, ledger-backend]
---

# Ledger backend is not goroutine-safe so backfill creates one instance per goroutine via factory

## Context

Parallel backfill uses a `pond.Pool` to process batches concurrently. Each batch goroutine needs to fetch ledgers from the Stellar history archive or RPC. The `LedgerBackend` interface is not goroutine-safe (it maintains internal state like cursor position and buffered data).

## Detail

In `setupDeps()`, a `LedgerBackendFactory` closure is created:

```go
ledgerBackendFactory := func(ctx context.Context) (ingest.LedgerBackend, error) {
    return ingest.NewLedgerBackend(ctx, cfg)
}
```

Each goroutine in the `pond.Pool` calls this factory to get its own isolated `LedgerBackend` instance. The factory captures `cfg` by value, so each call produces a fresh backend configured identically but with no shared state.

Live ingestion uses a single `LedgerBackend` instance (sequential processing, no concurrency needed).

## Implications

- Adding a new parallel ingestion feature must follow the factory pattern — never share a single `LedgerBackend` across goroutines.
- The factory approach is cheaper than a mutex: each goroutine has its own backend, so there's no lock contention on the hot path.
- Backend types (RPC vs Datastore) are irrelevant to this pattern — both implement the same interface and are created via the same factory.

## Source

`internal/ingest/ingest.go:setupDeps()` — factory closure definition
`internal/services/ingest_backfill.go:processBackfillBatchesParallel()` — per-goroutine factory call

## Related

The per-goroutine backends feed into the parallel pool described in [[indexer processes transactions in parallel within a ledger using pond pool with per-transaction buffers]] — each goroutine's backend fetches ledgers that the indexer then processes through per-transaction buffers.

The factory pattern is the goroutine-safety complement to [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — both entries describe infrastructure for the same parallel backfill path; this one ensures safe ledger fetching, that one ensures fast writes.

relevant_notes:
  - "[[indexer processes transactions in parallel within a ledger using pond pool with per-transaction buffers]] — grounds this: the per-goroutine backend factory and the per-transaction buffer pool are two levels of the same parallel processing stack"
  - "[[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — synthesizes with this: factory pattern and synchronous_commit=off are both backfill-specific infrastructure; this handles goroutine-safe reading, that handles fast writing"
