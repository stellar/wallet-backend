---
description: Each transaction produces an IndexerBuffer merged into ledger-level buffer after all complete; within-transaction processor fan-out is sequential; parallelism is at transaction granularity
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, indexer, concurrency, pond-pool, performance]
---

# Indexer processes transactions in parallel within a ledger using pond.Pool with per-transaction buffers

## Context

A ledger contains many transactions (potentially hundreds). Processing them sequentially is the bottleneck for high-throughput ingestion. Each transaction is independent, making parallelism straightforward.

## Detail

`Indexer.ProcessLedgerTransactions()` submits each transaction to a `pond.Pool` with unbounded workers (one goroutine per transaction). Each goroutine creates its own `IndexerBuffer`, processes the transaction through all 9 processors, and returns the buffer.

After all goroutines complete, the ledger-level buffer merges all per-transaction buffers in sequence: `ledgerBuffer.Merge(txBuffer)`.

**Within each transaction**, the processors run sequentially per operation:
- 3 `OperationProcessorInterface` processors run one by one per operation.
- 5 `LedgerChangeProcessor` processors run one by one per operation.
- `TokenTransferProcessor` runs at the transaction level (not per-operation).

Adding a goroutine pool per-operation (3 processors × N operations) adds overhead that exceeds the benefit for small processor counts.

## Implications

- The unbounded pool means heavy ledgers (many transactions) create many goroutines. Monitor the `indexer` pool metrics in Prometheus for worker count spikes.
- Per-transaction buffer isolation means processors in different transactions cannot see each other's writes during processing — they only merge after all complete.
- Merge order is deterministic (goroutines complete, then merge happens sequentially) but the order within the buffer depends on merge sequence. The dedup semantics handle any ordering issues.

## Source

`internal/indexer/indexer.go:ProcessLedgerTransactions()`
`internal/indexer/indexer_buffer.go:Merge()`

## Related

The memory efficiency of parallel per-transaction buffers depends on [[canonical pointer pattern in indexerbuffer avoids duplicating large xdr structs across participants]] — without shared pointers, running many goroutines simultaneously would multiply XDR struct duplication.

After all goroutines complete, the merged buffer passes through [[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] to resolve any intra-ledger conflicts before persistence.

relevant_notes:
  - "[[canonical pointer pattern in indexerbuffer avoids duplicating large xdr structs across participants]] — enables this: shared canonical pointers make running many per-transaction goroutines memory-efficient; without them, parallelism would cause XDR duplication"
  - "[[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] — extends this: after parallel goroutines merge their buffers, the dedup maps resolve conflicts that may arise when multiple transactions affect the same account or trustline"
