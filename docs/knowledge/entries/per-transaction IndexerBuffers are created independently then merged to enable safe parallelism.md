---
context: Each goroutine in pond.Pool creates its own IndexerBuffer; no shared mutable state during fan-out; ledgerBuffer.Merge() combines results sequentially after all goroutines complete
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# per-transaction IndexerBuffers are created independently then merged to enable safe parallelism

The Indexer achieves parallel transaction processing by ensuring each goroutine operates on completely independent state. The pattern:

1. **Before dispatch:** A shared ledger-level `IndexerBuffer` is initialized
2. **During fan-out:** Each goroutine creates a fresh `IndexerBuffer` for its transaction, runs all processors, and accumulates results into its private buffer
3. **After fan-in:** The ledger-level `ledgerBuffer.Merge(txBuffer)` call is made sequentially for each completed transaction buffer

The key safety property: during step 2, goroutines share NO mutable state with each other or with the ledger-level buffer. Each goroutine's `IndexerBuffer` is private — it is created, used, and discarded within the goroutine's scope. This means the parallel phase requires no synchronization between goroutines.

The merge in step 3 does require the ledger buffer's `sync.RWMutex`, but merges are sequential (the pond.Pool's completion callback runs on the calling goroutine after `Wait()`), so the mutex is never contended during merge.

This pattern is a Go idiom: scatter work into goroutines with private accumulators, collect results sequentially after all goroutines complete. It avoids channels (which add scheduling overhead for this use case) and shared memory with locks (which would serialize the parallel work).

Since [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]], the merge step has to handle pointer references correctly — it copies canonical pointers from the tx-level buffer into the ledger-level buffer's canonical store, not duplicate structs.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — the buffer design that makes merge efficient
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — the broader architecture this is part of

Areas:
- [[entries/ingestion]]
