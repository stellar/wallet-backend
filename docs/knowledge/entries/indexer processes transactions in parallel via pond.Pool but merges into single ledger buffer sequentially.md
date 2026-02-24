---
context: Unbounded worker pool for per-tx processing (each tx is independent); merge back is sequential to avoid lock contention; produces a ledger-level IndexerBuffer as output
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially

The Indexer's `ProcessLedgerTransactions()` uses a fan-out/fan-in model:

**Fan-out:** All transactions in a ledger are dispatched to a `pond.Pool` with unbounded workers. Each goroutine gets a single transaction, creates its own `IndexerBuffer`, runs all processors against that transaction, and produces a per-transaction buffer.

**Fan-in:** After all goroutines complete, the per-transaction buffers are merged into the single ledger-level `IndexerBuffer` sequentially via `ledgerBuffer.Merge()`.

The parallelism is safe because each goroutine works on a separate transaction and a separate `IndexerBuffer` — there is no shared mutable state during the fan-out phase. Transactions in a Stellar ledger are independent: they do not share mutable state with each other at the ledger level.

The merge is sequential by design. Adding a merge pool would create lock contention on the ledger buffer that would likely exceed the savings from parallelizing the merge itself (merges are fast; the expensive work is processing).

Since [[per-transaction IndexerBuffers are created independently then merged to enable safe parallelism]], the independence of per-transaction buffers is what makes this architecture correct.

The unbounded pool is appropriate here because the number of transactions per ledger is bounded (Stellar protocol limits) and each transaction processes quickly (microseconds to low milliseconds for the processing step).

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[per-transaction IndexerBuffers are created independently then merged to enable safe parallelism]] — the buffer design that enables this parallel model
- [[nine specialized processors run per transaction in the indexer fan-out]] — what runs inside each goroutine
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — the buffer structure being merged

Areas:
- [[entries/ingestion]]
