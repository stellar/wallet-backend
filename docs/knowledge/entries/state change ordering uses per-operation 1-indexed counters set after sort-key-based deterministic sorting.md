---
context: Raw processor output is unordered within an operation; sort key is derived from category+reason+address to produce canonical per-operation ordering before indexing
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# state change ordering uses per-operation 1-indexed counters set after sort-key-based deterministic sorting

Within a single operation, multiple `StateChange` records can be produced by different processors. The `order` field on `StateChange` is not set during construction — it is assigned in a post-processing step after all processors have run for an operation.

The ordering procedure:

1. Collect all `StateChange` records produced for an operation
2. Sort them by a canonical sort key: `Category + Reason + Address` (lexicographic)
3. Assign 1-indexed `order` values in the sorted sequence: first=1, second=2, etc.

This deterministic sort-then-index approach ensures that for any given operation, the same set of state changes always produces the same `order` values regardless of which goroutine or processor produced them first. This is critical for idempotent re-processing: re-ingesting a ledger must produce identical records, and `order` is part of the primary key for certain queries.

The 1-indexed (not 0-indexed) counter matches Stellar's convention for operation ordering within transactions.

---

Relevant Notes:
- [[StateChangeBuilder provides a fluent builder with Clone for constructing branching state change trees from a common base]] — builder that constructs the records before ordering
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — idempotency depends on deterministic ordering

Areas:
- [[entries/ingestion]]
