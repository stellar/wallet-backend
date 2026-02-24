---
context: Clear() resets all maps and slices (length = 0) but does not reallocate backing arrays; avoids GC pressure during sequential batch processing where the same buffer is reused
type: gotcha
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill

`IndexerBuffer.Clear()` resets all internal maps and slices to empty but intentionally does NOT release the allocated backing memory. This is a performance optimization for backfill batch processing.

During backfill, each batch processes potentially hundreds of ledgers. After each ledger's data is flushed to the database, `buffer.Clear()` is called to reset the buffer for the next ledger. If `Clear()` triggered garbage collection by reallocating all maps and slices, the GC pressure across hundreds of ledger cycles per batch would be significant.

By preserving the backing arrays (Go slices retain their capacity when length is set to 0), subsequent ledger processing can reuse the same memory without allocation. Maps are a trickier case — Go maps don't shrink when cleared, which means the same backing memory is reused automatically.

The behavioral implication for developers: after calling `Clear()`, the buffer appears empty (all length checks return 0, all map lookups find nothing), but the underlying memory is not freed. This is intentional — it is not a memory leak in the context of long-running batch processing.

If you are debugging apparent memory growth during backfill, this is the expected behavior: the buffer holds onto its peak-ledger-size allocation across the entire batch.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — the broader buffer design
- [[backfill batch size and DB insert batch size are separate concerns for memory bounding]] — the batch sizing that determines how often Clear() is called

Areas:
- [[entries/ingestion]]
