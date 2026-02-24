---
context: txByHash stores one *Transaction pointer per unique tx hash; participants reference same pointer instead of copying 10-50KB XDR structs; thread safety via sync.RWMutex
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs

`IndexerBuffer` uses a two-layer memory architecture to avoid the memory cost of duplicating large transaction structs when multiple participants reference the same transaction:

**Layer 1 — Canonical storage:**
- `txByHash: map[hash]*Transaction` — one pointer per unique transaction
- `opByID: map[id]*Operation` — one pointer per unique operation

**Layer 2 — Participant mappings:**
- `participantsByToID: map[toID] → Set[string]` — Stellar addresses per transaction
- `participantsByOpID: map[opID] → Set[string]` — Stellar addresses per operation

When multiple participants (accounts, signers) interact with the same transaction, they all reference the SAME canonical pointer in `txByHash` rather than storing duplicate copies. Transaction structs contain XDR-encoded fields that are typically 10-50+ KB each, so deduplication matters significantly when a transaction has many participants.

**Thread safety:** All public methods use `sync.RWMutex`. Per-transaction buffers created in parallel goroutines do not share state — each goroutine has its own `IndexerBuffer`. The ledger-level buffer uses the mutex only during the sequential merge phase.

**Dedup maps** (separate from canonical storage): Trustline, account, and SAC balance changes use separate maps with highest-OperationID-wins semantics — these are not pointer-sharing but last-write-wins deduplication.

Since [[IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill]], the buffer is designed for reuse across batches, which is why clearing resets but does not reallocate.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill]] — optimization that complements the pointer pattern
- [[highest-OperationID-wins semantics handles concurrent batch deduplication]] — the dedup strategy in the separate dedup maps
- [[per-transaction IndexerBuffers are created independently then merged to enable safe parallelism]] — how these buffers are used in the parallel model

Areas:
- [[entries/ingestion]]
