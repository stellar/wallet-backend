---
context: Asset and contract token IDs are UUIDv5(namespace, canonical_string); ON CONFLICT DO NOTHING makes streaming ingestion idempotent without pre-querying for existing rows
type: pattern
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, ingestion]
---

# deterministic UUID v5 eliminates DB roundtrips before BatchInsert by computing primary keys client-side

Asset records (trustline assets, contract tokens) are identified by stable, canonical string keys — e.g., `USDC:ISSUER_ACCOUNT` for a classic asset or `CONTRACT_ID` for a Soroban token. Rather than querying the database for an existing UUID or auto-generating a new one at insert time, the system derives UUIDs deterministically using UUID v5 with a fixed namespace and the canonical string as the name input.

UUID v5 is a content-addressed UUID: given the same namespace and name, it always produces the same UUID. This means:
1. The ID for a known asset can be computed locally without any database interaction
2. Multiple processes/batches computing IDs for the same asset produce identical UUIDs
3. `ON CONFLICT DO NOTHING` at insert time handles the "already exists" case without needing a prior SELECT

The streaming ingestion benefit: batches can compute all their asset/token IDs locally, build the insert payload, and write it in a single `BatchInsert` call. If the batch is replayed (due to crash recovery), the deterministic IDs produce the same rows, and `ON CONFLICT DO NOTHING` silently skips duplicates — making the write path idempotent.

Without deterministic UUIDs, the system would need to either: (a) pre-query for existing IDs and handle the ID→row mapping, or (b) use auto-increment IDs and accept that replays insert duplicate rows.

---

Relevant Notes:
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — the broader idempotency strategy this pattern supports
- [[highest-OperationID-wins semantics handles concurrent batch deduplication]] — a related deduplication pattern at the operation level

Areas:
- [[entries/data-layer]]
