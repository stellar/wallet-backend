---
context: GetByHash queries use a bloom filter sparse index on transactions.hash; without it, hash lookup would require scanning all chunks since hash is not correlated with the time partition
type: insight
created: 2026-02-24
---

# bloom filter sparse index on transactions hash enables chunk-level hash lookup pruning without full scan

Transaction hashes are opaque fixed-length identifiers with no correlation to time or ledger sequence. A query for `GetByHash` cannot exploit the hypertable's time partitioning because the hash tells the planner nothing about which chunk(s) contain the matching row.

Without any additional indexing, a hash lookup would require scanning all chunks — equivalent to a full hypertable scan.

TimescaleDB's bloom filter sparse index (`create_hypertable_sparse_index`) addresses this. A bloom filter probabilistically tests whether a chunk *might* contain a given hash value. For chunks where the bloom filter returns "definitely not", the planner skips them entirely. For chunks where it returns "might be", the planner does a regular scan within that chunk.

The trade-off:
- **False negatives are impossible** — if the hash is in the chunk, the bloom filter will always return "might be"
- **False positives are possible** — occasionally a chunk is scanned unnecessarily, but this is rare and bounded
- **Storage overhead** — bloom filter data is stored per chunk; minimal compared to the chunks themselves

This is complementary to the time-partition pruning: time-bounded queries prune by chunk range, hash lookups prune by bloom filter. Together they cover the two main access patterns on `transactions`.

---

Relevant Notes:
- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the primary pruning mechanism that bloom filter complements
- [[raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate]] — why TimescaleDB-specific features like this are used directly

Areas:
- [[entries/data-layer]]
