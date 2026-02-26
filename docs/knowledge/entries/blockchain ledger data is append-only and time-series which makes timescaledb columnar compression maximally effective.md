---
context: Blockchain ledger records are permanently committed and never updated — identical to the data shape columnar compression is designed to exploit; this property makes compression ratios high rather than merely adequate
type: insight
status: active
created: 2026-02-26
areas: [data-layer, timescaledb, blockchain, compression]
---

# Blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective

## Context

TimescaleDB's columnar compression was selected as the primary mechanism for making blockchain history storage feasible. The effectiveness of columnar compression is not uniform across all data — it depends on the structural properties of the data being compressed. Understanding why compression performs well on blockchain data requires examining those properties directly.

## Detail

Blockchain ledger data has two structural properties that maximize columnar compression effectiveness:

**Append-only** — Every transaction, operation, and state change in the Stellar ledger is a permanently committed historical fact. Records are written once and never modified. This means the compressed hypertable chunks never need to be decompressed for in-place updates — they are write-once, read-many.

**Time-series** — Ledger records are naturally ordered by ledger sequence number (a monotonically increasing time proxy). Every record has a creation ledger, and records are ingested in ledger order. This means data within any time chunk is coherent — it covers a bounded, contiguous historical period.

These two properties combine to enable high compression ratios:

Columnar storage groups all values for a given column across many rows into contiguous byte arrays. When values in a column repeat frequently or follow predictable patterns, compression algorithms (LZ4, zstd) achieve high ratios. Blockchain data has this structure naturally:

- `asset_code` and `asset_issuer` are heavily repeated — most operations involve a small number of popular assets.
- `account_id` columns repeat within accounts' history windows.
- `ledger_sequence` values within a chunk span a narrow integer range.
- `operation_type` is a small enum with few distinct values.
- `successful` is nearly always `true`.

Because the data is never updated, TimescaleDB can compress a chunk and leave it compressed indefinitely. There is no update-triggered decompression cycle that would degrade effective ratios over time. The contrast makes this concrete: [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — balance rows are mutated on every ledger close, which is precisely the access pattern that breaks columnar compression and requires row-oriented storage instead.

## Implications

- Compression should always be enabled on history hypertables in production. Running without compression works (the data is still valid) but defeats the primary storage efficiency rationale.
- Compression order should match the dominant sort order — for account-history queries, ordering by `account_id, ledger_sequence` before compression maximizes the runs of repeated account_id values.
- Backfill scenarios require managing compression actively: chunks arriving out-of-time-order or through bulk insert need to be explicitly compressed after ingestion.
- Vanilla Postgres tables cannot replicate this: even with table partitioning by date, row-oriented storage cannot compress columnar runs, so the storage advantage disappears.

## Source

[[why timescaleDB over vanilla Postgres]] (line 5) — original description of columnar compression as pillar 1 of the TimescaleDB decision

## Relevant Notes

- Since [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]], this insight is the structural justification for that decision — append-only time-series data is why compression works, not merely that it was tried.
- [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — backfill's safety of disabling durability guarantees relies on the same append-only property: if ingestion fails, re-deriving from source is safe precisely because records are never modified.
- [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] — managing compression during backfill is necessary because chunks arrive in bulk; this is operational consequence of the append-only model.
- [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] — append-only data makes chunk-drop retention safe: there are no forward references or updates from newer chunks pointing back to old ones, so dropping a time range is always a clean operation.
- [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — exemplifies the negative case: current-state tables lack the two structural properties argued here, which is exactly why they stay as row-oriented Postgres tables rather than compressed hypertables.
- [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — the append-only property enables COPY for backfill: because historical records are never duplicated on fresh insertion into gap ranges, COPY's inability to handle ON CONFLICT is not a limitation.
