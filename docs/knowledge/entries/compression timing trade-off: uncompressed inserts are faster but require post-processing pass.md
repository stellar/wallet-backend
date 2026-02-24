---
context: Historical backfill inserts uncompressed for write throughput; progressiveRecompressor runs post-batch to recover compression; live ingestion writes into already-compressed chunks differently
type: insight
status: current
subsystem: ingestion
areas: [ingestion, data-layer, timescaledb]
created: 2026-02-24
---

# compression timing trade-off: uncompressed inserts are faster but require post-processing pass

TimescaleDB compression improves query performance and reduces storage, but writing into compressed chunks requires decompression, modification, and recompression — significantly slower than writing to uncompressed chunks.

Historical backfill makes the explicit trade-off to sacrifice write-time compression for throughput:
- **Inserts go into uncompressed chunks** — fast writes, no compression overhead
- **`progressiveRecompressor` runs as a background process** — compresses chunks after batches complete, using a watermark to ensure in-order compression

The cost of this approach:
- Temporary storage increase during the backfill window (uncompressed data is 5-10x larger)
- An additional processing phase after batch completion
- A final verification pass to catch boundary chunks

The benefit:
- Substantially higher ingestion throughput during bulk backfill
- No risk of writing into already-compressed chunks (which would trigger chunk decompression, a blocking operation)

Live ingestion doesn't face this trade-off in the same way — it processes one ledger at a time into the latest (presumably uncompressed) time window. Compression of older chunks happens via TimescaleDB's scheduled compression policy, not inline.

The implication for operators: a running historical backfill will temporarily show elevated storage usage and the oldest chunks will be in an uncompressed state. This is expected behavior, not a problem.

---

The migration that creates a hypertable also creates the initial compression policy via `add_compression_policy()`. The `configureHypertableSettings()` function called at runtime does NOT create a new policy — it calls `alter_job()` to update the existing policy's schedule interval and `compress_after` threshold. This two-step design (migration creates, runtime configures) means the compression policy always exists after migration, but its operational parameters (how aggressively to compress, how far back) can be adjusted without schema migrations.

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]] — the mechanism that resolves this trade-off
- [[entries/data-layer]] — this trade-off is rooted in TimescaleDB chunk compression behavior; the data layer reference doc covers the compression policies and chunk interval design that this trade-off operates within

Areas:
- [[entries/ingestion]] | [[entries/data-layer]]
