---
context: Batch 0 must complete before any compression starts; watermark advances as contiguous batches complete; final verification pass catches boundary chunks missed by watermark logic
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, data-layer, timescaledb]
created: 2026-02-24
---

# progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill

Historical backfill inserts data into TimescaleDB chunks that are initially uncompressed, because uncompressed writes are significantly faster than writing into compressed chunks (compressed chunks require decompression, modification, and recompression). The `progressiveRecompressor` runs as a background process during backfill to recover the storage and query efficiency of compression.

The watermark mechanism works as follows:

1. **Batch 0 must complete first** — compression cannot start until the lowest-numbered batch (covering the earliest ledger range) completes. This ensures we don't compress chunks that might still receive earlier data.
2. **Watermark advances as contiguous batches complete** — as batch N completes, if all batches from 0 to N are done, the watermark advances and chunks covered by those batches are eligible for compression.
3. **`progressiveRecompressor.Wait()` is called after all parallel batches complete** — this ensures any remaining uncompressed chunks are compressed before the historical backfill function returns.
4. **Final verification pass** — catches boundary chunks that the watermark-based logic may have missed (e.g., a chunk that spans the boundary between two batches).

The reasoning for this approach: if you compress as batches complete out-of-order (e.g., batch 5 finishes before batch 3), and then batch 3 later tries to insert data into a now-compressed chunk range, you get compression write amplification. The watermark enforces in-order compression.

Since [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]], this is the deliberate design — optimize for write throughput during bulk ingestion, then recover read performance via post-processing.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]] — the trade-off this pattern resolves
- [[backfill batch size and DB insert batch size are separate concerns for memory bounding]] — the batch structure that the watermark operates on

Areas:
- [[entries/ingestion]]
