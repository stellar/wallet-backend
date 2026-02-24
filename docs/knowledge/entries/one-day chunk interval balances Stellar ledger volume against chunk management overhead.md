---
context: Stellar produces ~17,280 ledgers/day at 5s/ledger; 1-day chunks fit this volume comfortably while keeping the total chunk count manageable for retention and compression scheduling
type: decision
created: 2026-02-24
---

# one-day chunk interval balances Stellar ledger volume against chunk management overhead

TimescaleDB partitions each hypertable into fixed-size time slices called chunks. The chunk interval determines the time span each chunk covers. Too small an interval creates too many chunks, increasing metadata overhead and slowing down chunk-management operations (compression scheduling, retention drops). Too large an interval reduces chunk pruning granularity and slows chunk compression.

Stellar produces approximately 17,280 ledgers per day at the normal 5-second close time. One day of ledger data fits comfortably within a single chunk without producing excessively large chunk files. At this rate:
- **1-day chunks** create ~30 chunks per month — manageable for retention and compression policies
- **1-hour chunks** would create ~720 chunks/month — too many for routine chunk management
- **7-day chunks** would make time-range pruning coarser (a 2-day query could touch 2 full 7-day chunks)

The 1-day interval also aligns naturally with daily retention policy expressions ("keep 90 days" = "drop chunks older than 90 days") and with the `reconcile_oldest_cursor` job that runs on the same cadence.

---

Relevant Notes:
- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the time dimension that determines chunk boundaries
- [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]] — the compression watermark advances by chunk, so chunk size affects compression batch granularity

Areas:
- [[entries/data-layer]]
