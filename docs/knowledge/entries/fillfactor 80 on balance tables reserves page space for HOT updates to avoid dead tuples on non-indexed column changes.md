---
context: HOT (Heap Only Tuple) updates require the new version to fit on the same page as the old version; fillfactor 80 leaves 20% page slack for in-place updates, reducing dead tuple accumulation and autovacuum pressure
type: insight
created: 2026-02-24
---

# fillfactor 80 on balance tables reserves page space for HOT updates to avoid dead tuples on non-indexed column changes

PostgreSQL's HOT (Heap Only Tuple) update optimization allows an update to a row to avoid creating a new index entry when only non-indexed columns change. For this optimization to apply, the new version of the row must fit on the same page as the original row. If the page is full, PostgreSQL must move the new row to a different page, breaking the HOT chain and creating both a dead tuple in the old location and a new index entry.

Balance tables are updated every time the corresponding ledger data is ingested — effectively once per ledger close (~5 seconds). The updated columns (balance amounts) are not indexed. This is exactly the HOT update scenario.

Setting `fillfactor = 80` on balance tables reserves 20% of each page for updates. PostgreSQL inserts new rows only into the 80% target fill, leaving the remaining space available for in-place updates. When a balance row is updated, the new version can almost always fit in the reserved space on the same page — keeping the HOT optimization effective and preventing dead tuple accumulation.

Without fillfactor tuning, frequent balance updates would quickly fill pages, break HOT chains, and generate dead tuples faster than autovacuum can clean them, causing table bloat and degraded query performance over time.

---

Relevant Notes:
- [[balance tables use regular PostgreSQL tables not hypertables because they store current state not time-series events]] — the architecture decision that makes this tuning necessary
- [[aggressive autovacuum tuning on balance tables prevents stale statistics and dead tuple accumulation from per-ledger updates]] — the complementary autovacuum tuning that handles dead tuples that do accumulate

Areas:
- [[entries/data-layer]]
