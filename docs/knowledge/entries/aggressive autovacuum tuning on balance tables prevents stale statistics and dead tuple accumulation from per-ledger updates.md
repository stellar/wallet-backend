---
context: autovacuum_vacuum_scale_factor=0.02, autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_cost_delay=0, autovacuum_vacuum_cost_limit=1000 applied per-table via ALTER TABLE
type: insight
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, timescaledb]
---

# aggressive autovacuum tuning on balance tables prevents stale statistics and dead tuple accumulation from per-ledger updates

PostgreSQL's default autovacuum triggers (scale_factor 0.20 for VACUUM, 0.10 for ANALYZE) are designed for tables that change infrequently. Balance tables update every ~5 seconds (once per ledger close). With default settings on a large balance table, autovacuum would trigger too rarely, allowing dead tuples and stale statistics to accumulate significantly before a vacuum runs.

The tuning applied to balance tables:
- **`autovacuum_vacuum_scale_factor = 0.02`** — triggers VACUUM when 2% of rows have dead tuples (vs default 20%)
- **`autovacuum_analyze_scale_factor = 0.01`** — triggers ANALYZE when 1% of rows have changed (vs default 10%), keeping statistics fresh for the planner
- **`autovacuum_vacuum_cost_delay = 0`** — removes the throttling delay between vacuum I/O bursts, allowing vacuum to run at full speed
- **`autovacuum_vacuum_cost_limit = 1000`** — raises the I/O cost budget per vacuum cycle (vs default 200), allowing larger work units

The combined effect: autovacuum triggers more aggressively, runs faster, and performs larger work units when it does run. This keeps dead tuple counts low, statistics current, and avoids the table bloat that would otherwise occur from high-frequency updates.

The cost is increased background I/O on balance tables — an acceptable trade-off given that balance queries must remain fast for every API request.

---

Relevant Notes:
- [[fillfactor 80 on balance tables reserves page space for HOT updates to avoid dead tuples on non-indexed column changes]] — reduces dead tuple creation; autovacuum tuning handles the remainder
- [[balance tables use regular PostgreSQL tables not hypertables because they store current state not time-series events]] — the architecture decision that creates this maintenance requirement

Areas:
- [[entries/data-layer]]
