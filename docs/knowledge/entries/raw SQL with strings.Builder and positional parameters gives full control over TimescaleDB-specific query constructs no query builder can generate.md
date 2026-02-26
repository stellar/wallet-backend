---
context: All data-layer queries use strings.Builder with $N positional params; no ORM or query builder; this is necessary for MATERIALIZED CTEs, decomposed cursor conditions, and LATERAL joins that query builders cannot express
type: decision
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, timescaledb]
---

# raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate

The data layer writes all queries as raw SQL strings constructed with `strings.Builder` and positional `$N` parameters. There is no ORM, no query builder (like GORM, sqlc, or squirrel), and no named parameter interpolation.

This is a deliberate design choice driven by the TimescaleDB-specific query patterns required for correctness and performance:

- **MATERIALIZED CTE** — the `WITH ... AS MATERIALIZED` hint forces the planner to execute the CTE as a separate step, enabling ChunkAppend on join tables. Most query builders do not expose this hint.
- **Decomposed cursor conditions** — pagination requires `(a > $1) OR (a = $1 AND b > $2)` decomposed form rather than `(a, b) > ($1, $2)` tuple comparison. The tuple form is opaque to the TimescaleDB columnar filter engine and disables chunk pruning. No query builder generates the decomposed form automatically.
- **LATERAL joins** — used for O(1) parent-row lookup, rarely supported ergonomically by query builders.
- **ROW_NUMBER PARTITION BY** — window functions for per-parent pagination in dataloader batch queries; syntax varies across query builders.
- **appendTimeRangeConditions()** — dynamically appending time-range predicates based on optional parameters requires conditional SQL construction that is natural with `strings.Builder` but awkward with query builder APIs.

The cost of raw SQL is that the compiler does not type-check query parameters against schema. This is mitigated by comprehensive integration tests against a real TimescaleDB instance.

---

Relevant Notes:
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — the specific construct enabled by raw SQL
- [[decomposed OR cursor condition enables TimescaleDB chunk pruning whereas ROW tuple comparison is opaque to the columnar scan engine]] — the cursor pattern that requires raw SQL control

Areas:
- [[entries/data-layer]]
