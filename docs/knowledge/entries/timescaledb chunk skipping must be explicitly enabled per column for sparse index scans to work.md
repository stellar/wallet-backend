---
description: enable_chunk_skipping setting required per column; dbtest.Open enables at session level; dbtest.OpenWithoutMigrations enables at database level via ALTER DATABASE for migration testing
type: gotcha
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, timescaledb, chunk-skipping, testing, gotcha]
---

# TimescaleDB chunk skipping must be explicitly enabled per column for sparse index scans to work

## Context

TimescaleDB's chunk skipping uses min/max sparse indexes to skip entire chunks during queries. This is a non-default feature that must be explicitly enabled at both the column level (in migration SQL) and the session/database level.

## Why Chunk Skipping Matters for Wallet-Backend

The wallet-backend's primary query shape is an account's transaction or operation history over a time window: "give me account X's activity between ledger L1 and L2." This query touches a fraction of the full history — one account out of potentially millions, one time range out of the full retention period.

Without chunk skipping, a time-range query must scan every chunk that could contain the account, even chunks that are outside the requested time window. TimescaleDB partitions data by time (ledger sequence), so entire historical chunks that predate the query range can be physically discarded before any row scanning — but only when chunk skipping is enabled. The sparse min/max index per chunk records the range of account IDs (or operation IDs) contained in that chunk, letting the planner skip chunks that cannot contain the target account.

This is the query optimization that was cited as pillar 3 of the TimescaleDB architectural decision: "fast queries by discarding entire chunks." The configuration requirement documented here is what makes that architectural promise actually work in practice.

## Detail

Chunk skipping is configured at two levels:

**Column level** (in migration SQL):
```sql
SELECT add_column_to_orderby(
    'transactions', 'to_id', 'DESC',
    'enable_chunk_skipping', true
);
```
This creates the sparse index that records min/max `to_id` values per chunk.

**Session/database level** (must be set at runtime):
```sql
SET timescaledb.enable_chunk_skipping = on;    -- session level
ALTER DATABASE dbname SET timescaledb.enable_chunk_skipping = on;  -- database level
```

Without the session/database setting, TimescaleDB won't use the sparse indexes even though they exist on the columns.

`dbtest.Open(t)` sets it at session level for the test connection. `dbtest.OpenWithoutMigrations(t)` sets it at the database level (via `ALTER DATABASE`) because the migration runner opens its own connection that wouldn't inherit a session setting.

## Implications

- Production deployments must ensure `timescaledb.enable_chunk_skipping = on` is set in PostgreSQL configuration or at the database level.
- Failing to set this makes the sparse indexes on `to_id`, `operation_id` etc. decorative — they exist but are never used.
- Tests that don't use `dbtest.Open` may see different query plans than production.

## Source

`internal/db/dbtest/dbtest.go:Open()` and `OpenWithoutMigrations()`
`internal/db/migrations/2025-06-10.2-transactions.sql` — chunk skipping column setup

## Related

Since [[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]], the `dbtest.Open` helper that enables chunk skipping at the session level is why concrete model tests see the same query plans as production — without this, tests would silently miss the sparse index behavior.

The chunk-skipping configuration requirement is the operational consequence of a design choice made at the architecture level: [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — pillar 3 of that decision was fast queries via chunk pruning; this entry documents the non-obvious configuration that activates that capability.

Chunk skipping enables the sparse indexes — but two query-level techniques depend on those indexes being active to deliver their full benefit. Since chunk skipping must be on for sparse indexes to work, [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] only achieves its chunk-skipping payoff when this configuration is in place: the decomposed OR form gives the engine a pushable predicate, but the engine must have sparse indexes to consult. Similarly, [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] prevents the planner from collapsing the CTE and losing ChunkAppend — but ChunkAppend only skips chunks because the sparse indexes this entry activates are present. All three are required end to end.

relevant_notes:
  - "[[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — grounds this: dbtest.Open enables chunk skipping for concrete model tests, making test query plans match production"
  - "[[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — provides architectural origin: chunk-based fast queries (pillar 3) is what motivated this entry; chunk skipping configuration is the mechanism that delivers that promise"
  - "[[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] — enables this downstream: decomposed cursor gives the engine a pushable predicate, but its chunk-pruning benefit depends on sparse indexes that this entry activates"
  - "[[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] — enables this downstream: MATERIALIZED CTE preserves ChunkAppend at the join layer, but ChunkAppend only skips chunks because sparse indexes from this entry are active"
