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

relevant_notes:
  - "[[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — grounds this: dbtest.Open enables chunk skipping for concrete model tests, making test query plans match production"
