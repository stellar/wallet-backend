---
description: ROW(a,b) < ROW($1,$2) cannot be pushed into vectorized chunk filters; decomposed OR clause (col1 < $1 OR (col1 = $1 AND col2 < $2)) allows the engine to prune by ledger_created_at independently
type: gotcha
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, timescaledb, postgresql, query-optimization, pagination, chunk-pruning]
---

# The decomposed cursor condition enables TimescaleDB chunk pruning where row tuple comparison cannot

## Context

Cursor-based pagination requires comparing (current_position) against (cursor_position) across two columns: `ledger_created_at` and `to_id`/`id`. The natural SQL for this is a row tuple comparison `ROW(ledger_created_at, to_id) < ROW($1, $2)`, but TimescaleDB's ColumnarScan cannot vectorize this form.

## Detail

TimescaleDB's ColumnarScan (vectorized chunk filtering) works on simple column comparisons. Row tuple comparisons are opaque to the engine — it cannot determine from `ROW(ledger_created_at, to_id) < ROW($1, $2)` alone that `ledger_created_at < $1` implies the entire chunk can be skipped.

`buildDecomposedCursorCondition()` in `internal/data/query_utils.go` generates:
```sql
WHERE (ledger_created_at < $1
  OR (ledger_created_at = $1 AND to_id < $2))
```

This decomposed form is logically equivalent to the row tuple comparison, but the engine can now push `ledger_created_at < $1` into the chunk filter, skipping entire days of data before the cursor.

## Implications

- Never change pagination queries to use `ROW()` comparisons — this will silently degrade query performance on large datasets.
- The decomposed form is used for all 3 cursor types: 2-column composite, 4-column state change, and 3-column within-parent.
- Benchmark any changes to cursor SQL against real TimescaleDB data with `EXPLAIN (ANALYZE, BUFFERS)` to confirm `ChunkAppend` with selective chunk scanning.

## Source

`internal/data/query_utils.go:buildDecomposedCursorCondition()`
`internal/data/transactions.go`, `operations.go`, `statechanges.go` — usage sites

## Related

The decomposed cursor condition works alongside [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] — that technique preserves chunk pruning across the CTE/join boundary; this one preserves it within the cursor WHERE clause. Both are required for full TimescaleDB query optimization on account-scoped queries.

relevant_notes:
  - "[[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] — synthesizes with this: MATERIALIZED CTE handles chunk pruning at the join layer; decomposed cursor handles it at the pagination layer — both are needed for end-to-end pruning"
