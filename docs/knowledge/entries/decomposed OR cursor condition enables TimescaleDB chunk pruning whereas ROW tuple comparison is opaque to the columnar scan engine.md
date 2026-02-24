---
context: Cursor WHERE clauses use (a > $1) OR (a = $1 AND b > $2) form; ROW(a,b) > ROW($1,$2) tuple comparison is semantically equivalent but TimescaleDB cannot push it into vectorized chunk filtering
type: insight
created: 2026-02-24
---

# decomposed OR cursor condition enables TimescaleDB chunk pruning whereas ROW tuple comparison is opaque to the columnar scan engine

Relay-style cursor pagination requires expressing "rows after cursor position (a, b)". The mathematically correct condition is: `(a > $1) OR (a = $1 AND b > $2)`. PostgreSQL also supports the more concise tuple comparison syntax: `ROW(a, b) > ROW($1, $2)` — semantically identical, shorter to write.

However, the two forms have different performance characteristics in TimescaleDB. TimescaleDB's columnar scan engine can push simple column predicates (e.g., `a > $1`) into chunk-level filtering — this enables chunk pruning where chunks whose entire range of `a` falls below `$1` are skipped without scanning. This is the same mechanism that makes `since`/`until` time-range filtering efficient.

`ROW(a, b) > ROW($1, $2)` is treated as an opaque expression by the TimescaleDB planner. The tuple comparison cannot be decomposed into individual column predicates at the chunk-filter level. The result: every chunk must be scanned regardless of whether it could have been pruned.

For cursor-paginated queries where `a` is the time dimension (`ledger_created_at`), the cursor value is effectively a time lower bound — meaning significant chunks of historical data can be skipped via the `a > $1` predicate in the decomposed form. The tuple form eliminates this optimization entirely.

The implementation uses `strings.Builder` to compose the decomposed form, with cursor column combinations varying by cursor type: CursorTypeComposite uses `(ledger_created_at, to_id)` or `(ledger_created_at, id)`; CursorTypeStateChange adds `sc_order` and `op_id`.

---

Relevant Notes:
- [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — the cursor types whose SQL conditions this affects
- [[raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate]] — why raw SQL is required to express this pattern
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — another TimescaleDB-specific query optimization

Areas:
- [[entries/data-layer]]
