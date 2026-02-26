---
description: MATERIALIZED forces ChunkAppend on transactions_accounts; LATERAL gives O(1) join per row to transactions; without MATERIALIZED planner might merge and lose chunk pruning
type: gotcha
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, timescaledb, postgresql, query-optimization, cte, lateral-join]
---

# Materialized CTE plus LATERAL join forces separate planning to preserve chunk pruning on account queries

## Context

Account-scoped transaction queries must first find transactions that include a given account (`transactions_accounts`), then fetch the full transaction data (`transactions`). The join strategy significantly impacts performance with TimescaleDB chunk pruning.

## Detail

`TransactionModel.BatchGetByAccountAddress` uses:
```sql
WITH account_txns AS MATERIALIZED (
    SELECT tx_to_id, ledger_created_at
    FROM transactions_accounts
    WHERE account_id = $1
    ORDER BY ledger_created_at DESC, tx_to_id DESC
    LIMIT $N
)
SELECT {columns}
FROM account_txns ta,
LATERAL (SELECT * FROM transactions t
         WHERE t.to_id = ta.tx_to_id
           AND t.ledger_created_at = ta.ledger_created_at
         LIMIT 1) t
```

`MATERIALIZED` forces the planner to execute the CTE independently — `ChunkAppend` on `transactions_accounts` runs with `ledger_created_at` as the leading ORDER BY column. Without `MATERIALIZED`, the planner might fold the CTE into the outer query, losing the chunk-pruning opportunity on `transactions_accounts`.

`LATERAL` makes the join to `transactions` per-row in `account_txns`. Since each join uses the exact `(to_id, ledger_created_at)` primary key, it's O(1) per row — effectively a point lookup for each account transaction.

## Implications

- Never remove `MATERIALIZED` from these queries without benchmarking. PostgreSQL 14+ defaults to non-materialized CTEs; explicit `MATERIALIZED` overrides this optimization and is load-bearing here.
- The same pattern applies to `OperationModel.BatchGetByAccountAddress` with its join tables.

## Source

`internal/data/transactions.go:BatchGetByAccountAddress()`
`internal/data/operations.go:BatchGetByAccountAddress()`

## Related

The MATERIALIZED CTE preserves chunk pruning through the CTE boundary, and [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] is the complementary technique within the cursor WHERE clause — together they ensure the planner can prune chunks at both the join and pagination layers.

relevant_notes:
  - "[[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] — synthesizes with this: both are chunk-pruning techniques for TimescaleDB queries; MATERIALIZED handles the join layer, decomposed cursor handles the pagination layer"
