---
context: WITH accts AS MATERIALIZED (SELECT to_id FROM transactions_accounts WHERE account_id = $1) prevents planner from merging CTE into the outer query; ChunkAppend then applies to the transactions hypertable scan
type: insight
created: 2026-02-24
---

# MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable

Account-history queries must join the `transactions_accounts` (or `operations_accounts`) join table against the `transactions` hypertable. Without care, the planner may choose to merge these into a single plan that defeats TimescaleDB's ChunkAppend optimization.

The query pattern:
```sql
WITH accts AS MATERIALIZED (
    SELECT to_id FROM transactions_accounts
    WHERE account_id = $1
)
SELECT t.* FROM transactions t
JOIN accts ON t.to_id = accts.to_id
WHERE t.ledger_created_at > $2
```

The `MATERIALIZED` keyword is the key. Without it, PostgreSQL's CTE inlining optimization (enabled by default since PostgreSQL 12) may merge the CTE into the outer query, producing a single plan that the planner treats as a flat join. In this merged plan, the TimescaleDB planner cannot cleanly apply ChunkAppend — the chunk-pruning optimization that scans only chunks within the time range.

With `MATERIALIZED`, the planner is forced to execute the CTE as a separate materialized subquery first, producing a temporary result set. The outer query then joins this result against `transactions` — and because the outer query is now a clean hypertable scan with time predicates, ChunkAppend applies correctly.

The trade-off: materializating the CTE adds a temporary result set. For accounts with very high transaction volume, this may be non-trivial. But for the dominant case (accounts with moderate transaction counts), the chunk-pruning benefit outweighs the materialization cost.

---

Relevant Notes:
- [[LATERAL join for parent-row lookup is O(1) per row via primary key compared to a regular join requiring planner strategy selection]] — the complementary join pattern used alongside MATERIALIZED CTEs
- [[segmentby account_id on join tables collocates same-account rows for vectorized filtering without cross-account scanning]] — the physical layout that makes the CTE subquery efficient
- [[decomposed OR cursor condition enables TimescaleDB chunk pruning whereas ROW tuple comparison is opaque to the columnar scan engine]] — another chunk-pruning technique applied in the outer query

Areas:
- [[entries/data-layer]]
