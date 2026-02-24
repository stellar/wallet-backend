---
context: BatchGetByOperationIDs uses LATERAL (SELECT * FROM transactions WHERE to_id = op.id & ~x'FFF'::bigint LIMIT 1) instead of JOIN; this forces primary key lookup per operation row
type: insight
created: 2026-02-24
---

# LATERAL join for parent-row lookup is O(1) per row via primary key compared to a regular join requiring planner strategy selection

When fetching operations and their parent transactions in a single query, a natural approach is to JOIN transactions against operations on the TOID relationship. However, this leaves the planner free to choose any join strategy — hash join, merge join, nested loop — which may not be optimal when the parent-row lookup is guaranteed to be a primary-key point lookup.

The data layer uses LATERAL joins instead:
```sql
SELECT op.*, tx.*
FROM operations op
CROSS JOIN LATERAL (
    SELECT * FROM transactions
    WHERE to_id = op.id & (~x'FFF'::bigint)
    LIMIT 1
) tx
```

`LATERAL` allows the subquery to reference columns from the outer query (`op.id`). For each operation row, the planner executes the subquery independently — and since `to_id` is the primary key of `transactions`, this is a guaranteed primary key lookup: O(1), no sorting, no hash build, no merge step.

The `LIMIT 1` is a defensive clause (there is exactly one parent transaction per TOID prefix); it also signals to the planner that a single-row result is expected, preventing full scans.

The alternative (regular JOIN) would work correctly but surrenders the O(1) guarantee to the planner's statistics-driven strategy selection. For large operations batches, the planner might choose a hash join that builds an in-memory hash table of transactions — unnecessary given the primary-key relationship.

---

Relevant Notes:
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — the CTE pattern used alongside LATERAL joins
- [[TOID bit masking recovers parent transaction TOID from operation ID by clearing the lower 12 bits that encode op_index]] — the bit operation used in the LATERAL join predicate

Areas:
- [[entries/data-layer]]
