---
context: Dataloader batch queries use ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY ...) then filter WHERE rn <= limit; a global LIMIT would return all rows from the first few parents, leaving later parents with zero results
type: insight
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, graphql]
---

# ROW_NUMBER PARTITION BY parent_id guarantees each parent receives at most limit rows in dataloader batch queries preventing head-of-line starvation

DataLoader batch queries fetch related rows for multiple parent IDs in a single SQL query. For example, fetching the first N operations for each of K transactions in one round trip. The naive approach — `WHERE transaction_id IN ($1, ..., $K) LIMIT N` — has a critical flaw: `LIMIT N` applies globally, not per parent. If the first parent has thousands of operations, it consumes all N slots, and subsequent parents receive zero rows.

The correct approach uses `ROW_NUMBER()`:
```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY to_id ORDER BY id) AS rn
    FROM operations
    WHERE to_id IN ($1, ..., $K)
) sub
WHERE rn <= $limit
```

`ROW_NUMBER() OVER (PARTITION BY to_id)` assigns sequential numbers starting from 1 for each distinct `to_id` value. Filtering `WHERE rn <= $limit` keeps at most `$limit` rows per `to_id`. Every parent in the batch is guaranteed its own "budget" of up to `$limit` rows — no parent can starve another.

The ORDER BY within the window function (`ORDER BY id` or `ORDER BY ledger_created_at, id`) determines which rows are returned when the count exceeds the limit — this must match the pagination order used by the API to ensure consistent cursor-based pagination.

The performance implication: the window function adds an additional sort step. For most dataloader queries, this is outweighed by the benefit of collapsing K separate queries into one round trip.

---

Relevant Notes:
- [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]] — the dataloader lifecycle that uses these batch queries
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — another query optimization pattern in the data layer

Areas:
- [[entries/data-layer]]
