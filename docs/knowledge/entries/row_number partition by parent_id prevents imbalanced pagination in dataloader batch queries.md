---
description: Global LIMIT returns all rows from first parents and none from later ones; ROW_NUMBER() OVER (PARTITION BY parent_id) guarantees each parent gets at most N rows in one batch query
type: pattern
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, postgresql, dataloaders, pagination, graphql]
---

# ROW_NUMBER PARTITION BY parent_id prevents imbalanced pagination in dataloader batch queries

## Context

DataLoaders batch N parent IDs into a single DB query to fetch child rows. Using a simple `LIMIT` on the batch query returns too many rows from the first parents and none from later parents — an unfair distribution.

## Detail

`StateChangeModel.BatchGetByToIDs` and `BatchGetByOperationIDs` use:
```sql
WITH
    inputs (to_id) AS (SELECT * FROM UNNEST($1::bigint[])),
    ranked AS (
        SELECT sc.*, ROW_NUMBER() OVER (PARTITION BY sc.to_id ORDER BY ...) AS rn
        FROM state_changes sc
        JOIN inputs i ON sc.to_id = i.to_id
    )
SELECT ... FROM ranked WHERE rn <= {limit}
```

`ROW_NUMBER() OVER (PARTITION BY sc.to_id ...)` assigns a per-parent rank to each row. The outer `WHERE rn <= {limit}` clips each parent to at most `limit` rows, regardless of how many other parents are in the batch.

Without this pattern, a `LIMIT 100` with 20 parent IDs might return 100 rows from the first parent and 0 from the rest.

## Implications

- This pattern adds a CTE layer to every batch query — a worthwhile cost for correctness in DataLoader scenarios.
- The same pattern applies to `OperationModel.BatchGetByToIDs` for operations within transactions.
- The `PARTITION BY` column must match the DataLoader's key field exactly (e.g., `to_id` for the TransactionID loader).

## Source

`internal/data/statechanges.go:BatchGetByToIDs()`, `BatchGetByOperationIDs()`

## Related

These batch queries are issued by the loaders created in [[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — the per-request creation scope is what makes ROW_NUMBER partitioning semantics clean: each batch contains only keys from a single request's 5ms window.

relevant_notes:
  - "[[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — grounds this: per-request DataLoaders are what execute these partitioned batch queries; the request scope makes PARTITION BY semantics well-defined"
