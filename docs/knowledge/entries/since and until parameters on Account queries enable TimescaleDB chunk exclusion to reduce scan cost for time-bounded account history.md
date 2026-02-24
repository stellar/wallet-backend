---
context: Time-bounding at the GraphQL layer maps directly to WHERE clauses that allow the TimescaleDB planner to skip non-overlapping chunks entirely
type: insight
created: 2026-02-24
---

The `since` and `until` parameters on `transactionsByAccount`, `operationsByAccount`, and related queries are not just convenience filters — they are designed to align with TimescaleDB's chunk exclusion mechanism. When these parameters are present, the generated SQL includes time-range predicates on the hypertable's time dimension, allowing the planner to skip chunks whose time range falls entirely outside the query window. Without these predicates, the query degrades to a full hypertable scan. This is the cross-subsystem linkage between GraphQL schema design and the TimescaleDB-specific data layer optimization described in [[data-layer]].

At the data layer, the function `appendTimeRangeConditions()` is responsible for translating `since`/`until` into SQL predicates. It is called during query construction in the account-history query builders, appending `AND ledger_created_at >= $N` and/or `AND ledger_created_at <= $N` clauses to the `strings.Builder` query. The function is a no-op when neither parameter is set — degrading silently to a full hypertable scan rather than erroring. This is consistent with the API design (time-bounding is optional) but means operators must be aware that unbounded queries carry full-scan cost.
