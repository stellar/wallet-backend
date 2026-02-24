---
context: Time-bounding at the GraphQL layer maps directly to WHERE clauses that allow the TimescaleDB planner to skip non-overlapping chunks entirely
type: insight
created: 2026-02-24
---

# since and until parameters on Account queries enable TimescaleDB chunk exclusion to reduce scan cost for time-bounded account history

The `since` and `until` parameters on `transactionsByAccount`, `operationsByAccount`, and related queries are not just convenience filters — they are designed to align with TimescaleDB's chunk exclusion mechanism. When these parameters are present, the generated SQL includes time-range predicates on the hypertable's time dimension, allowing the planner to skip chunks whose time range falls entirely outside the query window. Without these predicates, the query degrades to a full hypertable scan. Since [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]], this design makes the API's optional filtering directly valuable for chunk pruning efficiency.

At the data layer, the function `appendTimeRangeConditions()` is responsible for translating `since`/`until` into SQL predicates. It is called during query construction in the account-history query builders, appending `AND ledger_created_at >= $N` and/or `AND ledger_created_at <= $N` clauses to the `strings.Builder` query. The function is a no-op when neither parameter is set — degrading silently to a full hypertable scan rather than erroring. This is consistent with the API design (time-bounding is optional) but means operators must be aware that unbounded queries carry full-scan cost.

---

Relevant Notes:
- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the partition design this query parameter exploits
- [[raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate]] — `appendTimeRangeConditions()` uses strings.Builder; this explains why raw SQL is necessary

Areas:
- [[entries/graphql-api]] | [[entries/data-layer]]
