---
context: Gotcha: adding a new compound GraphQL field without updating getDBColumns silently omits backing columns, causing nil or zero values in query results
type: pattern
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql, data-layer]
---

Some GraphQL fields are backed by multiple DB columns. For example, the `limit` field on a trustline state change requires both `trustline_limit_old` and `trustline_limit_new` to be present to compute the delta. The `getDBColumns` switch-case in `utils.go` must explicitly map compound fields to all their backing columns. A new compound field that is added to the schema but not mapped will produce silently incomplete SQL — no error, just zero or nil values where the data should be. This is a maintenance hazard whenever the schema grows.
