---
context: Implemented in internal/graphql/utils.go; avoids fetching all columns when the client only requested a subset of fields
type: pattern
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql, data-layer]
---

`GetDBColumnsForFields` inspects the `graphql.ResolveParams` field selection set at runtime to determine which GraphQL fields the client actually requested, then maps those to the corresponding DB columns. The resulting column list is passed to the SQL query builder, producing a `SELECT col1, col2, ...` instead of `SELECT *`. This is a meaningful optimization for wide rows (e.g., operations with many optional fields) and becomes especially valuable for paginated list queries. The mapping requires explicit maintenance — see [[GraphQL fields that map to multiple DB columns require explicit switch cases in getDBColumns to project all required backing columns]].
