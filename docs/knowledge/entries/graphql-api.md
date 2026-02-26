---
description: Knowledge map for the GraphQL API — request flow, schema-first design, resolver patterns, dataloaders, and mutations
type: reference
status: current
subsystem: graphql
areas: [navigation, graphql]
---

# GraphQL API — Knowledge Map

The GraphQL API is built with `gqlgen` using a schema-first approach. The middleware stack handles auth, metrics, dataloaders, and complexity limits. Three resolution patterns cover root queries, relationship fields, and mutations.

## Schema Design

- [[schema-first graphql with gqlgen means graphqls files are the single source of truth]] — The .graphqls-first workflow; make gql-generate; never edit generated/ directly

## DataLoaders

- [[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — Fresh loaders per request; 5ms batching window; 100-key batch capacity
- [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — The SQL pattern that makes DataLoader batch queries fair across all parent IDs
- [[toid bit-masking recovers parent transaction id from operation id without a join]] — SEP-35 bitmask used in operationsByToIDLoader grouping

## Mutations

- [[buildTransaction mutation returns signed xdr to the client rather than submitting it]] — The server builds+signs but never submits; client-controlled submission model

## Tensions

- Per-request DataLoader creation vs shared cache: cross-request safety vs cache efficiency. The per-request model is the safe default for multi-instance deployments.

## Open Questions

- Could the query complexity scorer emit per-field cost breakdown to help debug expensive queries? (ComplexityLogger currently records only total complexity)

---

Agent Notes:
- 2026-02-26: TOID bitmask appears in two DataLoader contexts — Go-side grouping in operationsByToIDLoader and SQL-side join in BatchGetByOperationIDs. [[toid bit-masking recovers parent transaction id from operation id without a join]] and [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] often appear in the same query.
- 2026-02-26: [[schema-first graphql with gqlgen means graphqls files are the single source of truth]] is the prerequisite for understanding why the DataLoader integration uses @goField directives — start there when onboarding to the GraphQL layer.
