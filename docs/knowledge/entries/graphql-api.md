---
description: Knowledge map for the GraphQL API subsystem
type: reference
subsystem: graphql
areas: [knowledge-map, graphql, resolvers, dataloaders, gqlgen]
vault: docs/knowledge
---

# GraphQL API Knowledge Map

The GraphQL API is built with gqlgen (schema-first). It exposes ledger data (transactions, operations, state changes) and transaction mutation endpoints. Dataloaders prevent N+1 queries for relationship fields.

**Key code:** `internal/serve/graphql/`, `internal/serve/serve.go`

## Reference Doc

[[references/graphql-api]] — request flow, middleware stack, resolver patterns, dataloader architecture, complexity scoring

## Decisions

<!-- - [[entries/why-schema-first-graphql]] -->
<!-- - [[entries/complexity-limit-design]] -->

## Insights

<!-- - [[entries/dataloader-batching-window]] -->

## Patterns

<!-- - [[entries/dataloader-pattern]] -->
<!-- - [[entries/resolver-service-split]] -->

## Gotchas

<!-- - [[entries/gql-generate-required-after-schema-change]] -->

## Topics

[[entries/index]] | [[references/graphql-api]]
