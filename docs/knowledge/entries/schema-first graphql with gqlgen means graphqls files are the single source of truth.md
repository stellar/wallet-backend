---
description: .graphqls files in schema/ drive make gql-generate; generated/ code must never be edited manually; resolver file layout mirrors schema file layout
type: decision
status: current
confidence: proven
subsystem: graphql
areas: [graphql, gqlgen, schema, code-generation]
---

# Schema-first GraphQL with gqlgen means .graphqls files are the single source of truth

## Context

GraphQL APIs can be built code-first (types defined in Go, schema derived) or schema-first (schema defined in SDL, code generated from it). This codebase uses schema-first with `gqlgen`.

## Detail

All `.graphqls` files in `internal/serve/graphql/schema/` define the complete API contract. Running `make gql-generate` regenerates `internal/serve/graphql/generated/` from those schemas. The generated code is checked into the repository but must never be edited directly — any manual change will be overwritten on the next `make gql-generate`.

Resolver files follow a one-to-one mapping with schema files:
- `queries.graphqls` → `queries.resolvers.go`
- `transaction.graphqls` → `transaction.resolvers.go`
- etc.

Hand-written resolver files (`resolver.go`, `utils.go`, `errors.go`, `balance_reader.go`) exist alongside the generated files for shared logic that gqlgen doesn't produce.

## Implications

- After editing any `.graphqls` file, always run `make gql-generate` before building or testing.
- New entity types added to the schema automatically produce resolver stubs that must be implemented.
- The `@goField(forceResolver: true)` directive forces gqlgen to generate a resolver method for fields that would otherwise be read directly from Go struct fields — required for lazy dataloader loading.

## Source

`internal/serve/graphql/schema/` — all schema source files
`internal/serve/graphql/generated/` — generated code (DO NOT EDIT)
`Makefile:gql-generate` — generation command

## Related

The schema drives generation of resolver stubs that call into [[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — `@goField(forceResolver: true)` on relationship fields is what causes gqlgen to generate methods that the DataLoader middleware populates.

relevant_notes:
  - "[[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — grounds this: forceResolver directives in .graphqls files are what generate the resolver methods that trigger DataLoader batch queries; schema-first is the prerequisite for that integration"
