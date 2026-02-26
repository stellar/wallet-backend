---
context: Omitting this step after schema edits leaves generated resolver signatures stale, causing compile errors or silent behavioral drift
type: fact
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql]
---

The gqlgen code-generator reads all `.graphqls` files and emits Go resolver interfaces, model types, and wiring boilerplate. Because the generated files are committed to the repo, any schema change that is not followed by `make gql-generate` results in the generated code diverging from the schema — this surfaces as compile errors at best, or silently missing fields at worst. The resolver file-per-schema-file pattern (`resolvers/transactions.resolvers.go` etc.) means each schema file owns its resolver file; gqlgen regenerates only the affected resolver file on each run.
