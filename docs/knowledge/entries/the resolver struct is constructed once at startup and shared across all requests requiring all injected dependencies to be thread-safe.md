---
context: Single resolver instance serves concurrent HTTP requests; any stateful dependency must use sync primitives or be per-request scoped
type: fact
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql]
---

The `Resolver` struct in `internal/graphql/resolvers/root.go` is instantiated once during server startup and registered with the gqlgen handler. All concurrent HTTP requests share this single instance. Any service, model, or client injected into the resolver must therefore be safe for concurrent access. In practice this is satisfied by using DB connection pools (which are thread-safe) and stateless service objects. Dataloaders are the notable exception — they are per-request scoped via middleware (see [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]]) and are not held on the Resolver struct itself.

This is the GraphQL mirror of the [[factory pattern for non-thread-safe resources enables safe parallelism]] used in the ingestion pipeline: rather than giving each concurrent worker its own resource instance, the resolver design demands that shared dependencies be inherently safe — or delegates non-safe state to per-request scope. The consequence is that any new dependency injected into the Resolver must be audited for thread safety before adding it to the struct.

---

Relevant Notes:
- [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]] — the per-request scoping that handles the thread-unsafe dataloader case
- [[factory pattern for non-thread-safe resources enables safe parallelism]] — the ingestion-side complement: when sharing is impossible, each worker gets its own instance via factory; the resolver pattern inverts this by requiring shared dependencies to be safe

Areas:
- [[entries/graphql-api]]
