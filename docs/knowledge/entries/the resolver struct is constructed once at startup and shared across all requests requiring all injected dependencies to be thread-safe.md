---
context: Single resolver instance serves concurrent HTTP requests; any stateful dependency must use sync primitives or be per-request scoped
type: fact
created: 2026-02-24
---

The `Resolver` struct in `internal/graphql/resolvers/root.go` is instantiated once during server startup and registered with the gqlgen handler. All concurrent HTTP requests share this single instance. Any service, model, or client injected into the resolver must therefore be safe for concurrent access. In practice this is satisfied by using DB connection pools (which are thread-safe) and stateless service objects. Dataloaders are the notable exception — they are per-request scoped via middleware (see [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]]) and are not held on the Resolver struct itself.
