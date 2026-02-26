---
description: Fresh Dataloaders instance per HTTP request via DataloaderMiddleware; context-injected; prevents stale reads in multi-instance deployments; sacrifices cross-request cache efficiency
type: decision
status: current
confidence: proven
subsystem: graphql
areas: [graphql, dataloaders, middleware, caching, concurrency]
---

# Dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments

## Context

A shared DataLoader across requests would cache DB results from one request and serve them to another request, potentially returning stale data. This is especially problematic when multiple serve instances run against the same database.

## Detail

`DataloaderMiddleware` calls `dataloaders.NewDataloaders(models)` at the start of each HTTP request, creating 8 fresh loader instances. The loaders are stored in the request context via `context.WithValue`. Resolvers retrieve them with:

```go
loaders := ctx.Value(middleware.LoadersKey).(*dataloaders.Dataloaders)
```

Each loader uses `dataloadgen` with `BatchCapacity(100)` and `Wait(5ms)`. The 5ms window collects keys from all concurrent sub-resolvers within a single request before firing the batch query.

A shared loader would serve cached results from Request A to Request B if the same keys are queried. In a single-instance deployment this might be acceptable, but in a load-balanced multi-instance deployment it's a correctness issue (see dataloader issue #62).

## Implications

- All 8 loaders are created per request even if the request only uses 1 or 2. The construction cost is minimal (no DB calls on creation).
- Cache hits only occur within a single request's 5ms batch window — this is the intended scope.
- If response latency becomes critical, per-request loader pooling could be explored, but the per-request model is the safe default.

## Source

`internal/serve/middleware/dataloader_middleware.go:DataloaderMiddleware()`
`internal/serve/graphql/dataloaders/dataloaders.go:NewDataloaders()`

## Related

The DataLoaders created per-request use [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] in their batch queries — the per-request model is the right scope for these batch queries because each request's 5ms window collects only that request's sub-resolver keys.

relevant_notes:
  - "[[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — grounds this: the batch queries that run within each per-request DataLoader instance use ROW_NUMBER partitioning; per-request creation bounds the scope of those batches correctly"
