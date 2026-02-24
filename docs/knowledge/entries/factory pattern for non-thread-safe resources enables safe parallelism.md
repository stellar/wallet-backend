---
context: func(ctx) → Resource passed at wiring time; each goroutine in pool calls factory to get isolated instance; hides concrete type from parallel code
type: pattern
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# factory pattern for non-thread-safe resources enables safe parallelism

When parallel workers need a resource that is not thread-safe, the factory pattern provides a clean solution: instead of sharing one instance, pass a `func(ctx) → Resource` that each worker calls to get its own isolated instance.

In the ingestion pipeline, `LedgerBackendFactory` is a `func(ctx) → LedgerBackend`. Each goroutine in the backfill `pond.Pool` calls the factory to create a fresh `LedgerBackend`, uses it for its batch, and closes it when done. The factory call internally creates a new backend and calls `PrepareRange(bounded)` for the specific batch range.

Benefits of this approach:

1. **Encapsulation of initialization:** The factory hides the details of how to construct a valid, initialized instance. Callers don't need to know constructor parameters.
2. **Concrete type transparency:** The parallel code works against the `LedgerBackend` interface — it doesn't know (or care) whether it's getting an RPC backend or a Datastore backend.
3. **Lifecycle management:** Each goroutine owns its instance and is responsible for closing it. This prevents resource leaks from shared instances.
4. **Testability:** Injecting a mock factory is easy — pass a factory that returns a test double.

The pattern applies beyond `LedgerBackend`. Any non-thread-safe resource needed by a parallel worker pool (database connections outside the pool, streaming API clients, file handles) can be managed this way.

The alternative — creating instances inside the goroutine — couples the goroutine to the concrete type. The alternative — using a pool of pre-created instances — requires explicit pool management. The factory pattern is the simplest approach when instance creation is cheap.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[backfill batch processing uses one LedgerBackend per goroutine because LedgerBackend is not thread-safe]] — the concrete application of this pattern
- [[LedgerBackend interface abstracts between real-time RPC and cloud storage datastore backends]] — the interface the factory produces
- [[the resolver struct is constructed once at startup and shared across all requests requiring all injected dependencies to be thread-safe]] — the GraphQL complement: instead of isolating per-worker, requires all shared dependencies to be thread-safe by design

Areas:
- [[entries/ingestion]]
