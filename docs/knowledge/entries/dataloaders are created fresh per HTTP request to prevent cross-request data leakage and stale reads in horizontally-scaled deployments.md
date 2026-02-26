---
context: Per-request creation adds minor allocation overhead but is required for correctness; global dataloader instances would cache one request's data into another's response
type: pattern
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql]
---

Each incoming HTTP request triggers construction of a new set of dataloaders via the dataloader middleware. This pattern is required because dataloaders cache results for the duration of their batch window — a shared global instance would serve cached data from one request to another. In a horizontally-scaled deployment this is especially dangerous since requests may be for different users or different time contexts. The per-request lifecycle is referenced in the dataloader library's own documentation (issue #62) as the canonical approach. The 5ms batching window and 100-key batch capacity are fixed defaults — see [[whether the 5ms dataloader batching window and 100-key batch capacity are optimally tuned for production traffic patterns]] for the open question about correctness of these values.

Since [[the resolver struct is constructed once at startup and shared across all requests requiring all injected dependencies to be thread-safe]], dataloaders cannot live on the Resolver struct itself — they are per-request scoped via middleware precisely because they carry mutable cache state.

---

Relevant Notes:
- [[the resolver struct is constructed once at startup and shared across all requests requiring all injected dependencies to be thread-safe]] — explains why dataloaders must be per-request: the shared resolver cannot hold stateful cache
- [[whether the 5ms dataloader batching window and 100-key batch capacity are optimally tuned for production traffic patterns]] — the open question about tuning the window and batch cap that operate within each per-request instance

Areas:
- [[entries/graphql-api]]
