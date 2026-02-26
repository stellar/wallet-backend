---
context: Both values are hardcoded defaults; no evidence of measurement against production query mix; wrong values cause either too many DB round-trips or unnecessary latency
type: question
status: open
created: 2026-02-24
subsystem: graphql
areas: [graphql]
---

The dataloader middleware configures a 5ms batching window and a 100-key maximum batch size. These are the library's default values and have not been tuned against production traffic. For a workload with many small concurrent requests, 5ms may be too long (adding latency unnecessarily). For a workload with large fan-out queries (e.g., fetching operations for 500 transactions in one request), 100-key batches may cause multiple round-trips that could have been avoided with a larger batch cap. Tuning would require profiling production query patterns, which has not been done.

Since [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]], the batching window operates within a single request's lifetime — not across requests. This scoping limits the blast radius of a mis-tuned window but also means the batcher cannot amortize costs across concurrent requests from different users.

---

Relevant Notes:
- [[dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]] — the per-request scoping that bounds both the batching opportunity and the correctness guarantee; tuning these values operates within that constraint

Areas:
- [[entries/graphql-api]]
