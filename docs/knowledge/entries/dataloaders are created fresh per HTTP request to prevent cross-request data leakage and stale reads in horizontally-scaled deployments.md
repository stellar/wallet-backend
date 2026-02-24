---
context: Per-request creation adds minor allocation overhead but is required for correctness; global dataloader instances would cache one request's data into another's response
type: pattern
created: 2026-02-24
---

Each incoming HTTP request triggers construction of a new set of dataloaders via the dataloader middleware. This pattern is required because dataloaders cache results for the duration of their batch window — a shared global instance would serve cached data from one request to another. In a horizontally-scaled deployment this is especially dangerous since requests may be for different users or different time contexts. The per-request lifecycle is referenced in the dataloader library's own documentation (issue #62) as the canonical approach. The 5ms batching window and 100-key batch capacity are fixed defaults — see the open question about whether these are optimally tuned.
