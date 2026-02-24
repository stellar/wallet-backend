---
context: All five hypertables (transactions, operations, state_changes, transactions_accounts, operations_accounts) use ledger_created_at as the time dimension; since/until GraphQL parameters map directly to this column for chunk exclusion
type: decision
created: 2026-02-24
---

# ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern

Every event stored in the system has a `ledger_created_at` timestamp — the wall-clock time at which the containing ledger was finalized by the Stellar network. All five hypertables use this column as their TimescaleDB time dimension, which determines how chunks are partitioned on disk.

The choice is deliberate: the vast majority of API queries are time-bounded. Clients ask for account history "in the last 30 days", or "since timestamp X". When `since` and `until` parameters are provided, the planner generates time-range predicates on `ledger_created_at`, allowing TimescaleDB to skip entire chunks that fall outside the window.

Alternative partition columns considered:
- **`ledger_sequence`** — would work for range queries but forces a conversion step; timestamps are more natural for client-facing time filtering and chunk retention policies
- **`id` / `to_id`** — TOID encodes ledger position but is not a timestamp type; TimescaleDB requires a time-based column for its chunk management

The downstream consequence: all retention policies, compression schedules, and the chunk interval of 1 day are expressed in time units, which aligns naturally with operational expectations ("keep 90 days of data").

---

Relevant Notes:
- [[since and until parameters on Account queries enable TimescaleDB chunk exclusion to reduce scan cost for time-bounded account history]] — the GraphQL layer that exploits this partition design
- [[one-day chunk interval balances Stellar ledger volume against chunk management overhead]] — the specific chunk interval chosen for this time dimension

Areas:
- [[entries/data-layer]]
