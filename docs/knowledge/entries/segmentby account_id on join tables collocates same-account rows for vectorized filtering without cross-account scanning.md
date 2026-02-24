---
context: transactions_accounts and operations_accounts use segmentby=account_id; the main hypertables (transactions, operations) do not use segmentby because they are queried by multiple dimensions
type: decision
created: 2026-02-24
---

# segmentby account_id on join tables collocates same-account rows for vectorized filtering without cross-account scanning

TimescaleDB's `segmentby` parameter on compressed chunks determines how rows are physically co-located within a compressed segment. When a column is used as a `segmentby` key, all rows sharing the same value for that column are stored together in the same compressed segment.

The join tables `transactions_accounts` and `operations_accounts` use `segmentby = account_id`. This means that when a query scans for a specific account's transactions, all matching rows for that account are in the same compressed segment — the decompression and filtering work is concentrated, with no wasted I/O from mixing rows belonging to different accounts.

The main hypertables (`transactions`, `operations`, `state_changes`) do NOT use `segmentby` because:
- They are queried by multiple dimensions (account, asset, time) with no dominant access pattern on a single dimension
- Adding `segmentby` on any one dimension would penalize queries that don't filter on that dimension

The practical effect: account-history queries that JOIN against `transactions_accounts` or `operations_accounts` benefit from vectorized filtering within segments, reducing the number of compressed segments the planner must decompress and scan.

---

Relevant Notes:
- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the partition key that determines chunk layout; segmentby operates within chunks
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — how the join table is queried in practice

Areas:
- [[entries/data-layer]]
