---
context: NativeBalance, TrustlineBalance, ContractBalance tables are regular PG tables; five event tables (transactions, operations, state_changes, *_accounts) are hypertables
type: decision
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, timescaledb]
---

# balance tables use regular PostgreSQL tables not hypertables because they store current state not time-series events

TimescaleDB hypertables are designed for append-only time-series data where historical records are immutable once written. The five event tables (transactions, operations, state_changes, transactions_accounts, operations_accounts) fit this model exactly — each ledger event is written once and never updated.

Balance tables are fundamentally different: they represent the *current* state of an account's balances. Each time a ledger is processed, balance rows are upserted — the same account's row is updated with new values. There is no accumulation of historical rows; only the latest state matters.

Converting balance tables to hypertables would be incorrect:
- Hypertables partition by time, but balance rows don't have a meaningful time dimension they accumulate on
- TimescaleDB's compression is optimized for sequential, append-only writes; balance upserts would constantly write into the "current" chunk, defeating compression
- Retention policies would incorrectly delete current balance state if applied

Instead, balance tables are tuned for their actual access pattern: frequent single-row upserts with occasional bulk reads. The tuning applied — high fillfactor, aggressive autovacuum — is specific to update-heavy relational tables, not hypertable features.

---

Relevant Notes:
- [[fillfactor 80 on balance tables reserves page space for HOT updates to avoid dead tuples on non-indexed column changes]] — the PG-specific tuning that makes balance upserts efficient
- [[aggressive autovacuum tuning on balance tables prevents stale statistics and dead tuple accumulation from per-ledger updates]] — the maintenance tuning that keeps balance tables clean

Areas:
- [[entries/data-layer]]
