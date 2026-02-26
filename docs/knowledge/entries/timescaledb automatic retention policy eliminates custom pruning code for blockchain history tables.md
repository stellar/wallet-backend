---
context: Built-in add_retention_policy drops entire time chunks automatically — replacing cron jobs, scheduled DELETEs, and custom partition management that vanilla Postgres would require for data lifecycle
type: decision
status: active
created: 2026-02-26
areas: [data-layer, timescaledb, ingestion, operations]
---

# TimescaleDB automatic retention policy eliminates custom pruning code for blockchain history tables

## Context

Blockchain history tables grow indefinitely. Any production deployment must prune old data to maintain storage bounds. When the wallet-backend data layer was designed, the choice of database engine determined whether data lifecycle management required custom application code or came built-in.

## Detail

Vanilla PostgreSQL has no built-in mechanism for time-based data expiration on regular tables. Keeping unbounded history in check would require:

- A background job (cron, pg_cron, or application scheduler) to run on a defined interval
- DELETE statements targeting rows older than a retention threshold
- Careful tuning to avoid lock contention during large deletes
- Error handling and retrying for failed runs
- Monitoring and alerting when the job falls behind or fails
- Manual partition drop scripts if the table is date-partitioned

This is a nontrivial operational surface for what is fundamentally an infrastructure concern.

TimescaleDB's `add_retention_policy` function configures automatic chunk drop at the hypertable level. Because hypertables partition data into time-ordered chunks, dropping data older than a threshold is as simple as dropping the chunk that contains that time range — no row-by-row DELETE, no lock escalation, no custom scheduler. The drop happens as a TimescaleDB background job and produces no application-level code.

In the wallet-backend, `configureHypertableSettings()` in `internal/ingest/timescaledb.go` applies retention policy to all five history hypertables at startup (live mode only). The retention window is a configuration parameter. Changing retention requires only a config change and a live process restart, not a schema migration or code change.

## Implications

- No application code exists for data pruning. If a history table's data is growing unexpectedly, the first check should be whether retention policy is configured and the hypertable's background scheduler is running.
- Retention policy is only applied in live mode (`configureHypertableSettings` is skipped in backfill). This means a database initialized only via backfill will not have retention configured until the first live run.
- Adding a new history table requires explicitly calling `add_retention_policy` for that table in `configureHypertableSettings` — it is not automatic for new hypertables.
- The retention window interacts with chunk interval: if the chunk interval is 1 week and retention is 6 months, roughly 26 chunks exist at steady state. Extremely large chunk intervals with short retention could leave large dangling data.

## Source

[[why timescaleDB over vanilla Postgres]] (line 7) — retention policy cited as pillar 2 of the TimescaleDB decision

## Relevant Notes

- Since [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]], retention policy was one of four explicit criteria in the database selection — it is not an afterthought but part of the original architecture rationale.
- [[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] — retention policy is one of the settings `configureHypertableSettings` manages; that entry explains why backfill skips this call to avoid overwriting a running instance's tuned policies.
- [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]] — the append-only property is also what makes chunk-drop retention safe: there are no references or updates to old chunks from newer data; dropping a time range is clean.
- Retention policy is scoped to hypertables only — [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] are explicitly excluded from this mechanism and manage their data lifecycle through autovacuum and upsert semantics instead; chunk-drop automation does not apply to them.
