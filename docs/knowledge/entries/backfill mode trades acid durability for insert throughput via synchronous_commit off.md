---
description: OpenDBConnectionPoolForBackfill sets synchronous_commit=off and attempts FK disable; live mode keeps full ACID; intentional design tradeoff for historical ingestion speed
type: decision
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, backfill, timescaledb, postgresql, performance, durability]
---

# Backfill mode trades ACID durability for insert throughput via synchronous_commit=off

## Context

Historical backfill processes hundreds of ledgers in parallel. Full ACID guarantees on every insert would throttle throughput to WAL-sync speed. Since backfill data is re-derivable from the Stellar history archive, some durability risk is acceptable.

## Detail

`OpenDBConnectionPoolForBackfill` appends `options=-c synchronous_commit=off` to the connection string. This applies to every connection in the pool without per-query hints. Separately, `ConfigureBackfillSession(ctx, db)` executes `SET session_replication_role = 'replica'` to disable FK constraint checking (requires superuser; if it fails, a warning is logged and backfill continues without this optimization).

Live mode uses `OpenDBConnectionPool` with no modifications — full WAL sync and FK enforcement are preserved.

The tradeoff is explicit: if the process crashes during backfill, some recent writes may not be on disk. This is acceptable because:
1. Gap detection will re-fill the missing ledgers on the next backfill run.
2. The `latestLedgerCursor` is NOT advanced during backfill for historical mode — only `oldestCursor` moves.

The safety of accepting durability risk depends on a structural property of the data: since [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]], every lost write can be re-derived cleanly from the Stellar history archive — there are no in-place modifications to lose, only missing insertions to re-run.

## Implications

- Never use `OpenDBConnectionPoolForBackfill` in the serve process — it's exclusively for bulk historical ingestion.
- A backfill crash is recoverable: restart with the same parameters and gap detection picks up where it left off.
- `ConfigureBackfillSession` requires a DB user with `REPLICATION` role or superuser; absence of this role degrades performance but doesn't break correctness.

## Source

`internal/db/db.go:OpenDBConnectionPoolForBackfill()`
`internal/db/db.go:ConfigureBackfillSession()`
`internal/ingest/ingest.go:setupDeps()` — mode-specific pool selection

## Related

The same mode-specific setup skips [[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] — both synchronous_commit=off and skipping configureHypertableSettings are backfill-only configurations that share the rationale of not interfering with a running live instance.

The COPY strategy in [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] pairs with synchronous_commit=off to form the full backfill performance profile — reduced WAL sync overhead and binary COPY protocol together enable the insert throughput needed for historical ingestion.

`session_replication_role = 'replica'` appears here at pool/session level (for all backfill connections), and also in [[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] at transaction level (LOCAL to a single transaction in `PopulateAccountTokens()`). The scope differs: backfill applies it connection-wide, checkpoint reverts it at COMMIT — both use FK-disable to handle the out-of-order insertion problem.

relevant_notes:
  - "[[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] — synthesizes with this: both entries describe backfill-only configurations; synchronous_commit=off and skipped hypertable settings share the same rationale"
  - "[[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — synthesizes with this: COPY and synchronous_commit=off are the two main backfill throughput optimizations; this entry handles WAL overhead, that entry handles per-row overhead"
  - "[[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] — contrasts with this: both apply session_replication_role=replica for FK-disable, but at different scopes — backfill applies it connection-wide; checkpoint applies it LOCAL to a single transaction and reverts at COMMIT"
