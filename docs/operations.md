# Production Operations

Reference for operating the wallet-backend's TimescaleDB-backed ingestion and API in production.

## TimescaleDB Upgrade Runbook

Upgrading a CNPG cluster across TimescaleDB minor releases:

1. Build a new `cnpg-timescaledb` image pinned to the target version (see `Dockerfile-timescale-cnpg`) and update the cluster's image reference.
2. Roll the cluster so every instance loads the new `.so`.
3. On the primary, apply the extension upgrade:
   ```sql
   ALTER EXTENSION timescaledb UPDATE TO '<version>';
   ```
   Multi-minor jumps (e.g. 2.25 -> 2.28) are supported in a single `ALTER EXTENSION` step; there is no need to step through each intermediate minor.
4. If the upgrade crosses 2.26 -> 2.27 or later, run the timescaledb-extras `2.27.x-fix-composite-bloom-columns.sql` catalog-rename script. Composite bloom index metadata created on 2.26 chunks is otherwise silently ignored by 2.27+. The script only renames catalog columns — it does not touch chunk data, is idempotent, and requires no recompression.
5. Verify:
   ```sql
   SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
   ```
   and spot-check an `EXPLAIN` on a chunk-skipping query to confirm the sparse index is still used.

Never stop an upgrade at 2.28.0 or 2.28.1 — both shipped migration-script defects that are fixed in 2.28.2. Land on 2.28.2 or later.

## Production Database Checklist

**CNPG `postgresql.parameters`:**

| Parameter | Value | Why |
|---|---|---|
| `timescaledb.enable_chunk_skipping` | `on` | Off by default. Min/max ranges used for chunk skipping are recorded at compression time, so this must be on **before** the first ingest — enabling it later requires recompressing all existing history to backfill the ranges. |
| `timescaledb.enable_sparse_index_bloom` | `on` | Enables bloom-filter sparse indexes on compressed chunks. |
| `timescaledb.stats_max_chunks` | `2048` (2.28+ only) | Steady-state compressed chunk count at 1-year retention is ~1,820, which exceeds the 1,024 default stats cache. |

**Instance sizing:**

| Resource | Target | Why |
|---|---|---|
| Memory | >= 64 GB, `shared_buffers` 16 GB, `effective_cache_size` ~48 GB | The actively-ingested day's chunk indexes total ~14 GB, fitting the shared_buffers 25%-of-RAM guidance. |
| Volume | >= 1.5 TB | Steady state at 1-year retention is ~1 TB (roughly 2.3 GB/day compressed across hypertables, plus balance tables), with the remainder as WAL/headroom. |

**Runtime flags** (`ingest` command):

| Flag / env var | Value | Why |
|---|---|---|
| `RETENTION_PERIOD` | `1 year` | Chunks older than this are dropped automatically. |
| `COMPRESSION_COMPRESS_AFTER` | `1 day` | Leaves one uncompressed day of headroom so gap-backfill writes land in the rowstore instead of a compressed chunk. |
| `COMPRESSION_SCHEDULE_INTERVAL` | `1 day` | How often the compression job checks for eligible chunks. |
| `COMPRESSION_MAX_CHUNKS` | `10` | Caps chunks compressed per job run to prevent overlapping runs. |
| `CHUNK_INTERVAL` | `1 day` | Hypertable chunk time interval; only affects newly created chunks. |

**GraphQL:** `GRAPHQL_INTROSPECTION_ENABLED` should be unset (or `false`) in production. Introspection makes the full schema, including any unreleased or internal-only fields, discoverable to anyone who can reach the endpoint. Leave it `true` in dev environments.

## Alerting & Failure Modes

**Alert on:**
- `wallet_ingestion_lag_ledgers` growth — ingestion falling behind the backend tip.
- Container restart counts — the ingest process exits immediately after a permanent failure.
- `wallet_db_query_errors_total{error_type="cursor_missing"}` — fires only when a protocol cursor that previously existed disappears, which is always a genuine incident (not a normal not-yet-initialized state).

**Do not alert on** `wallet_ingestion_retry_exhaustions_total` directly — the process exits right after incrementing it, so by the time an alert fires the process has already restarted. Treat it as a post-mortem breadcrumb, not a live signal.

**Poisoned ledgers:** a ledger that deterministically fails ingestion cannot self-heal by retrying — the ingest loop fails fast on permanent SQLSTATE classes, and recovery requires either a code fix or a manually-guarded cursor bump past the ledger.

**TimescaleDB job health** has no application-level metric. Monitor `timescaledb_information.job_stats` and `timescaledb_information.job_errors` directly via SQL (through the CNPG metrics exporter or a scheduled check) to catch stalled or failing compression/retention jobs.

## Connection Topology Constraint

Ingestion connects directly to the CNPG `rw` service and relies on pgx's server-side prepared-statement caching for performance. If a transaction-pooling PgBouncer/Pooler is ever placed in front of the ingest path, its pool mode must be switched to `QueryExecModeExec` — transaction pooling is incompatible with server-side prepared statements. The API server already runs in `Exec` mode.
