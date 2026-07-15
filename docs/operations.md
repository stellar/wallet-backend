# Production Operations

Reference for operating the wallet-backend's TimescaleDB-backed ingestion and API in production.

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

**Hypertable index usage:** `pg_stat_user_indexes` on the `public` schema always reports `idx_scan = 0` for hypertable indexes — scans accrue on the per-chunk indexes in `_timescaledb_internal`, and the parent's index entry is only a template. To measure real usage (e.g. for an unused-index audit), aggregate chunk-index stats:

```sql
SELECT ht.table_name AS hypertable, regexp_replace(sui.indexrelname, '^_hyper_\d+_\d+_chunk_', '') AS index_name,
       sum(sui.idx_scan) AS scans
FROM pg_stat_user_indexes sui
JOIN _timescaledb_catalog.chunk c ON c.schema_name = sui.schemaname AND c.table_name = sui.relname
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```

Every index in the schema must have a nameable consumer query; secondary indexes duplicating a primary key's column set are not kept (the PK's column order is chosen to serve its consumers directly — B-tree scans run forward or backward, so ASC/DESC variants of the same key are one index, not two).

## Connection Topology Constraint

Ingestion connects directly to the CNPG `rw` service and relies on pgx's server-side prepared-statement caching for performance. If a transaction-pooling PgBouncer/Pooler is ever placed in front of the ingest path, its pool mode must be switched to `QueryExecModeExec` — transaction pooling is incompatible with server-side prepared statements. The API server already runs in `Exec` mode.
