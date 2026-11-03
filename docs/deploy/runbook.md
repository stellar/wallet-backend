# Operations runbook

For the person on call for a wallet-backend deployment. After reading it you can size a deployment, read the metrics that matter, and handle the failures that happen.

## Contents

- [Hardware](#hardware)
- [Sizing](#sizing)
- [Metrics to alert on](#metrics-to-alert-on)
- [Restarts and cursors](#restarts-and-cursors)
- [Failure modes](#failure-modes)
- [Retention and compression](#retention-and-compression)
- [Database maintenance](#database-maintenance)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every one has a flag twin in the [configuration reference](configuration.md).

## Hardware

Figures come from the same pubnet and testnet deployments measured in [Sizing](#sizing), peaks over seven days ending 2026-10-06. "Minimum" is what the process needs to run through its heaviest phase; "Recommended" is what we run. Each process can live on its own host or share one; the ingester and the database do the work, the API is light.

| Component | Pubnet minimum | Pubnet recommended | Testnet | What drives it |
| --- | --- | --- | --- | --- |
| Live ingester (one per network) | 2 CPU, 12 GiB RAM | 4 CPU, 16 GiB RAM | 1 CPU, 4 GiB RAM | The first start loads the ledger-entry balances (native, trustline, liquidity pool, SAC) from the history archive checkpoint; SEP-41 balances come from `protocol-migrate current-state`. The load it peaked at 1.8 cores and 9.4 GiB on pubnet, 0.8 cores and 1.9 GiB on testnet. Steady state is 0.1 cores and 0.4 GiB |
| Backfill (optional, runs alongside) | 2 CPU, 4 GiB RAM | as many CPUs as you want speed; size RAM from the formula | same | CPU-bound; `BACKFILL_WORKERS` defaults to one per CPU. Memory is about `(DATASTORE_BUFFER_SIZE + BACKFILL_WORKERS + BACKFILL_FLUSH_SIZE) × 30 MB` at mainnet density, so it grows with each of the three. The run logs the estimate at start |
| API (per instance) | 0.5 CPU, 512 MiB RAM | 1 CPU, 1 GiB RAM, two or more instances | same | Stateless; near zero at low traffic (20 MiB resident). Scale out for throughput, not up |
| PostgreSQL + TimescaleDB | 4 CPU, 16 GiB RAM, SSD | 8 CPU, 32 GiB RAM, SSD | 2 CPU, 4 GiB RAM, SSD | Working set of about 20 GiB on pubnet (shared buffers and the current-state tables), 1.5 GiB on testnet; CPU peaks under 1 core |
| Disk for PostgreSQL | 11 GB + 1.7 GB per retained day + 20 GB headroom | 1 TB for a year of pubnet history | 1 GB + 0.12 GB per retained day | See [Sizing](#sizing) |
| stellar-rpc | its own host | | | Sized by the [stellar-rpc documentation](https://developers.stellar.org/docs/data/apis/rpc); the ingester only needs it reachable and synced |

Checked against a different network or a later protocol, every number here can move. Measure your own after the first day.

## Sizing

Measured on a pubnet deployment on 2026-10-01: PostgreSQL 17.6, TimescaleDB 2.28.2, a three-instance database on 8 CPU / 32 GiB hosts with SSD storage, 1-day chunks, and an ingest process allotted 4 CPU / 8 GiB. Testnet figures come from a testnet deployment on the same setup (2.5 GB database, 45,408 ledgers ingested). Peak and steady-state CPU and memory are over seven days ending 2026-10-06. Treat the figures as a starting point and measure your own.

| What | Measured | Notes |
|---|---|---|
| Pubnet ledgers per day | about 17,280 | one every 5 seconds |
| Rows per ledger | about 306 transactions, 721 operations, 1,142 state changes | late September 2026 traffic |
| Compressed size of one day (five hypertables) | about 1.7 GB | 204 MB transactions, 185 MB transactions_accounts, 621 MB operations, 200 MB operations_accounts, 491 MB state_changes |
| Compression ratio | 5.6x to 17.4x per table | `operations` compresses least, `operations_accounts` most |
| Open (uncompressed) chunk | up to about 18 GB per day | the current day's chunk sits uncompressed until `COMPRESSION_COMPRESS_AFTER` elapses |
| Current-state tables, pubnet | about 11 GB | `trustline_balances` 8.2 GB, `native_balances` 2.4 GB, the rest under 200 MB; filled at first start from the history archive |
| Live ingest, per ledger | mean 0.16 s, p99 0.44 s | the ingest process used 0.06 CPU and 0.37 GiB on average |
| Ingest lag | 0 ledgers at p50, 1 at p99 over 48 hours | `wallet_ingestion_lag_ledgers` |
| API latency | p99 50 ms | near-zero load; measure under yours |

Disk plan for pubnet: about 11 GB fixed, plus about 1.7 GB per retained day compressed, plus headroom for one uncompressed day (about 20 GB). A year of history is roughly 0.6 TB compressed. Testnet is a fraction of this.

Lookup table (`transaction_hashes`), measured on a pubnet history of 4.4 billion rows: 35 GB, 8.5 bytes per row. The table's maintenance runs delete the lookup rows of 30-day periods that closed before the oldest `transactions` row (whole compressed batches: 15 ms and 181 kB of WAL per chunk, no lock, measured) and recompress every chunk (68 ms and 3.4 MB of WAL per chunk for a week of rows, measured). Measured on the live dev mainnet table (4.4 billion rows, 1,024 chunks) with a week of rows in the rowstore parts, 40 million: one worker 17 min 7 s and about 11.5 GB of WAL, eight workers 2 min 47 s and about 10 GB. Live ingestion ran at its baseline through both (per-ledger p99 480 to 640 ms, no lag) and lookups ran alongside. A run with minutes of rows takes under 4 minutes on one worker. A retention step on an 8-chunk copy of real rows: 6 to 13 ms and 490 kB of WAL per chunk for three periods, 3.5 million rows. Figures at 10k tx/s: [Scaling by transaction rate](../architecture/data-model.md#scaling-by-transaction-rate).

Backfill is CPU-bound in the ingest process (`BACKFILL_WORKERS` defaults to one per CPU) and write-bound in the database. Give it more CPU and memory than live ingest. It compresses the chunks it writes on its own connections, recompresses the lookup table, and pauses every compression policy and the lookup table's maintenance jobs for the run, so it needs no `COMPRESSION_MAX_CHUNKS` change. Plan disk for about `(ceil(BACKFILL_COMPRESS_CONCURRENCY / 5) + 1) × 15 GB` of uncompressed windows on top of the compressed history, and for the live ingester's recent chunks, which stay uncompressed until the run ends. Both are mainnet estimates. The database needs `DB_MAX_CONNS` of at least 5 + `BACKFILL_COMPRESS_CONCURRENCY` + 1. See [backfill](backfill.md#sizing-and-tuning).

## Metrics to alert on

Scrape `/ingest-metrics` on the ingest port (default 8002) and `/api-metrics` on the API port (default 8001). Full list in [observability](observability.md).

| Alert | Expression | Meaning |
|---|---|---|
| Ingest behind | `wallet_ingestion_lag_ledgers > 10` for 5 minutes | ingest is not keeping up with RPC, or RPC is ahead of the data lake |
| Ingest stalled | `increase(wallet_ingestion_ledgers_total[10m]) == 0` | the loop stopped; check logs for the advisory lock or a permanent fetch error |
| Ingest erroring | `increase(wallet_ingestion_errors_total[10m]) > 0` | a ledger failed; the loop retries |
| Retries exhausted | `increase(wallet_ingestion_retry_exhaustions_total[10m]) > 0` | the process is about to exit |
| RPC unhealthy | `wallet_rpc_service_health == 0` | `/health` returns 500 (unreachable) or 503 (unhealthy) on both ingest and API |
| Pool saturated | `rate(wallet_db_pool_acquire_wait_seconds_total[5m])` rising or `wallet_db_pool_total_conns` at `wallet_db_pool_max_conns` | raise `DB_MAX_CONNS` (ingest needs at least 7) or add PgBouncer for the API |
| Persist falling behind | `histogram_quantile(0.5, rate(wallet_ingestion_persist_batch_size_bucket[5m])) > 1` | the persist stage is coalescing a backlog; look at database write latency |
| API errors | `rate(wallet_http_requests_total{status_code=~"5.."}[5m]) > 0` | check API logs and database health |
| Auth rejections | `rate(wallet_auth_expired_signatures_total[5m])` | clients signing with a window beyond `CLIENT_AUTH_MAX_TIMEOUT_SECONDS` or with clock skew |

`/health` on both processes returns 500 when the RPC cannot be reached, 503 when the RPC reports itself unhealthy, and 503 when the RPC's latest ledger is more than 50 ahead of `latest_ingest_ledger`. Use `/health` to decide whether an API instance should receive traffic, not whether to restart it: a restart policy keyed on `/health` restarts healthy API instances whenever ingest lags. For restart decisions use a TCP or process check.

## Restarts and cursors

| Cursor (`ingest_store.key`) | Meaning |
|---|---|
| `latest_ingest_ledger` | Last ledger committed by live ingest. Live mode resumes at this plus one. |
| `oldest_ingest_ledger` | Oldest ledger present. Backfill moves it down; retention moves it up through the reconcile job. |
| `protocol_<ID>_history_cursor`, `protocol_<ID>_current_state_cursor` | Progress of protocol data migrations. |

A batch of ledgers (one by default) is written by a set of transactions whose cursor commit is strictly last. A SIGTERM before that commit rolls the batch back and the next start re-ingests it; a crash between the commits leaves only bulk rows above the cursor, which the next start deletes. Stopping and starting ingest is safe at any point.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Ingest exits with `advisory lock not acquired` | Another live ingester for the same network passphrase holds the lock on this database | Run one live ingester per network. After a database failover the new ingester takes the lock once the old session is gone. |
| Ingest exits with `--start-ledger and --end-ledger apply to --ingestion-mode=backfill only` | `START_LEDGER` or `END_LEDGER` set on a live process | Unset them. |
| Backfill exits with `--end-ledger does not apply to backfill` | `END_LEDGER` is set on a backfill process | Unset it. The backfill always ends at the history live ingest holds. |
| Backfill exits with `no ingestion cursors yet` | Live ingestion has never run on this database | Start live ingestion once, then rerun. |
| Backfill exits non-zero after a window failed | A fetch, write or compression error that retries did not clear | Read the log, fix the cause, and rerun with the same `START_LEDGER`. It resumes from `oldest_ingest_ledger`. |
| First start takes a long time with no ledgers ingested | Checkpoint bootstrap is downloading the history archive and filling balance tables | Wait. Watch the ingest log for the checkpoint completion line, then `wallet_ingestion_latest_ledger` starts moving. |
| `/health` 500 or 503, logs show `getHealth` errors | RPC unreachable (500) or not synced (503) | Fix RPC. Ingest retries; API serves stale data but reports unhealthy. |
| Datastore backend exits with a buffer error | The data-lake reader died (missing files, credentials, throttling) | This is classified permanent; the process exits. Restart after fixing access. |
| `migrate up` fails on `tsdb.` or `sparse_index` options | TimescaleDB extension missing, too old, or `timescaledb.enable_sparse_index_bloom` off | See [running](install.md#requirements). |
| API 401 on every request | `CLIENT_AUTH_PUBLIC_KEYS` set and clients unsigned, or wrong key | See [authentication](../api/authentication.md). |
| API 401 on large signed requests only | Body larger than `CLIENT_AUTH_MAX_BODY_SIZE_BYTES`: the server hashes only that many bytes, so the signature no longer matches | Raise the limit or shrink the request. A 500 on auth means the body could not be read at all. |
| Disk fills faster than expected | Compression not running (no policy job, `COMPRESSION_COMPRESS_AFTER` too long) or retention off | Check `timescaledb_information.jobs`; set `RETENTION_PERIOD` if you do not need full history. |

## Retention and compression

The live ingester converges these on every start:

| Variable | Effect |
|---|---|
| `CHUNK_INTERVAL` | Chunk size for future chunks on the five bulk hypertables. Default `1 day`. |
| `RETENTION_PERIOD` | Adds or updates a drop-chunks policy on the five bulk hypertables. Empty removes it and the reconcile job. The lookup table follows `transactions` through its maintenance jobs |
| `TX_HASH_MAINTENANCE_INTERVAL` | How often each `transaction_hashes_maintenance` job of the lookup table runs. Default `1 day`. Set it so the rows written between runs stay in the low hundreds of millions. |
| `TX_HASH_MAINTENANCE_WORKERS` | Number of maintenance jobs for the lookup table. Default `1`. Each job takes a share of the chunks, so a run finishes about that many times faster. Each job is one TimescaleDB background worker and counts against `timescaledb.max_background_workers`. |
| `COMPRESSION_SCHEDULE_INTERVAL` | How often the compression job wakes. Empty leaves the job alone. |
| `COMPRESSION_COMPRESS_AFTER` | How long after a chunk closes it becomes eligible. On the measured deployment, 1 hour removed persist-latency spikes that a 1-minute setting caused. |
| `COMPRESSION_MAX_CHUNKS` | Chunks compressed per job run. |

Retention drops whole chunks, so the oldest data disappears in `CHUNK_INTERVAL`-sized steps. A reconcile job runs hourly to move `oldest_ingest_ledger` to the real minimum after a drop.

`COMPRESSION_SCHEDULE_INTERVAL`, `COMPRESSION_COMPRESS_AFTER` and `COMPRESSION_MAX_CHUNKS` apply to the five bulk hypertables only. The `transaction_hashes` lookup table has no columnstore or retention policy. The procedure `transaction_hashes_maintenance` runs as `TX_HASH_MAINTENANCE_WORKERS` TimescaleDB jobs on the `TX_HASH_MAINTENANCE_INTERVAL` schedule. Each job recompresses its chunks, folding rows written since the last run from the rowstore part into the columnstore. These two settings are the only ones the ingester applies to the table. `RETENTION_PERIOD` and `CHUNK_INTERVAL` do not apply to it. The lookup table follows `transactions`: each run removes the lookup rows of 30-day periods that closed before the oldest `transactions` row, whole compressed batches at a time. With retention off, no rows are removed.

A backfill never changes these settings. It pauses every compression policy and the lookup table's maintenance jobs while it runs. It compresses the history chunks it wrote itself, recompresses the whole lookup table after every 500 million lookup rows written and once at the end, and schedules the paused jobs again when it exits. The live ingester schedules them again at every start, so a killed backfill leaves nothing to repair. Chunks that also hold live rows stay with the live policies.

Check the lookup table's maintenance jobs with:

```sql
SELECT j.job_id, j.config->>'worker' AS worker, s.last_run_status,
       s.last_successful_finish, s.last_run_duration, s.total_failures, s.next_start
FROM timescaledb_information.jobs j
JOIN timescaledb_information.job_stats s USING (job_id)
WHERE j.proc_name = 'transaction_hashes_maintenance';
```

Act on any of these: `last_successful_finish` older than about twice `TX_HASH_MAINTENANCE_INTERVAL`, `total_failures` rising, or `last_run_duration` near the interval. Each run also writes one server log line (`transaction_hashes_maintenance worker i/N: ...`) with the chunks it touched, the rowstore rows it compressed, the rows it deleted and its duration; the rowstore rows per run are the number to size the interval by. A run that takes longer than the interval needs more workers (`TX_HASH_MAINTENANCE_WORKERS`) or a longer interval.

## Database maintenance

- Back up with your PostgreSQL tooling; the schema is ordinary tables plus TimescaleDB hypertables, and a logical dump needs `timescaledb_pre_restore()` / `timescaledb_post_restore()` around the restore.
- The API opens its pool in exec mode, so PgBouncer in transaction-pooling mode works for the API. Ingest holds a session-level advisory lock and needs a direct connection or session pooling.
- Reads for the API can go to a replica. Ingest must write to the primary.

## Where in the code

| Path | Role |
|---|---|
| `internal/ingest/timescaledb.go` | Chunk interval, retention, compression job tuning, reconcile job |
| `internal/services/ingest_live.go` | Advisory lock, cursor resume, checkpoint bootstrap trigger |
| `internal/services/ingest_backfill.go` | Backfill windows, cursor, retries |
| `internal/services/backfill_compressor.go` | Backfill compression, lookup recompression and policy pause |
| `internal/serve/httphandler/health.go` | `/health` rules (500 vs 503) |
| `internal/metrics/` | Every `wallet_*` metric |
| `internal/data/ingest_store.go` | Cursor keys |
