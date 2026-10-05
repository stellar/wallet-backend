# Operations runbook

For the person on call for a wallet-backend deployment. After reading it you can size a deployment, read the metrics that matter, and handle the failures that happen.

## Contents

- [Sizing](#sizing)
- [Metrics to alert on](#metrics-to-alert-on)
- [Restarts and cursors](#restarts-and-cursors)
- [Failure modes](#failure-modes)
- [Retention and compression](#retention-and-compression)
- [Database maintenance](#database-maintenance)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every one has a flag twin in the [configuration reference](../configuration.md).

## Sizing

Measured on a pubnet deployment on 2026-10-01: PostgreSQL 17.6, TimescaleDB 2.28.2, three 8-CPU / 32 GiB database nodes on gp3 storage, 1-day chunks, ingest pod 4 CPU / 8 GiB request. Treat the figures as a starting point and measure your own.

| What | Measured | Notes |
|---|---|---|
| Pubnet ledgers per day | about 17,280 | one every 5 seconds |
| Rows per ledger | about 306 transactions, 721 operations, 1,142 state changes | late September 2026 traffic |
| Compressed size of one day (five hypertables) | about 1.7 GB | 204 MB transactions, 185 MB transactions_accounts, 621 MB operations, 200 MB operations_accounts, 491 MB state_changes |
| Compression ratio | 5.6x to 17.4x per table | `operations` compresses least, `operations_accounts` most |
| Open (uncompressed) chunk | up to about 18 GB per day | the current day's chunk sits uncompressed until `COMPRESSION_COMPRESS_AFTER` elapses |
| Current-state tables, pubnet | about 11 GB | `trustline_balances` 8.2 GB, `native_balances` 2.4 GB, the rest under 200 MB; filled at first start from the history archive |
| Live ingest, per ledger | mean 0.16 s, p99 0.44 s | ingest pod used 0.06 CPU and 0.37 GiB on average |
| Ingest lag | 0 ledgers at p50, 1 at p99 over 48 hours | `wallet_ingestion_lag_ledgers` |
| API latency | p99 50 ms | near-zero load; measure under yours |

Disk plan for pubnet: about 11 GB fixed, plus about 1.7 GB per retained day compressed, plus headroom for one uncompressed day (about 20 GB). A year of history is roughly 0.6 TB compressed. Testnet is a fraction of this.

Backfill is CPU-bound in the ingest process (`BACKFILL_WORKERS` defaults to one per CPU) and write-bound in the database. Run it on a bigger pod than live ingest and set `COMPRESSION_MAX_CHUNKS` (around 10) so the compression job does not overlap itself while chunks fill fast.

## Metrics to alert on

Scrape `/ingest-metrics` on the ingest port (default 8002) and `/api-metrics` on the API port (default 8001). Full list in [observability](observability.md).

| Alert | Expression | Meaning |
|---|---|---|
| Ingest behind | `wallet_ingestion_lag_ledgers > 10` for 5 minutes | ingest is not keeping up with RPC, or RPC is ahead of the data lake |
| Ingest stalled | `increase(wallet_ingestion_ledgers_total[10m]) == 0` | the loop stopped; check logs for the advisory lock or a permanent fetch error |
| Ingest erroring | `increase(wallet_ingestion_errors_total[10m]) > 0` | a ledger failed; the loop retries |
| Retries exhausted | `increase(wallet_ingestion_retry_exhaustions_total[10m]) > 0` | the process is about to exit |
| RPC unhealthy | `wallet_rpc_service_health == 0` | `/health` returns 500 (unreachable) or 503 (unhealthy) on both ingest and API |
| Pool saturated | `rate(wallet_db_pool_acquire_wait_seconds_total[5m])` rising or `wallet_db_pool_total_conns` at `wallet_db_pool_max_conns` | raise `DB_MAX_CONNS` or add PgBouncer |
| API errors | `rate(wallet_http_requests_total{status_code=~"5.."}[5m]) > 0` | check API logs and database health |
| Auth rejections | `rate(wallet_auth_expired_signatures_total[5m])` | clients signing with a window beyond `CLIENT_AUTH_MAX_TIMEOUT_SECONDS` or with clock skew |

`/health` on both processes returns 500 when the RPC cannot be reached, 503 when the RPC reports itself unhealthy, and 503 when the RPC's latest ledger is more than 50 ahead of `latest_ingest_ledger`. A Kubernetes liveness probe on the API that uses `/health` restarts API pods when ingest lags; use a readiness probe or a plain TCP check for liveness instead.

## Restarts and cursors

| Cursor (`ingest_store.key`) | Meaning |
|---|---|
| `latest_ingest_ledger` | Last ledger committed by live ingest. Live mode resumes at this plus one. |
| `oldest_ingest_ledger` | Oldest ledger present. Backfill moves it down; retention moves it up through the reconcile job. |
| `protocol_<ID>_history_cursor`, `protocol_<ID>_current_state_cursor` | Progress of protocol data migrations. |

A ledger is written in one transaction. A SIGTERM mid-ledger rolls it back and the next start re-ingests it. Stopping and starting ingest is safe at any point.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Ingest exits with `advisory lock not acquired` | Another live ingester for the same network passphrase holds the lock on this database | Run one live ingester per network. After a database failover the new pod takes the lock once the old session is gone. |
| Ingest exits with `--start-ledger and --end-ledger apply to --ingestion-mode=backfill only` | `START_LEDGER` or `END_LEDGER` set on a live process | Unset them. |
| Backfill exits with `end ledger ... cannot be greater than latest ingested ledger` | Range extends past what live ingest has written | Set `END_LEDGER` at or below `latest_ingest_ledger`. |
| First start takes a long time with no ledgers ingested | Checkpoint bootstrap is downloading the history archive and filling balance tables | Wait. Watch the ingest log for the checkpoint completion line, then `wallet_ingestion_latest_ledger` starts moving. |
| `/health` 500 or 503, logs show `getHealth` errors | RPC unreachable (500) or not synced (503) | Fix RPC. Ingest retries; API serves stale data but reports unhealthy. |
| Datastore backend exits with a buffer error | The data-lake reader died (missing files, credentials, throttling) | This is classified permanent; the process exits. Restart after fixing access. |
| `migrate up` fails on `tsdb.` or `sparse_index` options | TimescaleDB extension missing, too old, or `timescaledb.enable_sparse_index_bloom` off | See [running](running.md#requirements). |
| API 401 on every request | `CLIENT_AUTH_PUBLIC_KEYS` set and clients unsigned, or wrong key | See [authentication](../api/authentication.md). |
| API 500 on auth | Body larger than `CLIENT_AUTH_MAX_BODY_SIZE_BYTES` could not be read | Raise the limit or shrink the request. |
| Disk fills faster than expected | Compression not running (no policy job, `COMPRESSION_COMPRESS_AFTER` too long) or retention off | Check `timescaledb_information.jobs`; set `RETENTION_PERIOD` if you do not need full history. |

## Retention and compression

The live ingester converges these on every start:

| Variable | Effect |
|---|---|
| `CHUNK_INTERVAL` | Chunk size for future chunks on all five hypertables. Default `1 day`. |
| `RETENTION_PERIOD` | Adds or updates a drop-chunks policy. Empty removes it and the reconcile job. |
| `COMPRESSION_SCHEDULE_INTERVAL` | How often the compression job wakes. Empty leaves the job alone. |
| `COMPRESSION_COMPRESS_AFTER` | How long after a chunk closes it becomes eligible. On the measured deployment, 1 hour removed persist-latency spikes that a 1-minute setting caused. |
| `COMPRESSION_MAX_CHUNKS` | Chunks compressed per job run. Set around 10 during backfill. |

Retention drops whole chunks, so the oldest data disappears in `CHUNK_INTERVAL`-sized steps. A reconcile job runs hourly to move `oldest_ingest_ledger` to the real minimum after a drop.

Backfill processes never change these policies.

## Database maintenance

- Back up with your PostgreSQL tooling; the schema is ordinary tables plus TimescaleDB hypertables, and a logical dump needs `timescaledb_pre_restore()` / `timescaledb_post_restore()` around the restore.
- The API opens its pool in exec mode, so PgBouncer in transaction-pooling mode works for the API. Ingest holds a session-level advisory lock and needs a direct connection or session pooling.
- Reads for the API can go to a replica. Ingest must write to the primary.

## Where in the code

| Path | Role |
|---|---|
| `internal/ingest/timescaledb.go` | Chunk interval, retention, compression job tuning, reconcile job |
| `internal/services/ingest_live.go` | Advisory lock, cursor resume, checkpoint bootstrap trigger |
| `internal/services/ingest_backfill.go` | Gap calculation and range checks |
| `internal/serve/httphandler/health.go` | `/health` rules (500 vs 503) |
| `internal/metrics/` | Every `wallet_*` metric |
| `internal/data/ingest_store.go` | Cursor keys |
