# Observability

For operators wiring wallet-backend into Prometheus, dashboards and log shipping. After reading it you can scrape every process, pick health probes, alert on the right series and read the logs.

## Endpoints

None of these endpoints take authentication, so keep them off the public network.

| Process | Path | Port variable | Auth | Serves |
| --- | --- | --- | --- | --- |
| `serve` | `/health` | `PORT` (default 8001) | none | Health check, see [Health](#health) |
| `serve` | `/api-metrics` | `PORT` (default 8001) | none | Prometheus registry of the API process. At most 5 concurrent scrapes, 10 s scrape timeout |
| `serve` | `/debug/pprof/` | `ADMIN_PORT` (default 0, off) | none | Go pprof, see [pprof](#pprof) |
| `ingest` | `/health` | `INGEST_SERVER_PORT` (default 8002) | none | Same health check as `serve` |
| `ingest` | `/ingest-metrics` | `INGEST_SERVER_PORT` (default 8002) | none | Prometheus registry of the ingest process |
| `ingest` | `/debug/pprof/` | `ADMIN_PORT` (default 0, off) | none | Go pprof |
| `protocol-migrate` | `/metrics` | `--metrics-port` flag, no env var (default 0, off) | none | Prometheus registry of the migration run, for the life of the run |

Each process has its own registry. Every registry carries all the `wallet_*` families below, but a family only gets samples in the process that does that work. For example, `wallet_http_*` and `wallet_graphql_*` fill only on `serve`, and `wallet_ingestion_*` fills only on `ingest`.

## Health

`serve` and `ingest` run the same handler. It checks, in order:

| Step | Condition | Response |
| --- | --- | --- |
| 1 | RPC `getHealth` call fails | `500` |
| 2 | RPC `getHealth` status is not `healthy` | `503`, `"error": "rpc is not healthy"` |
| 3 | Reading the `latest_ingest_ledger` cursor fails | `500` |
| 4 | RPC latest ledger minus `latest_ingest_ledger` is more than 50 | `503`, with both ledgers in `extras` |
| 5 | Otherwise | `200` |

Healthy response:

```json
{
  "status": "ok",
  "backend_latest_ledger": 123456
}
```

Out-of-sync response:

```json
{
  "error": "wallet backend is not in sync with the RPC",
  "extras": {
    "rpc_latest_ledger": 123520,
    "backend_latest_ledger": 123456
  }
}
```

Warning: the API's `/health` fails when ingest lags, so a liveness probe on it restarts healthy API pods during an ingest stall. Use it as a readiness probe.

## Metrics

Every registry also carries the standard Go runtime (`go_*`) and process (`process_*`) collectors, so heap, GC and goroutine counts are visible for `serve`, `ingest` and `protocol-migrate`.

### HTTP

Recorded on `serve` only. `endpoint` is the chi route pattern, or `unmatched` for requests that hit no route. `method` is the HTTP method, or `OTHER` for non-standard methods.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_http_requests_total` | counter | `endpoint`, `method`, `status_code` | HTTP requests served |
| `wallet_http_request_duration_seconds` | histogram | `endpoint`, `method` | Request duration. The top bucket is 30 s, the request timeout, so slow requests that finish land in a finite bucket |

### GraphQL

`operation_name` is the first root field of the query (`accountByAddress`, `transactionByHash`, `operationById`), never the client-supplied operation name. It is `<unnamed>` when no root field is found. `operation_type` is `query`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_graphql_operation_duration_seconds` | histogram | `operation_name`, `operation_type` | End-to-end operation time: parse, validation and all resolvers. The primary latency SLO metric |
| `wallet_graphql_operations_total` | counter | `operation_name`, `operation_type`, `status` | Completed operations. `status` is `success` or `error` |
| `wallet_graphql_in_flight_operations` | gauge | none | Operations being processed. A sustained high value means the server is saturated |
| `wallet_graphql_response_size_bytes` | histogram | `operation_name`, `operation_type` | Response body size. Catches oversized pages and broad selections |
| `wallet_graphql_complexity` | histogram | `operation_name` | Computed complexity score per query. Values near the limit point at heavy clients |
| `wallet_graphql_errors_total` | counter | `operation_name`, `error_type` | Errors by class: `validation_error`, `parse_error`, `bad_input`, `auth_error`, `forbidden`, `internal_error`, `unknown` |
| `wallet_graphql_deprecated_fields_total` | counter | `operation_name`, `field_name` | Reads of `@deprecated` fields. `field_name` is the field path. No series exist until a deprecated field is read |

### Dataloaders

`loader` is a fixed loader name.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_graphql_dataloader_batch_size` | histogram | `loader` | Distinct keys per batch. A distribution stuck at 1 means the loader never batches |
| `wallet_graphql_dataloader_fetch_duration_seconds` | histogram | `loader` | Time for one batch's fetch, across all its sub-fetches |

### Auth

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_auth_expired_signatures_total` | counter | none | Requests rejected with `401` because the JWT had expired. Only moves when `CLIENT_AUTH_PUBLIC_KEYS` is set |

### Database

`query_type` is the data-layer method name, for example `GetByHash`, `BatchCopy` or `BatchGetByToIDs`. `table` is the table it touches. `error_type` is a Postgres error class such as `unique_violation`, `deadlock`, `query_canceled` or `connection_closed`, or a guard failure such as `row_count_mismatch`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_db_query_duration_seconds` | histogram | `query_type`, `table` | Query latency. The primary DB performance metric |
| `wallet_db_queries_total` | counter | `query_type`, `table` | Completed queries |
| `wallet_db_query_errors_total` | counter | `query_type`, `table`, `error_type` | Failed queries by error class |
| `wallet_db_batch_operation_size` | histogram | `operation`, `table` | Rows per batch read or write. `operation` holds the method name |

### Connection pool

The pgx pool of each process. No labels.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_db_pool_acquired_conns` | gauge | none | Connections checked out by the application |
| `wallet_db_pool_idle_conns` | gauge | none | Connections ready for use. Constant zero means every acquire waits or dials |
| `wallet_db_pool_constructing_conns` | gauge | none | Connections being dialed. Spikes follow pool exhaustion or a database restart |
| `wallet_db_pool_total_conns` | gauge | none | All open connections: acquired, idle and constructing |
| `wallet_db_pool_max_conns` | gauge | none | Configured pool size. Divide `acquired_conns` by it for utilization |
| `wallet_db_pool_acquire_total` | counter | none | Connection acquisitions |
| `wallet_db_pool_acquire_wait_seconds_total` | counter | none | Total time spent waiting to acquire. Divide its rate by the `acquire_total` rate for mean wait |
| `wallet_db_pool_empty_acquire_total` | counter | none | Acquires that found no idle connection. A high share of `acquire_total` means the pool is too small |
| `wallet_db_pool_empty_acquire_wait_seconds_total` | counter | none | Wait time spent while the pool was empty |
| `wallet_db_pool_new_conns_total` | counter | none | Connections dialed. A high rate means churn |
| `wallet_db_pool_canceled_acquire_total` | counter | none | Acquires canceled by a context timeout. Any rate above zero means the application gave up waiting for a connection |
| `wallet_db_pool_max_lifetime_destroy_total` | counter | none | Connections closed by the max-lifetime policy |
| `wallet_db_pool_max_idle_destroy_total` | counter | none | Connections closed by the max-idle-time policy |

### Worker pools

Ingest worker pools. Each series carries a constant `pool_name` label: `ledger_indexer` or `backfill`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_pool_workers_running` | gauge | `pool_name` | Running worker goroutines. A value stuck at the pool size means saturation |
| `wallet_pool_tasks_submitted_total` | counter | `pool_name` | Tasks submitted |
| `wallet_pool_tasks_waiting` | gauge | `pool_name` | Queue depth. Growth means workers fall behind |
| `wallet_pool_tasks_successful_total` | counter | `pool_name` | Tasks that completed |
| `wallet_pool_tasks_failed_total` | counter | `pool_name` | Tasks that panicked. Any rate above zero needs a look |
| `wallet_pool_tasks_dropped_total` | counter | `pool_name` | Tasks dropped because the queue was full |

### RPC

Two layers. The `request` series measure the raw JSON-RPC HTTP call, where `method` is the JSON-RPC name such as `getLedgers`. The `method_*` series measure the Go wrapper including parsing, where `method` is the Go name such as `GetLedgers`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_rpc_request_duration_seconds` | histogram | `method` | Round-trip time of one JSON-RPC HTTP call |
| `wallet_rpc_requests_total` | counter | `method`, `status` | JSON-RPC calls. `status` is `success` or `failure` |
| `wallet_rpc_in_flight_requests` | gauge | none | JSON-RPC calls in progress |
| `wallet_rpc_response_size_bytes` | histogram | `method` | Response body size |
| `wallet_rpc_service_health` | gauge | none | `1` if the last `getHealth` said healthy, `0` if not. Updated on every `getHealth` call |
| `wallet_rpc_latest_ledger` | gauge | none | Latest ledger from the last successful `getHealth`. Flat means the RPC node stalled |
| `wallet_rpc_method_calls_total` | counter | `method` | Go-level RPC method calls |
| `wallet_rpc_method_duration_seconds` | histogram | `method` | Go-level duration, including JSON parsing and validation |
| `wallet_rpc_method_errors_total` | counter | `method`, `error_type` | Go-level errors: `rpc_error`, `json_unmarshal_error`, `validation_error`, `not_found_error`, `xdr_decode_error` |

### Ingestion

Recorded on `ingest`. The lag gauge updates once per second in live mode only.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_ingestion_latest_ledger` | gauge | none | Latest ledger ingested |
| `wallet_ingestion_oldest_ledger` | gauge | none | Oldest ledger ingested |
| `wallet_ingestion_lag_ledgers` | gauge | none | Ledger backend tip minus the ingest position |
| `wallet_ingestion_duration_seconds` | histogram | none | Time to process and persist one ledger. Excludes the fetch |
| `wallet_ingestion_phase_duration_seconds` | histogram | `phase` | Time per live phase: `process_ledger`, `prepare_classification`, `insert_into_db` |
| `wallet_ingestion_ledger_fetch_duration_seconds` | histogram | none | Time to fetch one ledger, including retries and waiting at the tip. At the tip its floor is the ledger close interval, not I/O latency |
| `wallet_ingestion_ledgers_total` | counter | none | Ledgers ingested |
| `wallet_ingestion_transactions_total` | counter | none | Transactions ingested |
| `wallet_ingestion_operations_total` | counter | none | Operations ingested |
| `wallet_ingestion_participants_per_ledger` | histogram | none | Unique participant accounts per ingest batch |
| `wallet_ingestion_retries_total` | counter | `operation` | Single retry attempts: `ledger_fetch`, `db_persist`, `batch_flush`, `classification_read` |
| `wallet_ingestion_retry_exhaustions_total` | counter | `operation` | Every attempt failed and ingest gave up: `db_persist`, `batch_flush` |
| `wallet_ingestion_errors_total` | counter | `operation` | Ingest errors: `ingest_live`, `db_persist`, `batch_flush` |
| `wallet_ingestion_state_change_processing_duration_seconds` | histogram | `processor` | Time per state-change processor, for example `EffectsProcessor` or `TokenTransferProcessor` |
| `wallet_ingestion_state_changes_total` | counter | `type`, `category` | State changes written. `type` holds the state-change reason |
| `wallet_ingestion_protocol_state_processing_duration_seconds` | histogram | `protocol_id`, `phase` | Protocol state time per phase: `process_ledger`, `persist_history`, `persist_current_state` |
| `wallet_ingestion_wasm_classification_failures_total` | counter | `protocol_id`, `reason` | WASM classification errors. `protocol_id` is the validator tried, or `unknown`. `reason` is `spec_extraction_error` or `validate_error`. Re-run `protocol-setup` to recover |
| `wallet_ingestion_external_ref_contracts_total` | counter | none | Contract instances whose executable is a CAP-0085 external reference with no WASM hash. These stay unclassified |

### Protocol migration

Recorded by `protocol-migrate history` and `protocol-migrate current-state`. Loop series have no `protocol_id`, because each ledger is fetched once for all protocols.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wallet_migration_current_ledger` | gauge | none | Latest ledger the loop processed |
| `wallet_migration_ledgers_total` | counter | none | Ledgers processed. Its rate is ledgers per second |
| `wallet_migration_phase_duration_seconds` | histogram | `phase` | Time per stage: `fetch`, `extract`, `extract_contract_data`, `process`, `frontier_gate`, `flush`. Buckets reach 30 s for large window flushes |
| `wallet_migration_target_tip` | gauge | none | Chain tip from RPC `getHealth`, the target for progress. `0` when no RPC tip is available |
| `wallet_migration_cursor` | gauge | `protocol_id` | Committed cursor per protocol |
| `wallet_migration_start_ledger` | gauge | `protocol_id` | Cursor when the run started. `cursor` minus `start_ledger` is ledgers migrated |
| `wallet_migration_handoffs_total` | counter | `protocol_id` | Times the migration lost the cursor race to live ingest and handed off |
| `wallet_migration_status` | gauge | `protocol_id` | `0` in progress, `1` success, `2` failed |

## Suggested alerts

Thresholds are starting points. Tune them to your traffic.

| Alert | Expression | For |
| --- | --- | --- |
| Ingest lag above the health threshold | `wallet_ingestion_lag_ledgers > 50` | 5m |
| Ingest stalled | `increase(wallet_ingestion_ledgers_total[5m]) == 0` | 5m |
| Retries exhausted | `increase(wallet_ingestion_retry_exhaustions_total[1h]) > 0` | 0m |
| RPC unhealthy | `wallet_rpc_service_health == 0` | 2m |
| DB pool saturated | `wallet_db_pool_acquired_conns / wallet_db_pool_max_conns > 0.9` | 10m |
| HTTP 5xx rate | `sum(rate(wallet_http_requests_total{status_code=~"5.."}[5m])) / sum(rate(wallet_http_requests_total[5m])) > 0.05` | 10m |
| GraphQL p99 latency | `histogram_quantile(0.99, sum by (le) (rate(wallet_graphql_operation_duration_seconds_bucket[5m]))) > 5` | 10m |
| Expired JWT spike | `rate(wallet_auth_expired_signatures_total[5m]) > 1` | 10m |

## Logs

All processes log with logrus in text format to stderr. Every line carries `time`, `level`, `msg` and `pid`:

```text
time="2026-10-01T16:44:40.426-04:00" level=info msg="wallet-backend <VERSION> (commit <COMMIT>)" pid=70839
```

There is no JSON log option.

`LOG_LEVEL` accepts `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` or `PANIC`, in any case.

The default is `INFO`. The few lines the process prints while it parses its configuration come out at `TRACE` before the configured level applies.

`DEBUG` adds per-ledger and per-retry detail. Two examples from ingest:

```text
level=debug msg="upserted <N> trustlines, deleted <N> trustlines"
level=debug msg="simulate <CONTRACT_ID>.<FUNCTION> transient err (attempt <N>/<MAX>): <ERROR>"
```

`serve` logs two `INFO` lines for every HTTP request, including probes and scrapes:

| Message | Fields |
| --- | --- |
| `starting request` | `subsys=http`, `req`, `path`, `method`, `ip`, `host`, `useragent` |
| `finished request` | `subsys=http`, `req`, `path`, `method`, `ip`, `status`, `bytes`, `duration`, `route` |

`serve` also logs `graphql query complexity` at `INFO` for every GraphQL query, with `operation_name` and `complexity`. The `ingest` HTTP server does not log requests.

## pprof

Set `ADMIN_PORT` to a value above 0 on `serve` or `ingest` to serve Go pprof at `/debug/pprof/` on that port. On `serve`, a bind failure on the admin port is logged and the API keeps running. On `ingest`, a bind failure stops the process.

Capture a 30 s CPU profile:

```bash
go tool pprof http://<HOST>:<ADMIN_PORT>/debug/pprof/profile?seconds=30
```

## Error reporting

There is no external error tracker. Every 500 and 503 response, every recovered panic and every ingest error is logged at `error` level with its cause, so ship logs to the system you alert from.

## Where in the code

| Path | Role |
| --- | --- |
| `internal/metrics/` | Every `wallet_*` metric definition, plus the `go_*` and `process_*` collectors |
| `internal/serve/serve.go` | `serve` routes: `/health`, `/api-metrics`, GraphQL, admin pprof server |
| `internal/ingest/ingest.go` | `ingest` server: `/health`, `/ingest-metrics`, admin pprof server |
| `cmd/protocol_migrate.go` | `/metrics` server on `--metrics-port` |
| `internal/serve/httphandler/health.go` | Health rules and the 50-ledger threshold |
| `internal/serve/middleware/metrics_middleware.go` | HTTP labels `endpoint` and `method` |
| `internal/serve/middleware/graphql_utils.go` | GraphQL `operation_name` label source |
| `internal/services/ingest_live.go` | Ingest phases, retries, lag gauge |
| `internal/services/protocol_migrate.go` | Migration phases |
| `internal/utils/db_errors.go` | DB `error_type` values |
| `cmd/root.go` | Logger bootstrap and the startup version line |
| `cmd/utils/custom_set_value.go` | `LOG_LEVEL` parsing |
| `cmd/utils/global_options.go` | `ADMIN_PORT`, `INGEST_SERVER_PORT`, `LOG_LEVEL` |
| `internal/serve/httperror/errors.go` | Error response bodies; each 500 and 503 is logged at error level |
