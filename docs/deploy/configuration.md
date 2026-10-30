# Configuration reference

For operators and developers who run `wallet-backend`. Lists every flag and environment variable for each command, with defaults and the checks each command runs at startup.

## Contents

- [Shared options](#shared-options)
- [serve](#serve)
- [ingest](#ingest)
  - [Mode and range](#mode-and-range)
  - [Ledger source](#ledger-source)
  - [History archive](#history-archive)
  - [Backfill tuning](#backfill-tuning)
  - [TimescaleDB policies](#timescaledb-policies)
- [migrate](#migrate)
- [protocol-setup](#protocol-setup)
- [protocol-migrate](#protocol-migrate)
- [version](#version)
- [Validation rules](#validation-rules)
- [Network presets](#network-presets)
- [Where in the code](#where-in-the-code)

Every option takes a flag or an environment variable. The variable name is the flag name in upper case with `-` replaced by `_`. When both are set, the flag wins. Options marked "no env var" are plain command-line flags.

```bash
wallet-backend serve --rpc-url <RPC_URL>
RPC_URL=<RPC_URL> wallet-backend serve
```

Required options reject an empty value. A required option with a default passes the check, so set it to your real value anyway.

## Shared options

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--database-url` | `DATABASE_URL` | `postgres://postgres@localhost:5432/wallet-backend?sslmode=disable` | yes | PostgreSQL connection URL |
| `--log-level` | `LOG_LEVEL` | `INFO` | no | One of `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`, `PANIC` |
| `--network-passphrase` | `NETWORK_PASSPHRASE` | `Test SDF Network ; September 2015` | yes | Stellar network passphrase |
| `--rpc-url` | `RPC_URL` | `http://localhost:8000` | yes | Stellar RPC URL |
| `--admin-port` | `ADMIN_PORT` | `0` | no | Port for pprof at `/debug/pprof`. `0` disables it |
| `--db-max-conns` | `DB_MAX_CONNS` | `12` | no | Maximum connections in the pool. Live ingest needs at least 7. Backfill needs at least 5 + `BACKFILL_COMPRESS_CONCURRENCY` + 1 |
| `--db-min-conns` | `DB_MIN_CONNS` | `5` | no | Minimum idle connections kept in the pool |
| `--db-max-conn-lifetime` | `DB_MAX_CONN_LIFETIME` | `5m` | no | Maximum connection lifetime, as a Go duration |
| `--db-max-conn-idle-time` | `DB_MAX_CONN_IDLE_TIME` | `10s` | no | Maximum connection idle time, as a Go duration |

Which commands take each shared option:

| Option | `serve` | `ingest` | `migrate` | `protocol-setup` | `protocol-migrate` |
| --- | :---: | :---: | :---: | :---: | :---: |
| `--database-url` | yes | yes | yes | yes | yes |
| `--log-level` | yes | yes | | yes | yes |
| `--network-passphrase` | yes | yes | | yes | yes |
| `--rpc-url` | yes | yes | | yes | own version |
| `--admin-port` | yes | yes | | | |
| `--db-*` pool options | yes | yes | | | |

`protocol-migrate` has its own `--rpc-url` with a different default and rule. See [protocol-migrate](#protocol-migrate).

## serve

Runs the GraphQL API server.

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--port` | `PORT` | `8001` | no | HTTP listen port |
| `--client-auth-public-keys` | `CLIENT_AUTH_PUBLIC_KEYS` | (none) | no | Comma-separated Stellar public keys allowed to sign requests. Empty disables auth |
| `--client-auth-max-timeout-seconds` | `CLIENT_AUTH_MAX_TIMEOUT_SECONDS` | `15` | yes | Maximum JWT lifetime accepted by client auth, in seconds |
| `--client-auth-max-body-size-bytes` | `CLIENT_AUTH_MAX_BODY_SIZE_BYTES` | `102400` | yes | Maximum request body size client auth reads, in bytes |
| `--hidden-contracts-file` | `HIDDEN_CONTRACTS_FILE` | (none) | no | YAML list of contracts the API does not return. Empty uses the list built into the binary. See [hiding tokens](hiding-tokens.md) |
| `--graphql-complexity-limit` | `GRAPHQL_COMPLEXITY_LIMIT` | `10000` | no | Maximum GraphQL query complexity |
| `--graphql-introspection-enabled` | `GRAPHQL_INTROSPECTION_ENABLED` | `false` | no | Serve `__schema` and `__type` introspection |

`serve` also takes all shared options.

## ingest

Reads ledgers and writes transactions, operations, state changes, and balances to the database. `ingest` also takes all shared options.

### Mode and range

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--ingestion-mode` | `INGESTION_MODE` | `live` | yes | `live` follows the chain tip. `backfill` fills every ledger from `--start-ledger` up to the history live ingestion already holds |
| `--start-ledger` | `START_LEDGER` | `0` | when backfill | First ledger the backfill fills, between 1 and 4294967294. Any explicit value, even `0`, is rejected in live mode |
| `--end-ledger` | `END_LEDGER` | `0` | no | Rejected in both modes when set, even to `0`. A backfill always ends at the history live ingestion holds |
| `--ingest-server-port` | `INGEST_SERVER_PORT` | `8002` | no | Port for `/health` and `/ingest-metrics` |

### Ledger source

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--ledger-backend-type` | `LEDGER_BACKEND_TYPE` | `rpc` | no | `rpc` reads ledgers from `--rpc-url`. `datastore` reads them from an S3 data lake |
| `--get-ledgers-limit` | `GET_LEDGERS_LIMIT` | `10` | no | Ledgers per RPC `getLedgers` call. Stay at `10` or below unless the RPC allows at least 5 s per call |
| `--datastore-bucket-path` | `DATASTORE_BUCKET_PATH` | (none) | when `datastore` | Bucket and path that hold the exported ledgers |
| `--datastore-region` | `DATASTORE_REGION` | `us-east-2` | no | AWS region of the bucket. Empty uses the AWS default chain |
| `--datastore-endpoint-url` | `DATASTORE_ENDPOINT_URL` | (none) | no | Custom S3 endpoint. Empty uses the AWS endpoint |
| `--datastore-buffer-size` | `DATASTORE_BUFFER_SIZE` | `100` | no | Ledger files to prefetch |
| `--datastore-num-workers` | `DATASTORE_NUM_WORKERS` | `10` | no | Concurrent file download workers |
| `--datastore-retry-limit` | `DATASTORE_RETRY_LIMIT` | `3` | no | Retries for a failed file download |
| `--datastore-retry-wait` | `DATASTORE_RETRY_WAIT` | `5s` | no | Wait between download retries, as a Go duration |
| `--datastore-ledgers-per-file` | `DATASTORE_LEDGERS_PER_FILE` | `0` | no | Ledgers per file. `0` reads it from the datastore manifest |
| `--datastore-files-per-partition` | `DATASTORE_FILES_PER_PARTITION` | `0` | no | Files per partition. `0` reads it from the datastore manifest |

`ingest` always needs `--rpc-url`, even with the `datastore` backend.

### Live persist

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--live-persist-max-batch-size` | `LIVE_PERSIST_MAX_BATCH_SIZE` | `1` | no | Consecutive ledgers coalesced into one commit set when live ingestion falls behind. `1` commits every ledger. The pipeline holds up to `2 × value + 1` ledger buffers in memory |

### History archive

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--archive-url` | `ARCHIVE_URL` | `https://history.stellar.org/prd/core-testnet/core_testnet_001/` | yes | History archive URL |
| `--checkpoint-frequency` | `CHECKPOINT_FREQUENCY` | `64` | no | Ledgers between archive checkpoints. Use `64` outside accelerated test networks |

### Backfill tuning

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--backfill-workers` | `BACKFILL_WORKERS` | `0` | no | Concurrent backfill workers. `0` uses one per CPU. Lower values use less RAM |
| `--backfill-flush-size` | `BACKFILL_FLUSH_SIZE` | `100` | no | Ledgers written per database flush. Each flush is one transaction per history table. Lower values use less RAM |
| `--backfill-compress-concurrency` | `BACKFILL_COMPRESS_CONCURRENCY` | `5` | no | Chunks compressed at once, each on its own database connection. The run keeps at most `ceil(value / 5) + 1` windows uncompressed on disk. Needs `DB_MAX_CONNS` of at least 5 + value + 1 |

Memory is about `(DATASTORE_BUFFER_SIZE + BACKFILL_WORKERS + BACKFILL_FLUSH_SIZE) × 30 MB` for a decoded mainnet ledger. Uncompressed windows take about `(ceil(BACKFILL_COMPRESS_CONCURRENCY / 5) + 1) × 15 GB` of disk on top of the compressed history. Both are estimates for mainnet at current density, and the run logs them at start. See [backfill](backfill.md#sizing-and-tuning).

### TimescaleDB policies

Only `--ingestion-mode=live` applies these. A backfill does not change them, except that it pauses every compression policy while it runs and schedules them again when it exits. It compresses the chunks it writes itself. Interval values use PostgreSQL `INTERVAL` syntax.

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--chunk-interval` | `CHUNK_INTERVAL` | `1 day` | no | Chunk time interval for hypertables. Applies to future chunks only |
| `--tx-hash-chunk-interval` | `TX_HASH_CHUNK_INTERVAL` | `7 days` | no | Chunk interval for the `transaction_hashes` lookup table. Applies to future chunks only. Keep a few million transactions per chunk: the table is segmented into 4096 buckets, and a chunk with fewer rows compresses into near-empty batches (8 bytes per row at 2M rows, 36 at 130k, 250 at 20k, measured). A hash lookup also checks every chunk, so longer is better, until the newest chunk's index (about 27 bytes per transaction) no longer fits in `shared_buffers` |
| `--retention-period` | `RETENTION_PERIOD` | (none) | no | Drop chunks older than this. Empty removes the retention policy |
| `--compression-schedule-interval` | `COMPRESSION_SCHEDULE_INTERVAL` | (none) | no | How often the compression job runs. Empty leaves it unchanged |
| `--compression-compress-after` | `COMPRESSION_COMPRESS_AFTER` | (none) | no | Age at which a closed chunk becomes eligible for compression. Empty leaves the other hypertables' policies unchanged and sets `transaction_hashes` to `1 day` |
| `--compression-max-chunks` | `COMPRESSION_MAX_CHUNKS` | `0` | no | Maximum chunks compressed per job run. `0` leaves it unchanged |

## migrate

Applies or reverts schema migrations. `migrate up` applies all pending migrations, then registers protocols. `migrate down <count>` reverts `<count>` migrations.

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--database-url` | `DATABASE_URL` | `postgres://postgres@localhost:5432/wallet-backend?sslmode=disable` | yes | PostgreSQL connection URL |

`migrate` takes no other options.

## protocol-setup

Fetches unclassified contract WASM bytecode over RPC and classifies it against the requested protocols. Takes `--database-url`, `--log-level`, `--network-passphrase`, and `--rpc-url` from the shared options.

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--protocol-id` | no env var | (none) | yes | Protocol to classify. Repeat the flag or pass a comma-separated list |

## protocol-migrate

Builds protocol data from ledgers. `protocol-migrate history` fills protocol history from the oldest ingested ledger to the tip. `protocol-migrate current-state` builds protocol current state from `--start-ledger` to the tip. Both take `--database-url`, `--log-level`, and `--network-passphrase` from the shared options, and every `--datastore-*` option from [Ledger source](#ledger-source) with the same env vars and defaults.

| Flag | Env var | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `--rpc-url` | `RPC_URL` | (none) | when `rpc` | Stellar RPC URL. Also used for contract metadata and the chain tip when set |
| `--ledger-backend-type` | `LEDGER_BACKEND_TYPE` | `rpc` | no | `rpc` or `datastore`. Use `datastore` for ranges older than the RPC retention window |
| `--get-ledgers-limit` | `GET_LEDGERS_LIMIT` | `10` | no | Ledgers per RPC request. Ignored for `datastore` |
| `--protocol-id` | no env var | (none) | yes | Protocol to migrate. Repeat the flag or pass a comma-separated list |
| `--window-size` | no env var | `100` | no | Ledgers per commit. `0` or `1` commits every ledger |
| `--metrics-port` | no env var | `0` | no | Port for Prometheus `/metrics`. `0` disables it |
| `--rebuild` | no env var | `false` | no | Delete the protocol's rows and rebuild them. Destructive: queries return partial data until the rebuild reaches the live tip |
| `--start-ledger` | no env var | `0` | yes, `current-state` only | First ledger for `current-state`. `history` does not take it |

## version

Prints the version, build commit, and Go version. Takes no options.

```bash
wallet-backend version
```

## Validation rules

Each command checks these at startup and exits with the quoted error.

- **Required option empty.** Any option marked required fails with `Invalid config: <name> is blank. Please specify --<name> on the command line or set the <ENV_VAR> environment variable.`
- **Unknown ingestion mode.** `ingest` rejects a mode other than `live` or `backfill` with `invalid ingestion-mode '<value>', must be 'live' or 'backfill'`.
- **Range in live mode.** `ingest` in live mode rejects an explicitly set start or end ledger, including `0`, whether it came as a flag or an environment variable: `--start-ledger and --end-ledger apply to --ingestion-mode=backfill only; live mode resumes from the stored cursor`.
- **Bad backfill start.** `ingest` in backfill mode needs a start between 1 and 4294967294: `--ingestion-mode=backfill needs --start-ledger between 1 and 4294967294 (got <value>)`.
- **End ledger in backfill mode.** Backfill rejects an explicitly set end ledger: `--end-ledger does not apply to backfill: the run fills every ledger from --start-ledger up to the live ingester's history`.
- **Bad backfill tuning.** Backfill needs positive `--backfill-flush-size` and `--backfill-compress-concurrency`, and `--backfill-workers` of 0 or more: `--backfill-flush-size must be positive (got <value>)`, `--backfill-compress-concurrency must be positive (got <value>)`, `--backfill-workers must be 0 (one per CPU) or positive (got <value>)`.
- **Pool too small for backfill.** `ingest` in backfill mode needs `DB_MAX_CONNS` of at least 5 + `--backfill-compress-concurrency` + 1: `db-max-conns is <n>, below the <m> connections a backfill with compress concurrency <k> requires`.
- **Pool too small for live mode.** `ingest` in live mode fails with `db-max-conns is <n>, below the 7 connections live persist requires`.
- **Unknown backend in ingest.** `ingest` fails with `invalid ledger-backend-type '<value>', must be 'rpc' or 'datastore'`.
- **Datastore without bucket in ingest.** `ingest` fails with `--datastore-bucket-path (DATASTORE_BUCKET_PATH) is required when --ledger-backend-type=datastore`.
- **No protocol given.** `protocol-setup` and `protocol-migrate` fail with `at least one --protocol-id is required`.
- **RPC backend without URL.** `protocol-migrate` fails with `--rpc-url is required when --ledger-backend-type=rpc`.
- **Datastore without bucket in protocol-migrate.** `protocol-migrate` fails with `--datastore-bucket-path is required when --ledger-backend-type=datastore`.
- **Unknown backend in protocol-migrate.** `protocol-migrate` fails with `invalid --ledger-backend-type "<value>", must be 'rpc' or 'datastore'`.
- **Missing start ledger.** `protocol-migrate current-state` fails with `--start-ledger is required and must be > 0`.
- **Bad duration.** The `--db-max-conn-*` and `--datastore-retry-wait` options fail with `<name> cannot be empty` or `couldn't parse duration in <name>: <cause>`.
- **Bad log level.** `--log-level` fails with `couldn't parse log level in log-level: <cause>`.
- **Bad auth key.** `serve` rejects an invalid Stellar address in `--client-auth-public-keys` with `validating public key "<key>" in client-auth-public-keys: <cause>`.
- **Bad hidden-contracts list.** `serve` refuses a missing file, unknown fields, a bad contract address, an empty reason, or an unknown protocol. The errors are listed in [hiding tokens](hiding-tokens.md#startup-errors).
- **Bad migrate count.** `migrate down` needs exactly one integer argument and fails otherwise with `invalid [count] argument: <value>`.

## Network presets

| Env var | Testnet | Pubnet |
| --- | --- | --- |
| `NETWORK_PASSPHRASE` | `Test SDF Network ; September 2015` | `Public Global Stellar Network ; September 2015` |
| `ARCHIVE_URL` | `https://history.stellar.org/prd/core-testnet/core_testnet_001/` | `https://history.stellar.org/prd/core-live/core_live_001/` |
| `DATASTORE_BUCKET_PATH` | `<ask your data-lake provider>` | `aws-public-blockchain/v1.1/stellar/ledgers/pubnet` |
| `RPC_URL` | `<TESTNET_RPC_URL>` | `<PUBNET_RPC_URL>` |

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/utils/global_options.go` | Shared options, pool options, and datastore options |
| `cmd/serve.go` | `serve` options |
| `cmd/ingest.go` | `ingest` options and mode, range, and backend checks |
| `cmd/migrate.go` | `migrate up` and `migrate down` |
| `cmd/protocol_setup.go` | `protocol-setup` options and the protocol ID check |
| `cmd/protocol_migrate.go` | `protocol-migrate` options and backend checks |
| `cmd/version.go` | `version` output |
| `cmd/utils/custom_set_value.go` | Parsing for log level, durations, and auth keys |
| `internal/ingest/timescaledb.go` | How live ingest applies the TimescaleDB policy options |
| `.env.example` | Environment template with every option |
