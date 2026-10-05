# Running wallet-backend

For an operator standing up wallet-backend outside the compose quickstart. After reading it you can prepare the database, start live ingestion and the API, backfill history, and turn on authentication.

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](../configuration.md).

## Requirements

| Component | Version | Notes |
| --- | --- | --- |
| PostgreSQL | 17 | One database per network |
| TimescaleDB | 2.28.x | The extension must exist in the database before `migrate up`. Migrations never run `CREATE EXTENSION` |
| `timescaledb.enable_chunk_skipping` | `on` | Server setting. Migrations call `enable_chunk_skipping()` on the hypertables, which TimescaleDB refuses while the setting is off, so `migrate up` fails |
| `timescaledb.enable_sparse_index_bloom` | `on` | Server setting. The hypertable migrations declare `bloom(...)` sparse indexes, which TimescaleDB only accepts with this setting on |
| stellar-rpc | 28.x, same network | Used for `getHealth`, `getLedgers`, `getLedgerEntries`, `simulateTransaction` |
| History archive | any | `ARCHIVE_URL`. First start reads balances from the latest checkpoint |
| S3 data lake | optional | Only for `LEDGER_BACKEND_TYPE=datastore`. See [Choose a ledger source](#choose-a-ledger-source) |
| wallet-backend | release image or source build | Image `public.ecr.aws/stellar/wallet-backend:<VERSION>` with entrypoint `wallet-backend`, or `make build` with Go 1.25 (from `go.mod`) |

Commands below call the binary as `wallet-backend`. With the image, run the same arguments through `docker run`:

```bash
docker run --rm -e DATABASE_URL=<DATABASE_URL> public.ecr.aws/stellar/wallet-backend:<VERSION> migrate up
```

## Create the database

1. Start PostgreSQL with both TimescaleDB settings on, then create the database and the extension.

   ```bash
   postgres -c timescaledb.enable_chunk_skipping=on -c timescaledb.enable_sparse_index_bloom=on
   ```

   ```sql
   CREATE DATABASE "wallet-backend";
   \c wallet-backend
   CREATE EXTENSION IF NOT EXISTS timescaledb;
   ```

   On a managed server, set the two values in `postgresql.conf` or the provider's parameter group instead of the command line.

   Verify: `SHOW timescaledb.enable_chunk_skipping;` and `SHOW timescaledb.enable_sparse_index_bloom;` both return `on`.

2. Apply the schema.

   ```bash
   DATABASE_URL=postgres://<USER>:<PASSWORD>@<HOST>:5432/wallet-backend?sslmode=require \
     wallet-backend migrate up
   ```

   Expect `Successfully applied <N> migrations up.` on a fresh database and `No migrations applied.` on a current one. The same command registers the built-in protocols.

   Verify:

   ```sql
   SELECT count(*) FROM gorp_migrations;
   \dx timescaledb
   ```

   The count matches the number of migrations in the release, and `\dx` lists `timescaledb` at 2.28.x.

Nothing runs migrations automatically. Run `migrate up` before the first start and after every upgrade.

## Start live ingestion

1. Set the minimum environment for testnet.

   ```bash
   DATABASE_URL=<DATABASE_URL>
   RPC_URL=<TESTNET_RPC_URL>
   NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
   ```

   `ARCHIVE_URL` defaults to the testnet archive. For pubnet, also set the values below. All presets are in [network presets](../configuration.md#network-presets).

   ```bash
   RPC_URL=<PUBNET_RPC_URL>
   NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
   ARCHIVE_URL=https://history.stellar.org/prd/core-live/core_live_001/
   ```

2. Start the ingester.

   ```bash
   wallet-backend ingest
   ```

   On an empty database the first start reads every balance from the archive's latest checkpoint before it ingests any ledger. How long that takes depends on the archive download and the size of the network's ledger state. Watch for these log lines in order:

   ```text
   Populating from checkpoint ledger = <LEDGER>
   Processed <N> entries (<N> trustlines, <N> accounts in <N> batches) in <MINUTES> minutes
   Starting ingestion from ledger: <LEDGER>
   Ingested ledger <LEDGER> in <SECONDS>s
   ```

   Later starts resume from the stored cursor and skip the checkpoint.

3. Verify.

   ```bash
   curl -s localhost:8002/health
   curl -s localhost:8002/ingest-metrics | grep '^wallet_ingestion_latest_ledger'
   ```

   `/health` returns `{"status":"ok","backend_latest_ledger":<LEDGER>}` once ingest is within 50 ledgers of the RPC. It returns 503 during the checkpoint bootstrap. `wallet_ingestion_latest_ledger` rises by one every ledger close. The port is `INGEST_SERVER_PORT`.

Run one live ingester per network. It holds a PostgreSQL advisory lock keyed on `NETWORK_PASSPHRASE` for its whole life. A second live ingester on the same database exits with:

```text
Error: running ingest: running 'ingest' from 0 to 0: advisory lock not acquired
```

## Choose a ledger source

`LEDGER_BACKEND_TYPE` picks where ingest reads ledgers.

| | `rpc` (default) | `datastore` |
| --- | --- | --- |
| Reads from | stellar-rpc `getLedgers` at `RPC_URL` | S3 data lake at `DATASTORE_BUCKET_PATH`, written by Galexie |
| History available | the RPC's retention window only | everything in the bucket |
| Throughput | one RPC request per `GET_LEDGERS_LIMIT` ledgers | `DATASTORE_NUM_WORKERS` parallel downloads with a prefetch buffer |
| Still needs `RPC_URL` | yes | yes, for `/health` and contract metadata |
| Cost | the RPC node you already run | S3 request and transfer charges for reading the bucket |
| Use for | live ingestion | backfill and protocol migrations of old ranges |

Datastore settings:

```bash
LEDGER_BACKEND_TYPE=datastore
DATASTORE_BUCKET_PATH=aws-public-blockchain/v1.1/stellar/ledgers/pubnet
DATASTORE_REGION=us-east-2
```

The path above is the public pubnet data lake. No testnet path is published in this repository. Ask your data-lake provider for one. Tuning variables are in [ledger source](../configuration.md#ledger-source).

## Start the API

1. Set the environment.

   ```bash
   DATABASE_URL=<DATABASE_URL>
   RPC_URL=<RPC_URL>
   NETWORK_PASSPHRASE=<NETWORK_PASSPHRASE>
   PORT=8001
   CLIENT_AUTH_PUBLIC_KEYS=<G_PUBLIC_KEY>
   ```

   Leave `CLIENT_AUTH_PUBLIC_KEYS` unset to run without authentication. See [Enable authentication](#enable-authentication).

2. Start the server.

   ```bash
   wallet-backend serve
   ```

3. Verify with a real query. Introspection is off unless `GRAPHQL_INTROSPECTION_ENABLED=true`.

   ```graphql
   query Account($address: String!) {
     accountByAddress(address: $address) {
       address
     }
   }
   ```

   ```json
   { "address": "<G_ADDRESS>" }
   ```

   ```bash
   curl -s localhost:8001/graphql/query \
     -H 'Content-Type: application/json' \
     -d '{"query":"query Account($address: String!) { accountByAddress(address: $address) { address } }","variables":{"address":"<G_ADDRESS>"}}'
   ```

   ```json
   { "data": { "accountByAddress": { "address": "<G_ADDRESS>" } } }
   ```

   With authentication on, add `-H 'Authorization: Bearer <JWT>'`. Without it the server returns 401.

`GET /health` on the API port runs the same check as the ingester: 500 when the RPC cannot be reached, 503 when the RPC reports itself unhealthy or when ingest is more than 50 ledgers behind the RPC. Point load-balancer health checks at it only if a lagging ingester should take the API out of rotation.

## Backfill history

Live ingestion starts at the checkpoint ledger it bootstrapped from. To serve older history, backfill the range below it.

1. Read the current range.

   ```sql
   SELECT key, value FROM ingest_store WHERE key IN ('oldest_ingest_ledger', 'latest_ingest_ledger');
   ```

2. Run a backfill alongside the live ingester. It takes no lock. `END_LEDGER` must be at or below `latest_ingest_ledger`.

   ```bash
   INGESTION_MODE=backfill \
   START_LEDGER=<FIRST_LEDGER> \
   END_LEDGER=<OLDEST_INGEST_LEDGER_MINUS_1> \
   LEDGER_BACKEND_TYPE=datastore \
   DATASTORE_BUCKET_PATH=<BUCKET_PATH> \
   INGEST_SERVER_PORT=<FREE_PORT> \
     wallet-backend ingest
   ```

   Use the datastore source for any range outside the RPC's retention window. Give the backfill its own `INGEST_SERVER_PORT` when it shares a host with the live ingester. Backfill only fills gaps: ledgers already in the database are skipped. It leaves retention and compression policies to the live ingester.

   Tuning lives in [backfill tuning](../configuration.md#backfill-tuning). `BACKFILL_WORKERS` defaults to one per CPU. Lower it, or the batch sizes, to cut memory use.

3. Verify. Each batch logs `Batch <I>/<N> [<START> - <END>] completed`, and the run ends with `Backfilling completed in <DURATION>: <N> batches`. `wallet_ingestion_oldest_ledger` on the live ingester's `/ingest-metrics` drops toward `START_LEDGER`.

Measure the backfill rate on your hardware. No backfill rate has been measured yet. As a floor: live pubnet ingest on 2026-10-01 averaged 0.16 s per ledger (p99 0.44 s) on a 4 CPU / 8 GiB pod against PostgreSQL 17.6 and TimescaleDB 2.28.2 on 8 CPU / 32 GiB nodes.

## Enable authentication

1. Generate a Stellar keypair for each client. With the Stellar CLI:

   ```bash
   stellar keys generate <CLIENT_NAME>
   stellar keys address <CLIENT_NAME>
   ```

   Any Stellar SDK works too. The client keeps the secret key. The server gets only the `G...` public key.

2. Set the public keys on the API and restart it.

   ```bash
   CLIENT_AUTH_PUBLIC_KEYS=<G_KEY_1>,<G_KEY_2>
   ```

3. Verify: an unsigned request to `/graphql/query` returns 401. `/health` and `/api-metrics` stay open.

Token format and signing steps are in [request authentication](../api/authentication.md).

## Run multiple networks

| Item | Rule |
| --- | --- |
| Database | One per network. Tables have no network column, so two networks in one database mix their data |
| Live ingester | One per network, pointed at that network's database |
| Lock | The live-ingest lock key is derived from `NETWORK_PASSPHRASE`, and advisory locks are per database |
| API | One per network, or more behind a load balancer. The API takes no lock |
| Schemas | Not supported. The code never sets `search_path`; use separate databases |

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/ingest.go` | `ingest` options and mode validation |
| `internal/ingest/ingest.go` | Ingest wiring, `/health` and `/ingest-metrics` server |
| `internal/services/ingest_live.go` | Advisory lock, checkpoint bootstrap, live loop |
| `internal/services/checkpoint.go` | Balance bootstrap from the history archive |
| `internal/services/ingest_backfill.go` | Gap detection and parallel backfill |
| `internal/ingest/ledger_backend.go` | `rpc` and `datastore` ledger sources |
| `internal/serve/serve.go` | API routes and GraphQL server |
| `internal/serve/httphandler/health.go` | `/health` logic and the 50-ledger threshold |
| `internal/db/migrate.go` | `migrate up` and protocol registration |
| `docker-compose.yaml` | Reference PostgreSQL settings |
