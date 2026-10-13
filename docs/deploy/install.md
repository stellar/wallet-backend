# Install

Step 1 of [deploying wallet-backend](README.md). After it you have a database with the schema applied and know what each process needs.

## Contents

- [Requirements](#requirements)
- [Create the database](#create-the-database)
- [Run multiple networks](#run-multiple-networks)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](configuration.md).

## Requirements

| Component | Version | Notes |
| --- | --- | --- |
| PostgreSQL | 17 | One database per network |
| TimescaleDB | 2.28.x | The extension must exist in the database before `migrate up`. Migrations never run `CREATE EXTENSION` |
| `timescaledb.enable_chunk_skipping` | `on` | Server setting. Migrations call `enable_chunk_skipping()` on the hypertables, which TimescaleDB refuses while the setting is off, so `migrate up` fails |
| `timescaledb.enable_sparse_index_bloom` | `on` | Server setting. The hypertable migrations declare `bloom(...)` sparse indexes, which TimescaleDB only accepts with this setting on |
| stellar-rpc | 28.x, same network | Used for `getHealth`, `getLedgers`, `getLedgerEntries`, `simulateTransaction` |
| History archive | any | `ARCHIVE_URL`. First start reads balances from the latest checkpoint |
| S3 data lake | optional | Only for `LEDGER_BACKEND_TYPE=datastore`. See [Choose a ledger source](ingest.md#choose-a-ledger-source) |
| Hardware | see [runbook](runbook.md#hardware) | Minimum and recommended CPU, memory and disk per component, for pubnet and testnet |
| wallet-backend | release image or source build | Image `public.ecr.aws/stellar/wallet-backend:<VERSION>` (also on Docker Hub as `stellar/wallet-backend:<VERSION>`) with entrypoint `wallet-backend`, or `make build` with Go 1.25 (from `go.mod`) |

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

## Run multiple networks

| Item | Rule |
| --- | --- |
| Database | One per network. Tables have no network column, so two networks in one database mix their data |
| Live ingester | One per network, pointed at that network's database |
| Lock | The live-ingest lock key is derived from `NETWORK_PASSPHRASE`, and advisory locks are per database |
| API | One per network, or more behind a load balancer. The API takes no lock |
| Schemas | Not supported. The code never sets `search_path`; use separate databases |

Next: [start live ingestion](ingest.md).

## Where in the code

| Path | Role |
| --- | --- |
| `internal/db/migrate.go` | `migrate up` and protocol registration |
| `docker-compose.yaml` | Reference PostgreSQL settings |
