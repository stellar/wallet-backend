# Ingest

Step 2 of [deploying wallet-backend](README.md). After it the live ingester follows the network tip and you know which ledger source to use.

## Contents

- [Start live ingestion](#start-live-ingestion)
- [Choose a ledger source](#choose-a-ledger-source)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](configuration.md).

## Start live ingestion

1. Set the minimum environment for testnet.

   ```bash
   DATABASE_URL=<DATABASE_URL>
   RPC_URL=<TESTNET_RPC_URL>
   NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
   ```

   `ARCHIVE_URL` defaults to the testnet archive. For pubnet, also set the values below. All presets are in [network presets](configuration.md#network-presets).

   ```bash
   RPC_URL=<PUBNET_RPC_URL>
   NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
   ARCHIVE_URL=https://history.stellar.org/prd/core-live/core_live_001/
   ```

2. Start the ingester.

   ```bash
   wallet-backend ingest
   ```

   On an empty database the first start loads the ledger-entry balances (native, classic, SAC, liquidity pool) from the archive's latest checkpoint before it ingests any ledger. SEP-41 balances are not in the archive; step 3, [Protocols](protocols.md), fills them. How long that takes depends on the archive download and the size of the network's ledger state. Watch for these log lines in order:

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

The path above is the public pubnet data lake. No testnet path is published in this repository. Ask your data-lake provider for one. Tuning variables are in [ledger source](configuration.md#ledger-source).

Next: [set up protocols](protocols.md).

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/ingest.go` | `ingest` options and mode validation |
| `internal/ingest/ingest.go` | Ingest wiring, `/health` and `/ingest-metrics` server |
| `internal/services/ingest_live.go` | Advisory lock, checkpoint bootstrap, live loop |
| `internal/services/checkpoint.go` | Balance bootstrap from the history archive |
| `internal/ingest/ledger_backend.go` | `rpc` and `datastore` ledger sources |
| `internal/serve/httphandler/health.go` | `/health` logic and the 50-ledger threshold |
