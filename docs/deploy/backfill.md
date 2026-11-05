# Backfill

Step 5 of [deploying wallet-backend](README.md), optional. After it the database holds history older than the ledger live ingestion started from.

## Contents

- [Backfill history](#backfill-history)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](configuration.md).

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

   Tuning lives in [backfill tuning](configuration.md#backfill-tuning). `BACKFILL_WORKERS` defaults to one per CPU. Lower it, or the batch sizes, to cut memory use.

3. Verify. Each batch logs `Batch <I>/<N> [<START> - <END>] completed`, and the run ends with `Backfilling completed in <DURATION>: <N> batches`. `wallet_ingestion_oldest_ledger` on the live ingester's `/ingest-metrics` drops toward `START_LEDGER`.

Measure the backfill rate on your hardware. No backfill rate has been measured yet. As a floor: live pubnet ingest on 2026-10-01 averaged 0.16 s per ledger (p99 0.44 s) with 4 CPU / 8 GiB for the ingest process, against PostgreSQL 17.6 and TimescaleDB 2.28.2 on 8 CPU / 32 GiB hosts.

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/ingest.go` | `ingest` options and mode validation |
| `internal/services/ingest_backfill.go` | Gap detection and parallel backfill |
| `internal/ingest/ledger_backend.go` | `rpc` and `datastore` ledger sources |
