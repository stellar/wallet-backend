# Data migrations

For an operator upgrading to a release that changes what wallet-backend writes for history rows. After it, rows ingested by an earlier release hold the same values the release writes for new ledgers.

## Contents

- [What a data migration is](#what-a-data-migration-is)
- [Which command fixes which data](#which-command-fixes-which-data)
- [Order of operations](#order-of-operations)
- [data-migrate run](#data-migrate-run)
- [data-migrate status](#data-migrate-status)
- [Monitoring](#monitoring)
- [Resuming and failures](#resuming-and-failures)
- [Troubleshooting](#troubleshooting)
- [Where in the code](#where-in-the-code)

## What a data migration is

A release can add a column to a history table or correct a value the indexer wrote. Live ingestion writes the new form from the ledger it resumes at. A data migration rewrites the rows below that ledger. It re-reads each ledger from a ledger source and updates the rows in place.

| Term | Meaning |
| --- | --- |
| Boundary ledger | The first ledger the upgraded ingester writes. Rows from it on are already correct. The live ingester records it the first time it starts with a binary that knows the migration |
| Cursor | The lowest ledger rewritten so far. It starts at the boundary and walks down to `oldest_ingest_ledger` |
| Status | `pending`, `in_progress`, or `done` |

On a database that a binary bootstrapped from a checkpoint, that binary records every data migration it knows as `done`. No older rows exist to rewrite.

The data migrations this release knows:

| Name | Rewrites | Until it reaches a row |
| --- | --- | --- |
| `transactions-memo` | `memo_type` and `memo` on `transactions`. See [Transaction memos](upgrading.md#transaction-memos) | `Transaction.memoType` and `Transaction.memo` are `null`, and a memo filter does not match the transaction |

## Which command fixes which data

A data migration rewrites each ledger from that ledger alone. Data that depends on earlier ledgers needs a different command.

| Data | Fixed by |
| --- | --- |
| History rows: `transactions`, `operations`, `state_changes`, `transactions_accounts`, `operations_accounts` | `data-migrate run <name>` |
| Protocol current state that is a running total: SEP-41 balances and allowances, Blend positions, pools, and claimed totals | `protocol-migrate current-state --rebuild`. It truncates the tables, then replays from `--start-ledger`, so the API serves empty or partial data for them until it finishes. See [protocols](protocols.md#protocol-migrate-current-state) |
| Protocol state changes | `protocol-migrate history --rebuild`. It deletes the protocol's rows, then migrates again, so the API serves partial history for the protocol until it finishes. See [protocols](protocols.md#protocol-migrate-history) |
| Classic balance tables: `native_balances`, `trustline_balances`, `sac_balances`, `liquidity_pools`, `liquidity_pool_balances` | No command. These tables copy ledger entries, so a reload from a history archive checkpoint could rebuild them, but wallet-backend has no command for it |

## Order of operations

1. Apply migrations with the release binary.

   ```bash
   DATABASE_URL=<DATABASE_URL> wallet-backend migrate up
   ```

2. Restart the live ingester on the release binary. On start it records each data migration it knows, with the boundary set to `latest_ingest_ledger + 1`.

   Verify: `data-migrate status` shows the migration as `pending` with a `BOUNDARY`.

3. Run the data migration as a one-off process.

   ```bash
   DATABASE_URL=<DATABASE_URL> NETWORK_PASSPHRASE=<NETWORK_PASSPHRASE> \
     LEDGER_BACKEND_TYPE=datastore DATASTORE_BUCKET_PATH=<DATASTORE_BUCKET_PATH> \
     wallet-backend data-migrate run transactions-memo
   ```

   Verify: the process exits with status 0 and `data-migrate status` shows `done`.

Ingestion and the API keep running through all three steps. Rows the walk has not reached yet keep their old values. Running step 3 before step 2 fails with `data migration is not recorded yet: start the upgraded ingester first, then run it again`.

## data-migrate run

Rewrites one named migration for every ledger below its boundary. Pass exactly one name. The command takes no tuning flags.

| Setting | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL`, `NETWORK_PASSPHRASE`, `LOG_LEVEL` | | Same as `ingest` |
| `LEDGER_BACKEND_TYPE` | `rpc` | Use `datastore` when retained history is older than the RPC retention window |
| `RPC_URL` | none | Required with `rpc` |
| `DATASTORE_*` | | Same as `ingest`. `DATASTORE_BUCKET_PATH` is required with `datastore`. See [ledger source](configuration.md#ledger-source) |
| `GET_LEDGERS_LIMIT` | `10` | RPC backend only |
| `--metrics-port` | `0` | Port for `/metrics`. `0` turns it off. No environment variable |

A run goes through these steps:

| Step | Behavior |
| --- | --- |
| Lock | Takes an advisory lock shared by all data migrations. Only one data migration runs at a time per database. A second `run`, of any name, exits at once with `another data migration is running` |
| Done check | A migration with status `done` logs `data migration <NAME> is already done` and exits with status 0 |
| Source check | Reads `oldest_ingest_ledger` from the ledger source before it writes anything. If the source cannot serve it, the run exits. See [Troubleshooting](#troubleshooting) |
| Walk | Newest first, from the cursor down to `oldest_ingest_ledger`, in slices of 10,000 ledgers. After each slice it reads `oldest_ingest_ledger` again, so retention or a backfill that moves it during the run is followed |
| Commit | Each slice moves the cursor in the same transaction as its last write |
| Compressed chunks | Rows in compressed chunks are updated in place. Each chunk compressed before the run is compressed again once the walk has passed it. Expect extra disk I/O and some temporary extra disk use while it runs |
| Finish | Sets the status to `done` |

Live ingestion writes only ledgers from the boundary on, and the walk writes only ledgers below it. The two never write the same rows.

The run logs:

```text
data migration <NAME>: rewriting ledgers [<OLDEST>, <HIGH>] newest-first, <N> compressed chunks to recompress
data migration <NAME>: ledgers [<LO>, <HI>] done in <DURATION>
data migration <NAME>: done down to ledger <OLDEST>
```

## data-migrate status

Prints one line per data migration the binary knows. It takes only `DATABASE_URL`.

```bash
DATABASE_URL=<DATABASE_URL> wallet-backend data-migrate status
```

```text
NAME               STATUS       BOUNDARY    CURSOR    FLOOR     STARTED                 FINISHED  LAST ERROR
transactions-memo  in_progress  <BOUNDARY>  <CURSOR>  <FLOOR>   <YYYY-MM-DDTHH:MM:SSZ>  -         -
```

| Column | Meaning |
| --- | --- |
| `NAME` | The migration name, as passed to `run` |
| `STATUS` | `not recorded` until the upgraded live ingester starts, then `pending`, `in_progress`, or `done` |
| `BOUNDARY` | First ledger the upgraded ingester wrote |
| `CURSOR` | Lowest ledger rewritten so far. `CURSOR` minus `FLOOR` is the ledgers left |
| `FLOOR` | Current `oldest_ingest_ledger`, where the walk ends |
| `STARTED` | When the first `run` started, in UTC. A resumed run keeps it |
| `FINISHED` | When the status became `done`, in UTC |
| `LAST ERROR` | Why the last run stopped. A new run clears it |

`-` marks an empty value. The status is for operators only. Nothing else in wallet-backend reads it, and the API serves rows whatever it says.

## Monitoring

Start the run with `--metrics-port <PORT>` and scrape `http://<HOST>:<PORT>/metrics`.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `wallet_data_migration_cursor` | `name` | Lowest ledger rewritten. It falls toward `wallet_ingestion_oldest_ledger` |
| `wallet_data_migration_errors_total` | `name` | Failed slice attempts |

Without metrics, follow the log or run `data-migrate status`.

## Resuming and failures

Each slice is tried 3 times, 10 seconds apart. Each failed attempt logs `data migration <NAME>: ledgers [<LO>, <HI>], attempt <N> of 3: <ERROR>`. After the third failure the run stores the error in `last_error` and exits with a non-zero status. The status stays `in_progress`.

To resume, fix the cause and run the same command again. The run starts from the cursor. A run stopped by SIGTERM or a crash resumes the same way and redoes at most one slice. Rewriting a ledger twice gives the same rows.

`done` is final. Running a `done` migration again does nothing. When a later release fixes the same data again, it ships a migration with a new name.

## Troubleshooting

| Message | Cause | Fix |
| --- | --- | --- |
| `data migration is not recorded yet: start the upgraded ingester first, then run it again` | The live ingester has not started on the release binary | Start it, check `data-migrate status` shows `pending`, then run again |
| `another data migration is running` | Another `data-migrate run` holds the lock | Wait for it, or stop it and run again |
| `unknown data migration "<NAME>", known: <NAMES>` | A typo, or a binary older than the migration | Use a name from `data-migrate status` |
| `the ledger backend cannot serve the oldest ingested ledger <N> (history older than the RPC retention window needs --ledger-backend-type=datastore)` | The ledger is outside the RPC retention window | Run with `LEDGER_BACKEND_TYPE=datastore` and `DATASTORE_BUCKET_PATH` set |
| `ledgers [<LO>, <HI>] failed 3 times: <ERROR>` | `<ERROR>` names the cause, often the ledger source | Fix the cause and run again |
| `interrupted at ledgers [<LO>, <HI>]` | The process got SIGTERM or an interrupt | Run again |
| `--rpc-url is required when --ledger-backend-type=rpc` | `RPC_URL` is not set | Set it, or use the `datastore` backend |

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/data_migrate.go` | `data-migrate run` and `data-migrate status` options and output |
| `internal/datamigrations/runner.go` | Lock, source check, walk, retries, recompression |
| `internal/datamigrations/migration.go` | `Migration` interface and the list of known migrations |
| `internal/datamigrations/transactions_memo.go` | `transactions-memo` |
| `internal/data/data_migrations.go` | `data_migrations` reads and writes |
| `internal/services/ingest_live.go` | Records each migration's boundary on start |
| `internal/metrics/data_migration.go` | Data migration metrics |
