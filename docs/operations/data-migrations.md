# Protocol data migrations

For an operator whose database holds ledgers ingested before a protocol was registered. After reading it you can classify that protocol's contracts and fill its history and current state for the ledgers already in the database.

## Contents

- [When you need one](#when-you-need-one)
- [Order of operations](#order-of-operations)
- [protocol-setup](#protocol-setup)
- [protocol-migrate history](#protocol-migrate-history)
- [protocol-migrate current-state](#protocol-migrate-current-state)
- [Monitoring](#monitoring)
- [Resuming and failures](#resuming-and-failures)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. The flags `--protocol-id`, `--window-size`, `--metrics-port`, `--rebuild`, and the `--start-ledger` of `protocol-migrate current-state` have no environment variable. Pass them on the command line.

## When you need one

Live ingestion produces a protocol's data only from the moment that protocol's migration cursors exist. Ledgers ingested before then hold no rows for it. A data migration fills them.

| You have | You run |
| --- | --- |
| A fresh database, protocol present from the start | `protocol-setup`, then both migrations, so live ingestion starts producing the protocol |
| A release that adds a protocol to a populated database | Every step in [Order of operations](#order-of-operations) |
| Wrong data for a protocol that already migrated | The matching migration with `--rebuild` |

The only registered protocol is `SEP41`.

## Order of operations

1. Apply migrations with the release binary. `migrate up` also inserts the protocol's row into `protocols`.

   ```bash
   DATABASE_URL=<DATABASE_URL> wallet-backend migrate up
   ```

   Verify: `SELECT id, classification_status FROM protocols;` lists the protocol as `not_started`.

2. Restart live ingestion on the release binary. It then classifies contracts deployed from this point on. Leave it running: a migration finishes only by handing off to live ingestion.

   Verify: the log shows `protocol <ID> history production disabled; cursor not initialized` and the same line for current-state.

3. Classify the contracts already on chain.

   ```bash
   DATABASE_URL=<DATABASE_URL> RPC_URL=<RPC_URL> NETWORK_PASSPHRASE=<NETWORK_PASSPHRASE> \
     wallet-backend protocol-setup --protocol-id SEP41
   ```

   Verify: `classification_status` is `success`.

4. Run one or both migrations. They take separate locks, so they can run at the same time.

   ```bash
   wallet-backend protocol-migrate current-state --protocol-id SEP41 --start-ledger <LEDGER>
   wallet-backend protocol-migrate history --protocol-id SEP41
   ```

   Verify: `history_migration_status` and `current_state_migration_status` reach `success`, and live ingestion logs `protocol <ID> history cursor initialized; production enabled`. Live ingestion re-checks for cursors every 100 ledgers, so it needs no restart.

## protocol-setup

Classifies contract WASMs for the given protocols. It reads WASM hashes that `protocol_wasms` holds without a protocol, fetches their bytecode from the RPC with `getLedgerEntries`, and runs each protocol's validator over them. Matches are stamped with the protocol ID, and validator side writes, such as SEP-41 token metadata in `contract_tokens`, commit in the same transaction. Hashes the RPC cannot return, because their entries expired or were evicted, are skipped.

| Setting | Required | Notes |
| --- | --- | --- |
| `DATABASE_URL` | yes | |
| `RPC_URL` | yes | Must serve the network named by `NETWORK_PASSPHRASE` |
| `NETWORK_PASSPHRASE` | yes | |
| `LOG_LEVEL` | no | Default `INFO` |
| `--protocol-id` | yes | Repeat the flag, or comma-separate, for several protocols |

It sets `classification_status` to `in_progress`, then to `success` or `failed`. It ends with `Protocol setup completed successfully for protocols: [<ID>]`. Rerunning it is safe: it only looks at unclassified hashes.

## protocol-migrate history

Writes the protocol's state-change rows into `state_changes` for every ledger from `oldest_ingest_ledger` up to the point where live ingestion takes over. Backfill writes no protocol rows. After a backfill lowers `oldest_ingest_ledger`, run history with `--rebuild` to cover the older ledgers. A plain run skips a protocol whose status is `success`.

| Setting | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL`, `NETWORK_PASSPHRASE`, `LOG_LEVEL` | | Same as `ingest` |
| `LEDGER_BACKEND_TYPE` | `rpc` | Use `datastore` for ranges older than the RPC retention window |
| `RPC_URL` | none | Required with `rpc`. With `datastore` it is optional, and without it the run skips contract metadata and the tip gauge |
| `DATASTORE_*` | | Same as `ingest`. See [ledger source](../configuration.md#ledger-source) |
| `GET_LEDGERS_LIMIT` | `10` | RPC backend only |
| `--protocol-id` | none | Required, repeatable |
| `--window-size` | `100` | Ledgers per commit. `0` or `1` commits every ledger |
| `--metrics-port` | `0` | Port for `/metrics`. `0` turns it off |
| `--rebuild` | `false` | Destructive. See below |

`--rebuild` resets the history cursor to `oldest_ingest_ledger - 1` and the status to `not_started`. It then deletes the protocol's rows in `state_changes` for ledgers `oldest_ingest_ledger` through `latest_ingest_ledger`, in slices of 10,000 ledgers, and migrates again. Live ingestion writes no history for the protocol until the rebuild reaches it, so the API returns empty or partial history for that protocol meanwhile. A rebuild refuses to start while the status is `in_progress`. Find out why the last run died first.

The run takes an advisory lock per protocol for history. A second history run or rebuild for the same protocol exits with:

```text
history lock for protocol "<ID>" is held: another migration or rebuild is running
```

Its cursor is the `ingest_store` key `protocol_<ID>_history_cursor`.

## protocol-migrate current-state

Builds the protocol's current-state tables from `--start-ledger` up to the point where live ingestion takes over. For SEP-41 these are `sep41_balances` and `sep41_allowances`.

Settings are the same as `protocol-migrate history`, plus:

| Flag | Default | Notes |
| --- | --- | --- |
| `--start-ledger` | none | Required, above 0. Used only when the cursor does not exist yet, and by `--rebuild` |

Current-state values are running totals. Pick a start ledger at or before the first ledger in which any of the protocol's contracts changed state. A later start gives wrong totals. The ledger source must serve that ledger, which usually means `LEDGER_BACKEND_TYPE=datastore`.

`--rebuild` runs one transaction that truncates the current-state tables (`TRUNCATE sep41_balances, sep41_allowances` for SEP-41), resets the cursor to `--start-ledger - 1`, and sets the status to `not_started`. `contract_tokens` is kept. Live ingestion stalls for every protocol while that transaction holds the cursor row, so the transaction gives up after waiting 5 seconds for a table lock. Rerun it if that happens. The API returns empty or partial balances for the protocol until the rebuild reaches live ingestion.

The lock and its error follow the history pattern with `current-state` in place of `history`. Its cursor is `protocol_<ID>_current_state_cursor`.

## Monitoring

Start the run with `--metrics-port <PORT>` and scrape `http://<HOST>:<PORT>/metrics`.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `wallet_migration_current_ledger` | | Last ledger the loop processed |
| `wallet_migration_ledgers_total` | | Ledgers processed. `rate()` gives ledgers per second |
| `wallet_migration_phase_duration_seconds` | `phase` | Time per phase: `fetch`, `extract`, `process`, `flush` |
| `wallet_migration_target_tip` | | RPC chain tip. `0` without `RPC_URL` |
| `wallet_migration_cursor` | `protocol_id` | Committed cursor |
| `wallet_migration_start_ledger` | `protocol_id` | Cursor when the run began. `cursor - start_ledger` is ledgers migrated |
| `wallet_migration_handoffs_total` | `protocol_id` | Handoffs to live ingestion |
| `wallet_migration_status` | `protocol_id` | `0` in progress, `1` success, `2` failed |

Without metrics, follow the log. Every 1,000 ledgers it prints:

```text
Progress: processed ledger <LEDGER> | <N> ledgers in <DURATION> (<RATE> l/s) | gate-wait=<P>% fetch-wait=<P>% extract=<P>% process=<P>% flush=<P>%
```

Near the tip the run waits for live ingestion and logs `migration gated on live ingestion's classification frontier`. The run ends with `Protocol <ID>: CAS failed at window [<A>,<B>], handoff to live ingestion detected`, then `<history|current state> migration completed successfully for protocols: [<ID>]`.

## Resuming and failures

| Situation | What to do |
| --- | --- |
| The run exits with an error or is killed | Fix the cause and run the same command again. It resumes from its cursor, which advances one window at a time |
| Status is `failed` | Rerun. A failed run resumes the same way |
| The run sits at `migration gated on live ingestion's classification frontier` | Start or fix live ingestion. The migration cannot finish without it |
| The run says `migration already completed, skipping` | The status is `success`. Use `--rebuild` to redo it |
| `protocol "<ID>" classification not complete` | Run `protocol-setup` first |
| `ingestion has not started yet (oldest_ingest_ledger is 0)` | Start live ingestion first. History needs ingested ledgers |
| The lock-held error | Another run for the same protocol and scope is active. Wait for it or stop it |

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/protocol_setup.go` | `protocol-setup` options |
| `cmd/protocol_migrate.go` | `protocol-migrate` options and the `/metrics` server |
| `internal/services/protocol_setup.go` | WASM classification |
| `internal/services/protocol_migrate.go` | Shared migration loop, gating, handoff |
| `internal/services/protocol_migrate_history.go` | History start ledger and persistence |
| `internal/services/protocol_migrate_current_state.go` | Current-state start ledger and persistence |
| `internal/services/protocol_migrate_history_rebuild.go` | History wipe |
| `internal/services/protocol_migrate_current_state_rebuild.go` | Current-state wipe |
| `internal/services/protocol_migrate_lock.go` | Per-protocol advisory locks |
| `internal/services/ingest_live.go` | Live cursor checks and re-probe |
| `internal/metrics/migration.go` | Migration metrics |
| `internal/db/migrations/protocols/001_sep41.sql` | SEP-41 registration |
