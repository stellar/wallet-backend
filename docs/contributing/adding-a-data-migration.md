# Adding a data migration

For a contributor whose change alters what the indexer writes for history rows: a new column, or a corrected value. After following these steps, deployments that ingested history with an earlier release can rewrite those rows with `data-migrate run <name>`.

`transactions-memo` is the reference implementation. Every code block below is trimmed from it. Read [Data migrations](../deploy/data-migrations.md) first for how operators run one.

## Contents

- [When to use one](#when-to-use-one)
- [Steps](#steps)
- [Rules](#rules)
- [Changing the set of rows](#changing-the-set-of-rows)
- [Checklist](#checklist)
- [Where in the code](#where-in-the-code)

## When to use one

A data migration rewrites each ledger from that ledger's data alone. Use it only when the correct rows for a ledger depend on nothing outside that ledger.

| Your change | Use |
| --- | --- |
| A new or corrected column on `transactions`, `operations`, `state_changes`, or the participant tables | A data migration |
| Added or removed rows in those tables | A data migration that follows [Changing the set of rows](#changing-the-set-of-rows) |
| A protocol's current state, which is a running total over earlier ledgers | `protocol-migrate current-state --rebuild`. See [protocols](../deploy/protocols.md) |
| A protocol's state changes | `protocol-migrate history --rebuild` |

## Steps

1. **Change the live write path first.**

   A data migration covers only ledgers below the boundary. Live ingestion and backfill must already write the new form for every later ledger. For memos, the indexer writes `memo_type` and `memo`, and `internal/db/migrations/2026-11-10.0-transactions-memo.sql` adds the columns:

   ```sql
   ALTER TABLE transactions ADD COLUMN memo_type SMALLINT, ADD COLUMN memo BYTEA;
   ```

   Add a column as nullable with no default. On compressed chunks that `ALTER` changes only metadata, so it is fast on any history size. `NULL` then marks a row the migration has not reached.

   Verify: `make unit-test` passes with the indexer change.

2. **Write the database method `Apply` will call.**

   Put it on the table's model in `internal/data/`. It runs inside the transaction it is given and may touch compressed chunks. `BatchUpdateMemo` in `internal/data/transactions.go` shows the two things it needs:

   | Need | How `BatchUpdateMemo` does it |
   | --- | --- |
   | Prune chunks | Bounds the `UPDATE` by `ledger_created_at`, the partition column, as well as by `to_id` |
   | Write through compressed chunks | Runs `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0`, because one call covers up to 1,000 ledgers and can pass the default limit |

   ```go
   const query = `
       UPDATE transactions t SET memo_type = v.memo_type, memo = v.memo
       FROM unnest($1::bigint[], $2::smallint[], $3::bytea[]) AS v(to_id, memo_type, memo)
       WHERE t.to_id = v.to_id AND t.ledger_created_at BETWEEN $4 AND $5
   `
   ```

   Verify: a test that opens a database with `dbtest.Open(t)`, compresses a chunk, and calls the method passes.

3. **Implement `Migration`.**

   Create `internal/datamigrations/<name>.go`. The interface has four methods:

   | Method | Returns | Rules |
   | --- | --- | --- |
   | `Name` | the name in `data_migrations` and on the command line | Lower-case, hyphenated. Permanent once released |
   | `Tables` | the hypertables `Apply` writes | The runner compresses their chunks again once the walk has passed them |
   | `Extract` | the rows to write for one ledger, as `[]any` | Pure: reads only the ledger and `Deps`. No database access |
   | `Apply` | error | Writes rows `Extract` returned for a run of consecutive ledgers, inside `dbTx`. Idempotent |

   `Deps` carries `Models`, `NetworkPassphrase`, and `Pool`, a worker pool for per-ledger CPU work such as transaction hashing.

   ```go
   type transactionsMemo struct{}

   func (transactionsMemo) Name() string { return "transactions-memo" }

   func (transactionsMemo) Tables() []string { return []string{"transactions"} }

   func (transactionsMemo) Extract(ctx context.Context, deps Deps, ledger xdr.LedgerCloseMeta) ([]any, error) {
       seq := ledger.LedgerSequence()
       txs, err := indexer.GetLedgerTransactions(ctx, deps.NetworkPassphrase, ledger, deps.Pool)
       if err != nil {
           return nil, fmt.Errorf("reading ledger %d transactions: %w", seq, err)
       }
       rows := make([]any, 0, len(txs))
       for _, tx := range txs {
           memoType, memo := utils.EncodeMemo(tx.Envelope.Memo())
           rows = append(rows, data.TransactionMemo{
               ToID:            toid.New(int32(seq), int32(tx.Index), 0).ToInt64(),
               LedgerCreatedAt: ledger.ClosedAt(),
               MemoType:        memoType,
               Memo:            memo,
           })
       }
       return rows, nil
   }

   func (transactionsMemo) Apply(ctx context.Context, deps Deps, dbTx pgx.Tx, rows []any) error {
       memos := make([]data.TransactionMemo, len(rows))
       for i, r := range rows {
           memos[i] = r.(data.TransactionMemo) //nolint:errcheck // Extract returns only this type
       }
       if _, err := deps.Models.Transactions.BatchUpdateMemo(ctx, dbTx, memos); err != nil {
           return fmt.Errorf("updating memos: %w", err)
       }
       return nil
   }
   ```

   Reuse the code the live indexer runs to build the value, as `utils.EncodeMemo` does here. Then a migrated row and a live row cannot differ.

4. **Register it in `All`.**

   Add the type to `All` in `internal/datamigrations/migration.go`:

   ```go
   var All = []Migration{
       transactionsMemo{},
   }
   ```

   On its next start the live ingester inserts a `data_migrations` row for every entry in `All` it has not recorded: `pending` with the boundary on a populated database, `done` on a database it bootstraps.

   Verify: `go run . data-migrate status --database-url <DATABASE_URL>` lists the name as `not recorded`. After `go run . ingest` starts, it shows `pending`.

5. **Add tests.**

   | Kind | Where `transactions-memo`'s live | Covers |
   | --- | --- | --- |
   | Runner tests | `internal/datamigrations/runner_test.go` | Walk order, recompression, resume from the cursor, a failing slice that records `last_error` and then resumes, cancellation, and the cases that do not run |
   | Integration | `internal/integrationtests/memo_migration_test.go` | Clears memos on a real database, runs `data-migrate run`, and checks every row |

   For a new migration, check at least:

   - `Extract` output for a ledger matches what live ingestion writes for the same ledger.
   - `Apply` run twice on the same rows leaves the table as one run does.
   - A run over a compressed chunk leaves the chunk compressed with the new values.
   - A run stopped mid-walk and started again ends with the same rows as one uninterrupted run.

   `runner_test.go` serves ledgers from a fake backend and compresses a chunk before the run. Copy its fixture for a migration on another table.

   Verify: `make unit-test` passes.

6. **Document it for operators.**

   Add a row to the table of known migrations in [Data migrations](../deploy/data-migrations.md#what-a-data-migration-is), and a release note in [upgrading](../deploy/upgrading.md#release-notes) that names the migration and the step to run it.

   Verify: `make docs-check` passes.

## Rules

| Rule | Why |
| --- | --- |
| `Extract` is a pure function of one ledger | The runner reads ledgers in slices, newest first, through a fresh backend per slice. Nothing carries over from one ledger to the next |
| `Apply` is idempotent | An interrupted run redoes up to one slice. A ledger written twice must end the same as written once |
| The name is permanent | `done` is final. A later fix to the same data is a migration with a new name, so deployments that finished the first one run the fix too |
| `Apply` writes only the rows of the ledgers it is given | The runner gives it only ledgers below the boundary. The live ingester writes every ledger from the boundary on, and the two never coordinate |
| No tuning knobs | Slice size, flush size, and retries are constants in `runner.go`. Operators run every migration the same way |

## Changing the set of rows

An `UPDATE` by key fits a fix that changes values on rows that exist. A fix that adds or removes rows does not:

| Table | Why an update or a plain insert fails |
| --- | --- |
| `state_changes` | `state_change_id` is an ordinal within its operation, numbered per emitter. Adding or dropping one change renumbers the ones after it, so new rows collide with old keys or leave stale rows |
| `transactions_accounts`, `operations_accounts` | One row per participant. A row for an account the fix drops as a participant stays unless `Apply` deletes it |

In `Apply`, replace the ledger's whole set of rows for the affected emitter: delete them, then insert the new set, both in `dbTx`. An emitter is one `state_change_id` namespace, as defined in `internal/indexer/types/types.go`: the indexer's processors each have a sub-namespace, and each protocol has its own.

Model the delete on `DeleteNamespaceLedgerRange` in `internal/data/statechanges.go`. It bounds `to_id` by the ledger range and `state_change_id` by the namespace, so it prunes compressed batches without decompressing them. It opens its own transaction, so `Apply` cannot call it as it is. No shared helper for this exists yet; add one to the model that takes the caller's transaction.

## Checklist

Files touched for `transactions-memo`:

| Path | Purpose |
| --- | --- |
| `internal/db/migrations/2026-11-10.0-transactions-memo.sql` | Adds the nullable `memo_type` and `memo` columns |
| `internal/indexer/processors/utils.go` | Writes the memo for new ledgers |
| `internal/indexer/types/types.go` | Memo fields on the transaction row |
| `internal/utils/ingestion_utils.go` | `EncodeMemo`, shared by the indexer and the migration |
| `internal/data/transactions.go` | `TransactionMemo` and `BatchUpdateMemo` |
| `internal/datamigrations/transactions_memo.go` | The `Migration` |
| `internal/datamigrations/migration.go` | Registers it in `All` |
| `internal/datamigrations/runner_test.go` | Runner tests on `transactions-memo` |
| `internal/integrationtests/memo_migration_test.go` | End-to-end migration test |
| `docs/deploy/data-migrations.md` | Known migrations table |
| `docs/deploy/upgrading.md` | Release note |

## Where in the code

| Path | Role |
| --- | --- |
| `internal/datamigrations/migration.go` | `Migration`, `Deps`, `All`, `Find` |
| `internal/datamigrations/runner.go` | Lock, walk, slices, retries, recompression |
| `internal/data/data_migrations.go` | `data_migrations` rows and the chunk helpers |
| `internal/db/migrations/2026-11-10.1-data-migrations.sql` | `data_migrations` table |
| `internal/services/ingest_live.go` | Records each migration on start |
| `cmd/data_migrate.go` | `data-migrate run` and `data-migrate status` |
