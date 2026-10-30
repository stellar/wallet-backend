# Upgrading

For an operator moving a running deployment to a later wallet-backend release. After reading it you can upgrade in the right order, check the result, and know what to do when an upgrade goes wrong.

## Contents

- [Compatibility promise](#compatibility-promise)
- [Procedure](#procedure)
- [Upgrading the TimescaleDB extension](#upgrading-the-timescaledb-extension)
- [Filling `transaction_hashes` for history ingested before it existed](#filling-transaction_hashes-for-history-ingested-before-it-existed)
- [Version skew](#version-skew)
- [Rolling back](#rolling-back)
- [Where in the code](#where-in-the-code)

## Compatibility promise

Releases follow semantic versioning from v1.0.0.

| Surface | Promise |
| --- | --- |
| GraphQL schema | A field is removed only after it has carried `@deprecated` for at least one minor release |
| Flag and environment variable names | Stable within a major version |
| Database migrations | Forward-only. `migrate down` exists for local testing and is unsupported on a real deployment |

## Procedure

1. Read the release on [GitHub Releases](https://github.com/stellar/wallet-backend/releases). Check the **Tested with** block for PostgreSQL, TimescaleDB, stellar-rpc, and protocol versions, and follow any **Upgrade notes**.

2. Pull the image.

   ```bash
   docker pull public.ecr.aws/stellar/wallet-backend:<VERSION>
   ```

3. Stop the live ingester. It exits cleanly on SIGTERM: a batch that has not reached its commit barrier rolls back, and a barrier already in progress completes first. Stop any backfill too.

4. Apply migrations with the release binary.

   ```bash
   docker run --rm -e DATABASE_URL=<DATABASE_URL> public.ecr.aws/stellar/wallet-backend:<VERSION> migrate up
   ```

   Expect `Successfully applied <N> migrations up.` or `No migrations applied.`

5. Start the live ingester on the release image. It resumes from `latest_ingest_ledger`.

6. Restart the API instances on the release image.

7. Verify.

   ```bash
   docker run --rm public.ecr.aws/stellar/wallet-backend:<VERSION> version
   curl -s localhost:8002/health
   curl -s localhost:8001/health
   ```

   `version` prints `wallet-backend <VERSION> (commit <SHA>, <GO_VERSION>)`. A promoted image reports the release candidate it was built as, for example `v1.2.0-rc.2`. Every service also logs `wallet-backend <VERSION> (commit <SHA>)` at startup. Both `/health` calls return `"status":"ok"` once ingest is within 50 ledgers of the RPC.

If the release adds a protocol, run its data migration after step 5. See [protocol data migrations](protocols.md).

## Upgrading the TimescaleDB extension

The wallet-backend images and migrations require TimescaleDB 2.31.0 or newer. `ingest` refuses to start on an older extension and names the version it found and the version it requires.

To upgrade the extension:

1. Stop any running backfill.

2. Deploy the new database image. For Docker Compose, use the pinned `timescale/timescaledb:2.31.0-pg17`. For a managed or operator-run cluster, use the operator's image update.

3. In every database that has the extension, run this as the first statement of a fresh session started with `psql -X`.

   ```sql
   ALTER EXTENSION timescaledb UPDATE;
   ```

   The update must run before the old version is loaded into the session. A second attempt in the same session fails with `cannot be updated after the old version has already been loaded`. Running it again in a new session is harmless and reports that the version is already installed.

4. Verify.

   ```sql
   SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
   ```

The update leaves your data, compressed and uncompressed chunks, compression settings, and policies with their schedules intact. The background scheduler restarts itself during the update.

TimescaleDB 2.31.0 removes two settings: `timescaledb.enable_direct_compress_insert_sort_batches` and `timescaledb.enable_direct_compress_copy_sort_batches`. A leftover entry in `postgresql.conf` does not block startup. Remove it when convenient.

## Filling `transaction_hashes` for history ingested before it existed

The `transaction_hashes` lookup table (migration `2026-10-27.0`) gets a row for every transaction ingested after the migration. A database that already held history when the migration ran has no rows for that history, so `transactionByHash` returns `null` for those transactions. Two ways to fill it:

- Re-backfill the history with this release. Backfill writes the lookup rows with the transactions.
- Run the script below once. Save it as `fill-transaction-hashes.sql` and run:

  ```bash
  psql "<DATABASE_URL>" -v ON_ERROR_STOP=1 -f fill-transaction-hashes.sql
  ```

  It walks the history in 7-day ranges and commits after each one, writing compressed batches directly (TimescaleDB 2.28 or later): about a minute per 50 million transactions, measured. It prints one line per range. Run it with this release's ingester running or stopped, never while an older release ingests: transactions ingested by this release while the script runs get their rows from ingestion, an older release writes none. If the script stops part way, run it again; it skips complete ranges and writes only the rows a range is missing. It turns off `statement_timeout` for its own session, because the whole run is one statement.

```sql
SET statement_timeout = 0;

CREATE OR REPLACE PROCEDURE populate_transaction_hashes(from_epoch BIGINT DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    range_seconds CONSTANT BIGINT := 604800;
    range_start BIGINT;
    range_end BIGINT;
    newest BIGINT;
    lookup_rows BIGINT;
    transaction_rows BIGINT;
    inserted BIGINT;
BEGIN
    SELECT floor(extract(epoch FROM ledger_created_at))::bigint INTO newest
    FROM transactions ORDER BY ledger_created_at DESC LIMIT 1;
    IF newest IS NULL THEN
        RETURN;
    END IF;
    IF from_epoch IS NULL THEN
        SELECT floor(extract(epoch FROM ledger_created_at))::bigint INTO from_epoch
        FROM transactions ORDER BY ledger_created_at ASC LIMIT 1;
    END IF;
    range_start := from_epoch / range_seconds * range_seconds;
    WHILE range_start <= newest LOOP
        range_end := range_start + range_seconds;
        SELECT count(*) INTO lookup_rows FROM transaction_hashes
        WHERE ledger_close_epoch >= range_start AND ledger_close_epoch < range_end;
        SELECT count(*) INTO transaction_rows FROM transactions
        WHERE ledger_created_at >= to_timestamp(range_start) AND ledger_created_at < to_timestamp(range_end);
        IF lookup_rows <> transaction_rows THEN
            IF lookup_rows = 0 THEN
                SET LOCAL timescaledb.enable_direct_compress_insert = on;
                SET LOCAL timescaledb.enable_direct_compress_insert_client_sorted = on;
                INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch)
                SELECT ((('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint) >> 28)::smallint,
                       ('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint,
                       floor(extract(epoch FROM ledger_created_at))::bigint
                FROM transactions
                WHERE ledger_created_at >= to_timestamp(range_start) AND ledger_created_at < to_timestamp(range_end)
                ORDER BY 1, 2;
            ELSE
                INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch)
                SELECT ((('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint) >> 28)::smallint,
                       ('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint,
                       floor(extract(epoch FROM ledger_created_at))::bigint
                FROM transactions t
                WHERE t.ledger_created_at >= to_timestamp(range_start) AND t.ledger_created_at < to_timestamp(range_end)
                  AND NOT EXISTS (
                    SELECT 1 FROM transaction_hashes h
                    WHERE h.hash_bucket = ((('x' || encode(substr(t.hash, 1, 5), 'hex'))::bit(40)::bigint) >> 28)::smallint
                      AND h.hash_prefix = ('x' || encode(substr(t.hash, 1, 5), 'hex'))::bit(40)::bigint
                      AND h.ledger_close_epoch = floor(extract(epoch FROM t.ledger_created_at))::bigint
                  )
                ORDER BY 1, 2;
            END IF;
            GET DIAGNOSTICS inserted = ROW_COUNT;
            RAISE NOTICE 'transaction_hashes: wrote % rows for % .. %', inserted, to_timestamp(range_start), to_timestamp(range_end);
        END IF;
        COMMIT;
        range_start := range_end;
    END LOOP;
END
$$;

CALL populate_transaction_hashes();
DROP PROCEDURE IF EXISTS populate_transaction_hashes(BIGINT);
```

## Version skew

| Rule | Why |
| --- | --- |
| Run the API and the ingester on the same version | The schema, the ingest code, and the GraphQL resolvers ship together |
| Run `migrate up` with the release binary before any instance of that release starts | Nothing applies migrations automatically |
| Skipping releases is fine | `migrate up` applies every pending migration in order. A release note that says otherwise wins |

## Rolling back

Rolling back after `migrate up` is not supported. To return to an earlier release, restore the database from a backup taken before the upgrade and run the earlier image against it.

Lower the risk by running a new release against a copy of your data, or in a non-production environment, before it reaches production.

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/migrate.go` | `migrate up` and `migrate down` |
| `cmd/version.go` | `version` output |
| `cmd/root.go` | Startup version log line |
| `internal/ingest/ingest.go` | Clean shutdown on SIGTERM |
| `internal/db/migrations/` | Migration files, applied in name order |
