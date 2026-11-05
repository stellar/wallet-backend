# Upgrading

For an operator moving a running deployment to a later wallet-backend release. After reading it you can upgrade in the right order, check the result, and know what to do when an upgrade goes wrong.

## Contents

- [Compatibility promise](#compatibility-promise)
- [Procedure](#procedure)
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

## Filling `transaction_hashes` for history ingested before it existed

The `transaction_hashes` lookup table (migration `2026-10-27.0`) gets a row for every transaction ingested after the migration. A database that already held history when the migration ran has no rows for that history, so `transactionByHash` returns `null` for those transactions. Two ways to fill it:

- Re-backfill the history with this release. Backfill writes the lookup rows with the transactions and recompresses the lookup table as it goes. This is the faster path for years of history.
- Run the script below once. Save it as `fill-transaction-hashes.sql` and run:

  ```bash
  psql "<DATABASE_URL>" -v ON_ERROR_STOP=1 -f fill-transaction-hashes.sql
  ```

  It walks the history in 7-day ranges of `transactions`, from the oldest transaction to the moment the migration ran, and commits after each one. The lookup table is partitioned by hash, so a range's rows spread over all of its chunks and sit in their rowstore parts until the table is recompressed. After every 500 million rows inserted since the last recompression, and at the end, the script runs the table's maintenance procedure, recompression only. The pass size is counted in rows, not ranges: between two of those calls the rowstore parts hold up to 500 million rows plus one range, at about 90 bytes each with the index, about 45 GB. A restarted run counts from zero, so its first pass can follow a partial one. Measured on a pubnet database: the insert of a 25-million-row week takes about 5 minutes and writes 4.4 GB of WAL, and each recompress pass rewrites every batch the ranges wrote into, which for years of history is most of the table. It prints one line per range and one per recompression. The script is for deployments that already hold history at today's scale. A deployment started at a higher transaction rate gets its lookup rows from the backfill, so the script never runs at that scale. Run it with this release's ingester running or stopped, never while an older release ingests: transactions ingested by this release get their rows from ingestion, an older release writes none. Ledgers that closed before the migration and were ingested by this release get a second row from the script; lookups tolerate that. If the script stops part way, run it again; it skips the ranges recorded in `transaction_hashes_fill`, and a run after a complete one writes nothing. Drop that table once the fill is done. The script turns off `statement_timeout` for its own session, because the whole run is one statement.

```sql
SET statement_timeout = 0;

CREATE TABLE IF NOT EXISTS transaction_hashes_fill (range_start BIGINT PRIMARY KEY);

-- Fills transaction_hashes for the transactions that closed before until_epoch (default: when
-- migration 2026-10-27.0 ran), 7 days at a time, recompressing the table after every 500,000,000 rows inserted.
CREATE OR REPLACE PROCEDURE populate_transaction_hashes(until_epoch BIGINT DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    range_seconds CONSTANT BIGINT := 604800;
    rows_per_recompress CONSTANT BIGINT := 500000000;
    cur_start BIGINT;
    cur_end BIGINT;
    inserted BIGINT;
    since_recompress BIGINT := 0;
BEGIN
    IF until_epoch IS NULL THEN
        SELECT extract(epoch FROM applied_at)::bigint INTO until_epoch
        FROM gorp_migrations WHERE id = '2026-10-27.0-transaction-hashes.sql';
    END IF;
    SELECT floor(extract(epoch FROM ledger_created_at))::bigint / range_seconds * range_seconds INTO cur_start
    FROM transactions ORDER BY ledger_created_at ASC LIMIT 1;
    IF cur_start IS NULL THEN
        RETURN;
    END IF;
    WHILE cur_start < until_epoch LOOP
        cur_end := LEAST(cur_start + range_seconds, until_epoch);
        IF NOT EXISTS (SELECT 1 FROM transaction_hashes_fill f WHERE f.range_start = cur_start) THEN
            INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch)
            SELECT ((('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint) >> 20)::integer,
                   ('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint,
                   floor(extract(epoch FROM ledger_created_at))::bigint
            FROM transactions
            WHERE ledger_created_at >= to_timestamp(cur_start) AND ledger_created_at < to_timestamp(cur_end);
            GET DIAGNOSTICS inserted = ROW_COUNT;
            INSERT INTO transaction_hashes_fill VALUES (cur_start);
            RAISE NOTICE 'transaction_hashes: wrote % rows for % .. %', inserted, to_timestamp(cur_start), to_timestamp(cur_end);
            COMMIT;
            since_recompress := since_recompress + inserted;
            IF since_recompress >= rows_per_recompress THEN
                RAISE NOTICE 'transaction_hashes: recompressing after % rows', since_recompress;
                CALL transaction_hashes_maintenance(0, '{"workers": 1, "worker": 0}');
                since_recompress := 0;
            END IF;
        END IF;
        cur_start := cur_start + range_seconds;
    END LOOP;
    RAISE NOTICE 'transaction_hashes: recompressing after % rows', since_recompress;
    CALL transaction_hashes_maintenance(0, '{"workers": 1, "worker": 0}');
END
$$;

CALL populate_transaction_hashes();
DROP PROCEDURE populate_transaction_hashes(BIGINT);
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
