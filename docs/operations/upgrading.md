# Upgrading

For an operator moving a running deployment to a later wallet-backend release. After reading it you can upgrade in the right order, check the result, and know what to do when an upgrade goes wrong.

## Contents

- [Compatibility promise](#compatibility-promise)
- [Procedure](#procedure)
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

3. Stop the live ingester. It exits cleanly on SIGTERM and rolls back the ledger in flight. Stop any backfill too.

4. Apply migrations with the release binary.

   ```bash
   docker run --rm -e DATABASE_URL=<DATABASE_URL> public.ecr.aws/stellar/wallet-backend:<VERSION> migrate up
   ```

   Expect `Successfully applied <N> migrations up.` or `No migrations applied.`

5. Start the live ingester on the release image. It resumes from `latest_ingest_ledger`.

6. Roll the API pods to the release image.

7. Verify.

   ```bash
   docker run --rm public.ecr.aws/stellar/wallet-backend:<VERSION> version
   curl -s localhost:8002/health
   curl -s localhost:8001/health
   ```

   `version` prints `wallet-backend <VERSION> (commit <SHA>, <GO_VERSION>)`. A promoted image reports the release candidate it was built as, for example `v1.2.0-rc.2`. Every service also logs `wallet-backend <VERSION> (commit <SHA>)` at startup. Both `/health` calls return `"status":"ok"` once ingest is within 50 ledgers of the RPC.

If the release adds a protocol, run its data migration after step 5. See [protocol data migrations](data-migrations.md).

## Version skew

| Rule | Why |
| --- | --- |
| Run the API and the ingester on the same version | The schema, the ingest code, and the GraphQL resolvers ship together |
| Run `migrate up` with the release binary before any pod of that release starts | Nothing applies migrations automatically |
| Skipping releases is fine | `migrate up` applies every pending migration in order. A release note that says otherwise wins |

## Rolling back

Rolling back after `migrate up` is not supported. To return to an earlier release, restore the database from a backup taken before the upgrade and run the earlier image against it.

Lower the risk by running each release candidate on staging for at least a day before it reaches production. The release flow is in [releasing](../releasing.md).

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/migrate.go` | `migrate up` and `migrate down` |
| `cmd/version.go` | `version` output |
| `cmd/root.go` | Startup version log line |
| `internal/ingest/ingest.go` | Clean shutdown on SIGTERM |
| `internal/db/migrations/` | Migration files, applied in name order |
