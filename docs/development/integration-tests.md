# Integration tests

For a contributor who needs to run, extend or debug the end-to-end tests. After reading it you can run the suite locally, run one suite on its own, and find out why a run failed.

The tests start a private Stellar network, submit real transactions, and check what wallet-backend ingests and serves. Everything runs in Docker through testcontainers-go.

## What they cover

`TestIntegrationTests` in `internal/integrationtests/main_test.go` runs the suites in this order, on one shared set of containers:

| Order | Suite | Checks |
| --- | --- | --- |
| 1 | `AccountBalancesAfterCheckpointTestSuite` | Balances loaded from the history archive checkpoint on first start: native, trustline, SAC, SEP-41, contract holders and pool shares, plus balance pagination. |
| 2 | `BackfillTestSuite` | A backfill container fills a ledger range while live ingestion keeps running. |
| 3 | `DataMigrationTestSuite` | `protocol-setup` classifies the SEP-41 WASM, then `protocol-migrate current-state` builds SEP-41 balances from the object-store datastore. |
| 4 | `DataValidationTestSuite` | Transactions, operations and state changes for each fixture: payments, sponsorships, custom assets, auth flags, merges, contract calls and deploys, claimable balances, liquidity pools. |
| 5 | `AccountBalancesAfterLiveIngestionTestSuite` | Balances after live ingestion applies the fixture transactions. |

Between suites 1 and 2 the harness submits the fixture transactions to RPC. Two gates stop the run early:

| Gate | Effect |
| --- | --- |
| Suite 1 fails | Suites 2 to 5 are skipped. |
| Any of suites 1 to 4 failed | Suite 5 is skipped. |

## Run them

1. Start Docker. The harness builds an image and starts containers through the local Docker daemon.

   Verify: `docker info` exits 0.

2. Run the full suite.

   ```bash
   make integration-test
   ```

   This runs `ENABLE_INTEGRATION_TESTS=true go test -v ./internal/integrationtests/... -timeout 30m`. Without `ENABLE_INTEGRATION_TESTS=true` the test skips itself, which is why `make unit-test` never starts containers.

   Verify: the output ends with `--- PASS: TestIntegrationTests` and `ok`.

3. Rebuild the wallet-backend image and run.

   ```bash
   make integration-test-clean
   ```

   This sets `FORCE_REBUILD=true`. The image tag is `wallet-backend:integration-test-<SHORT_SHA>`, keyed on the `HEAD` commit. Without `FORCE_REBUILD=true` an existing image with that tag is reused, so uncommitted code changes are not in it. Use this target whenever you test uncommitted changes.

   Verify: the log shows `FORCE_REBUILD=true, rebuilding image`.

4. Run one suite or one test with `-run`. Suites are subtests of `TestIntegrationTests`, so the pattern has two or three parts:

   ```bash
   ENABLE_INTEGRATION_TESTS=true go test -v ./internal/integrationtests/... -timeout 30m \
     -run 'TestIntegrationTests/DataMigrationTestSuite'

   ENABLE_INTEGRATION_TESTS=true go test -v ./internal/integrationtests/... -timeout 30m \
     -run 'TestIntegrationTests/DataValidationTestSuite/TestPaymentOperationDataValidation'
   ```

   Container setup and fixture submission still run in full. Only the suite selection changes.

   Verify: the output lists only the selected subtest under `TestIntegrationTests`.

| Variable | Effect |
| --- | --- |
| `ENABLE_INTEGRATION_TESTS=true` | Runs the tests. Any other value skips them. |
| `FORCE_REBUILD=true` | Rebuilds the wallet-backend image even if the tag exists. |

The Makefile and CI both set a 30-minute `go test` timeout.

CI runs the same command in the `integration-test` job of `.github/workflows/go.yaml`. That job builds the image with Docker Buildx and a GitHub Actions layer cache, tags it `wallet-backend:integration-test-<SHORT_SHA>`, and loads it into Docker, so the harness finds it and skips its own build.

## How the harness works

All containers join one Docker network and carry the label `org.testcontainers.session-id=wallet-backend-integration-tests`.

| Container | Image | Role | Exposed port |
| --- | --- | --- | --- |
| `core-postgres` | `postgres:9.6.17-alpine` | Database for stellar-core. | 5432 |
| `stellar-core` | `stellar/stellar-core:28` | Standalone validator and history archive. | 11625 peer, 11626 HTTP, 1570 archive |
| `stellar-rpc` | `stellar/stellar-rpc:28.0.0` | RPC over the standalone network. | 8000 |
| `object-store` | `chrislusf/seaweedfs:4.46` | S3-compatible store for the datastore ledger backend. | 8333 |
| `wallet-backend-db` | `timescale/timescaledb:latest-pg17` | wallet-backend database. | 5432 |
| `wallet-backend-ingest` | `wallet-backend:integration-test-<SHORT_SHA>` | Runs `migrate up`, then `ingest`. | 8003 |
| `wallet-backend-api` | `wallet-backend:integration-test-<SHORT_SHA>` | Runs `serve`. | 8002 |
| `wallet-backend-backfill-<N>` | `wallet-backend:integration-test-<SHORT_SHA>` | Backfill run started by `BackfillTestSuite`. | 8003 |
| `wallet-backend-protocol-setup`, `wallet-backend-protocol-migrate` | `wallet-backend:integration-test-<SHORT_SHA>` | One-shot commands started by `DataMigrationTestSuite`. | none |

Setup runs in this order:

1. Build the wallet-backend image from the repository's `Dockerfile` with `--build-arg GIT_COMMIT=<SHORT_SHA>`, unless the tag exists and `FORCE_REBUILD` is not `true`.
2. Start `core-postgres`, `stellar-core`, `stellar-rpc` and `object-store`. stellar-core runs standalone with the network passphrase `Standalone Network ; February 2017` and upgrades to protocol 28.
3. Create test accounts and trustlines, deploy the native and credit-asset SACs, upload and deploy the SEP-41 token and a holder contract, then mint and transfer. Wait for the next checkpoint so these appear in the archive.
4. Start `wallet-backend-db` and `wallet-backend-ingest`. On the empty database ingest bootstraps from the latest checkpoint.
5. Wait until RPC's latest ledger minus `latest_ingest_ledger` is 50 or less, polling every 2 seconds for up to 5 minutes.
6. Start `wallet-backend-api` and wait for `/health` to respond.

**Accelerated checkpoints.** `standalone-core.cfg` and `captive-core.cfg` set `ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING=true`. Checkpoints then come every 8 ledgers instead of 64. `stellar_rpc_config.toml` sets `CHECKPOINT_FREQUENCY=8`, and the ingest and backfill containers set `CHECKPOINT_FREQUENCY=8` to match.

**Auth.** The API and ingest containers set `CLIENT_AUTH_PUBLIC_KEYS` to a keypair the harness generates, and the test client signs its requests with it. The API container sets `GRAPHQL_COMPLEXITY_LIMIT=5000`.

**Datastore.** For `DataMigrationTestSuite` the harness runs an exporter that reads ledgers from RPC and writes them to the `ledgers` bucket in `object-store`, one ledger per file. `protocol-migrate` reads them back with `LEDGER_BACKEND_TYPE=datastore`.

## Test data

Pre-compiled WASM files live in `internal/integrationtests/infrastructure/testdata/`, so tests need no Rust toolchain.

| File | Source | Used as |
| --- | --- | --- |
| `soroban_token_contract.wasm` | `token` example from stellar/soroban-examples v22.0.1 | The SEP-41 token. The harness deploys it with name `SEP41 Token`, symbol `SEP41`, 7 decimals, and the master account as admin. |
| `soroban_increment_contract.wasm` | stellar/go SDK integration test data | A contract that holds SAC and SEP-41 balances. |

The token WASM's SHA-256 is `e80840a63de88eda39d2a2525e8f059201e17066e02777e0d410f1959d888c1d`.

To rebuild the token WASM:

1. Clone the examples at the pinned tag.

   ```bash
   git clone -b v22.0.1 https://github.com/stellar/soroban-examples.git
   cd soroban-examples/token
   ```

2. Build it.

   ```bash
   stellar contract build
   ```

3. Copy the output into the test data folder.

   ```bash
   cp target/wasm32v1-none/release/soroban_token_contract.wasm \
     <WALLET_BACKEND_REPO>/internal/integrationtests/infrastructure/testdata/
   ```

Verify: `shasum -a 256 internal/integrationtests/infrastructure/testdata/soroban_token_contract.wasm` prints the hash, and you update the hash in `testdata/README.md` if it changed.

The token's name, symbol and decimals come from constructor arguments in `main_setup.go`, not from the WASM.

## Debugging a failure

1. Read the failing assertion. Command containers include their logs in the assertion message when they exit non-zero. Container start failures print the container's logs before the error.

2. Read container logs while the run is going. Every wallet-backend container runs with `LOG_LEVEL=DEBUG`, and the test process logs at debug level too.

   ```bash
   docker ps --filter label=org.testcontainers.session-id=wallet-backend-integration-tests
   docker logs -f wallet-backend-ingest
   ```

3. Read logs after the run. Cleanup terminates the shared containers when `TestIntegrationTests` returns. Backfill containers and the `protocol-setup` and `protocol-migrate` containers are not terminated, so `docker logs` works on them afterwards.

   ```bash
   docker logs wallet-backend-protocol-setup
   docker logs wallet-backend-backfill-1
   ```

   The harness has no option to keep the shared containers running after the test ends.

4. Remove leftovers before the next run. Shared containers have fixed names and are created with reuse on, so a container left by an aborted run is picked up again.

   ```bash
   docker rm -f $(docker ps -aq --filter label=org.testcontainers.session-id=wallet-backend-integration-tests)
   ```

   Verify: the `docker ps` filter from step 2 lists nothing.

5. Test uncommitted changes with `make integration-test-clean`. A stale image built from `HEAD` is the most common cause of a test that fails locally after a fix.

## Where in the code

| Path | Role |
| --- | --- |
| `Makefile` | `integration-test`, `integration-test-clean`, `build-integration-image`, `rebuild-integration-image`. |
| `internal/integrationtests/main_test.go` | Suite order and skip-on-failure gates. |
| `internal/integrationtests/infrastructure/main_setup.go` | Setup sequence, fixtures, ingest sync wait, cleanup. |
| `internal/integrationtests/infrastructure/containers.go` | Container definitions, image build and `FORCE_REBUILD`. |
| `internal/integrationtests/infrastructure/ledger_exporter.go` | Object store settings and the RPC-to-datastore exporter. |
| `internal/integrationtests/infrastructure/run_command.go` | One-shot wallet-backend command containers. |
| `internal/integrationtests/infrastructure/config/` | stellar-core, captive-core and RPC configs. |
| `internal/integrationtests/infrastructure/testdata/` | WASM files and their provenance. |
| `.github/workflows/go.yaml` | CI `integration-test` job. |
