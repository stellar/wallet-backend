# Adding a protocol

For a contributor adding support for a Soroban contract protocol, such as a token or lending interface. After following these steps the protocol's contracts are classified, their events become rows, and the rows can be served over GraphQL.

SEP-41 is the reference implementation. Every code block below is trimmed from it. Replace `SEP41` and `sep41` with your protocol's ID and package name. Read [Protocols and data migrations](../architecture/protocols.md) first for how classification and processing fit together.

## Steps

1. **Pick the protocol ID and write the registration SQL.**

   The ID is an upper-case string stored in `protocols.id`. Classification is first-match-wins in lexicographic ID order. If your protocol's interface overlaps another protocol's, the alphabetically earlier ID claims the WASM.

   Create `internal/db/migrations/protocols/<NNN>_<protocol>.sql`. The files run in alphabetical order inside one transaction on every `migrate up` and every `protocol-setup`, with no tracking table, so the statement must be idempotent:

   ```sql
   INSERT INTO protocols (id) VALUES ('SEP41') ON CONFLICT (id) DO NOTHING;
   ```

   Verify: run `go run . migrate up --database-url <DATABASE_URL>`, then `SELECT id, classification_status FROM protocols;` lists your ID as `not_started`.

2. **Implement `ProtocolValidator`.**

   The validator decides which WASMs belong to the protocol and writes per-contract metadata. Create `internal/services/<protocol>/validator.go`. The interface has four methods:

   | Method | Receives | Returns | Rules |
   | --- | --- | --- | --- |
   | `ProtocolID` | nothing | the ID from step 1 | Must match the SQL row. |
   | `Match` | `[]WasmCandidate` with `Hash`, `Bytecode` and `SpecEntries` | the set of hashes it claims | Pure: no RPC, no DB. Candidates claimed by an earlier protocol are already removed. |
   | `Prefetch` | the RPC service, the candidates, the matched set, and `[]ContractCandidate` | an opaque plan | Runs before any DB transaction. `rpc` may be nil. Absorb per-contract RPC failures and return a smaller plan instead of an error. |
   | `Apply` | a `pgx.Tx`, the matched set, the contracts, the plan, and `*data.Models` | error | Runs inside the caller's transaction. No network calls. |

   Spec entries reach `Match` already decoded. The dispatcher compiles each WASM once with wazero, reads its `contractspecv0` custom section, and puts the `xdr.ScSpecEntry` values in `WasmCandidate.SpecEntries`. A WASM whose spec cannot be extracted is dropped before `Match` and stored unclassified.

   A `ContractCandidate` has `KnownProtocolID` set when its WASM was classified on an earlier run. Claim those too, so contracts deployed against an already-classified WASM get metadata.

   ```go
   const ProtocolID = "SEP41"

   type Validator struct {
       fetcher *metadataFetcher
       pool    pond.Pool
       models  *data.Models
   }

   var _ services.ProtocolValidator = (*Validator)(nil)

   func (v *Validator) ProtocolID() string { return ProtocolID }

   func (v *Validator) Match(candidates []services.WasmCandidate) map[types.HashBytea]struct{} {
       matched := map[types.HashBytea]struct{}{}
       for _, cand := range candidates {
           if len(cand.SpecEntries) == 0 {
               continue
           }
           if matchSEP41Spec(cand.SpecEntries) {
               matched[cand.Hash] = struct{}{}
           }
       }
       return matched
   }

   func (v *Validator) Prefetch(ctx context.Context, _ services.RPCService, _ []services.WasmCandidate,
       matched map[types.HashBytea]struct{}, contracts []services.ContractCandidate) (any, error) {
       contractsForUs := v.collectClaimedContracts(contracts, matched)
       if len(contractsForUs) == 0 || v.fetcher == nil {
           return sep41Prefetch{}, nil
       }
       // ... fetch name/symbol/decimals; on error log and return sep41Prefetch{}, nil
   }

   func (v *Validator) Apply(ctx context.Context, dbTx pgx.Tx, matched map[types.HashBytea]struct{},
       contracts []services.ContractCandidate, plan any, models *data.Models) error {
       contractsForUs := v.collectClaimedContracts(contracts, matched)
       if len(contractsForUs) == 0 {
           return nil
       }
       prefetch, _ := plan.(sep41Prefetch)
       return v.applyContractTokens(ctx, dbTx, models, contractsForUs, prefetch)
   }
   ```

   Verify: a table test of `Match` against real and non-matching spec entries passes with `go test ./internal/services/<protocol> -run Match`.

3. **Implement `ProtocolProcessor`.**

   The processor turns ledger data for the protocol's contracts into history rows and current-state rows. Create `internal/services/<protocol>/processor.go`.

   | Method | Job |
   | --- | --- |
   | `ProtocolID` | Returns the ID from step 1. |
   | `StateChangeOrdinalBase` | Returns this protocol's `state_change_id` namespace base. |
   | `ProcessLedger` | Folds one ledger into staged sets. It does not reset between ledgers. |
   | `RequiresContractData` | Returns true only if `ProcessLedger` reads `ContractDataChanges`. Event-only protocols return false and skip that extraction. |
   | `Reset` | Clears the staged sets. The caller invokes it after each window or ledger. |
   | `PersistHistory` | Writes staged state changes in the caller's transaction. |
   | `PersistCurrentState` | Writes staged balances or other current state in the caller's transaction. |
   | `WipeCurrentState` | Deletes all rows of the protocol's current-state tables. It must not touch `contract_tokens`, `protocol_wasms` or `protocol_contracts`. |

   **Ordinal base.** Add a constant to the `StateChangeOrdinalBase*` block in `internal/indexer/types/types.go`. Use the next multiple of `StateChangeOrdinalNamespaceWidth` that no constant in that block uses. The base is frozen once rows exist with it. `BuildProcessors` rejects a base that is zero, not a multiple of the width, or shared with another processor.

   **Determinism.** `PersistHistory` must assign IDs with `types.AssignStateChangeOrdinals` and this base. Within one `(to_id, operation_id)` group, emission order must come from on-chain data, never from Go map iteration or goroutine timing. SEP-41 sorts event keys by transaction and operation index before folding.

   **Batch equivalence.** Folding a window of ledgers and persisting once must give the same rows as persisting each ledger. Balances sum, last-write-wins values keep the newest, history appends. A protocol that cannot meet this must be migrated with `--window-size=1`.

   **Dependencies.** The factory receives `services.ProtocolDeps`. Pull the fields you need from it. Do not add protocol-specific wiring to `cmd/` or `internal/ingest/`.

   | Field | Notes |
   | --- | --- |
   | `NetworkPassphrase` | The network's passphrase. |
   | `Models` | `*data.Models`, including your models from step 4. |
   | `RPCService` | May be nil on paths without RPC configured. |
   | `ContractMetadataService` | May be nil. SEP-41's validator skips metadata enrichment when it is. |
   | `MetricsService` | `*metrics.Metrics`. |

   ```go
   type processor struct {
       networkPassphrase string
       balances          sep41data.BalanceModelInterface
       allowances        sep41data.AllowanceModelInterface
       stateChanges      data.StateChangeWriter
       // staged sets ...
   }

   func newProcessor(deps services.ProtocolDeps) *processor {
       p := &processor{networkPassphrase: deps.NetworkPassphrase, sep41Contracts: map[string]struct{}{}}
       if deps.Models != nil {
           p.balances = deps.Models.SEP41.Balances
           p.allowances = deps.Models.SEP41.Allowances
           p.stateChanges = deps.Models.StateChanges
       }
       p.Reset()
       return p
   }

   var _ services.ProtocolProcessor = (*processor)(nil)

   func (p *processor) ProtocolID() string { return ProtocolID }

   func (p *processor) StateChangeOrdinalBase() int64 { return types.StateChangeOrdinalBaseSEP41 }

   func (p *processor) RequiresContractData() bool { return false }

   func (p *processor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
       if len(p.stagedStateChanges) == 0 {
           return nil
       }
       types.AssignStateChangeOrdinals(p.stagedStateChanges, p.StateChangeOrdinalBase())
       _, err := p.stateChanges.BatchCopy(ctx, dbTx, p.stagedStateChanges)
       return err
   }
   ```

   `ProtocolProcessorInput` carries `LedgerSequence`, `LedgerCloseTime`, `ContractEvents` keyed by transaction and operation index, `ProtocolContracts` for the contracts classified as your protocol, `StagingMode`, and `ContractDataChanges` when requested. Events come from successful transactions only. Filter them to your contracts yourself.

   Verify: `go test ./internal/services/<protocol> -run Processor` passes, including a test that folds two ledgers and checks the persisted rows match per-ledger persistence.

4. **Add data models.**

   Create `internal/data/<protocol>/` with one file per table and a `models.go` aggregate. Expose interfaces so the processor can be tested with mocks.

   ```go
   type Models struct {
       Balances   BalanceModelInterface
       Allowances AllowanceModelInterface
   }

   func NewModels(pool *pgxpool.Pool, dbMetrics *metrics.DBMetrics) Models {
       return Models{
           Balances:   &BalanceModel{DB: pool, Metrics: dbMetrics},
           Allowances: &AllowanceModel{DB: pool, Metrics: dbMetrics},
       }
   }
   ```

   Add a field to `data.Models` in `internal/data/models.go` and set it in `NewModels`:

   ```go
   SEP41: sep41.NewModels(pool, dbMetrics),
   ```

   Put the wipe statement for step 3's `WipeCurrentState` here too. SEP-41 uses `TRUNCATE sep41_balances, sep41_allowances`, which costs the same at any row count.

   Verify: `go build ./...` succeeds.

5. **Add migrations for the protocol's tables.**

   Create `internal/db/migrations/<YYYY-MM-DD>.<N>-<protocol>_<table>.sql`. Files are embedded and applied by `migrate up` through sql-migrate. Each file has an Up and a Down block:

   ```sql
   -- +migrate Up
   CREATE TABLE sep41_balances (
       account_id BYTEA NOT NULL,
       contract_id UUID NOT NULL,
       balance NUMERIC NOT NULL DEFAULT 0,
       last_modified_ledger INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY (account_id, contract_id),
       CONSTRAINT fk_sep41_contract_token
           FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
           DEFERRABLE INITIALLY DEFERRED
   );

   -- +migrate Down
   DROP TABLE IF EXISTS sep41_balances;
   ```

   Current-state tables are plain tables. History rows go into the shared `state_changes` hypertable, so most protocols need no history table. Give every index a query that uses it.

   Verify: `go run . migrate up --database-url <DATABASE_URL>` reports the migration applied, and your data-model tests pass with `go test ./internal/data/<protocol>`.

6. **Register the protocol.**

   Create `internal/services/<protocol>/register.go`:

   ```go
   func init() {
       services.RegisterValidator(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolValidator {
           return newValidator(deps)
       })
       services.RegisterProcessor(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolProcessor {
           return newProcessor(deps)
       })
   }
   ```

   Blank-import the package in every entry point that builds validators or processors, and in the registry test:

   | File | Why |
   | --- | --- |
   | `internal/ingest/ingest.go` | Live ingestion classifies and processes the protocol. |
   | `cmd/protocol_setup.go` | `protocol-setup` classifies stored WASMs. |
   | `cmd/protocol_migrate.go` | `protocol-migrate` backfills history and current state. |
   | `internal/services/processor_registry_ext_test.go` | CI checks the ordinal base. |

   ```go
   _ "github.com/stellar/wallet-backend/internal/services/sep41" // registers SEP-41 validator + processor via init()
   ```

   Verify: `go test ./internal/services -run TestBuildProcessorsAllRegistered` passes, and `go run . protocol-setup --protocol-id <PROTOCOL_ID> --rpc-url <RPC_URL> --database-url <DATABASE_URL>` logs `Protocol setup completed successfully for protocols: [<PROTOCOL_ID>]`.

7. **Expose the data through GraphQL, if clients need it.**

   SEP-41 adds types to the schema files in `internal/serve/graphql/schema/`:

   | File | Addition |
   | --- | --- |
   | `balances.graphqls` | `SEP41Balance implements Balance`, `SEP41Allowance` |
   | `account.graphqls` | `Account.sep41Allowances` |
   | `pagination.graphqls` | `SEP41AllowanceConnection`, `SEP41AllowanceEdge` |
   | `enums.graphqls` | `SEP41` in `TokenType` |

   Readers go through the `BalanceReader` interface in `internal/serve/graphql/resolvers/resolver.go`. SEP-41's state changes reuse the existing `BALANCE` and `ALLOWANCE` categories, so it adds no state change type. A protocol with its own category needs a `StateChangeCategory` constant in `internal/indexer/types/types.go`, a type in `statechange.graphqls`, the enum value in `enums.graphqls`, and a case in `convertStateChangeTypes` in `internal/serve/graphql/resolvers/utils.go`.

   Every type and field needs a description. Regenerate code and the schema reference:

   ```bash
   make gql-generate
   make gql-docs
   ```

   Verify: `make gql-docs-check` passes and `make check` passes.

8. **Add tests.**

   | Kind | Where SEP-41's live | Notes |
   | --- | --- | --- |
   | Validator and processor unit tests | `internal/services/sep41/*_test.go` | Table-driven, `testify` `require` and `assert`. |
   | Data-model tests | `internal/data/sep41/*_test.go` | Use `dbtest.Open(t)` for an isolated TimescaleDB with migrations applied. |
   | Integration | `internal/integrationtests/data_migration_test.go` | Runs `protocol-setup` then `protocol-migrate current-state` in containers and checks balances. |

   For the integration suite, deploy a contract of your protocol in `internal/integrationtests/infrastructure/main_setup.go` and add a test method next to `TestProtocolSetupThenCurrentStateMigration`. See [Integration tests](integration-tests.md).

   Mocks for the data-model interfaces are hand-written with `testify/mock` in `mocks.go` inside the package, following `internal/data/sep41/mocks.go`. `.mockery.yml` lists no interfaces today. If you change an interface, update its mock in the same commit.

   Verify: `make unit-test` passes.

9. **Backfill history for an existing deployment.**

   Live ingestion produces the protocol's rows only after its migration cursors exist. Ledgers already ingested need `protocol-setup` and both `protocol-migrate` runs. Follow [Protocol data migrations](../operations/data-migrations.md).

## Checklist

Files touched for SEP-41:

| Path | Purpose |
| --- | --- |
| `internal/db/migrations/protocols/001_sep41.sql` | Inserts the `SEP41` row into `protocols`. |
| `internal/db/migrations/2026-04-17.0-sep41_balances.sql` | Creates `sep41_balances`. |
| `internal/db/migrations/2026-04-17.1-sep41_allowances.sql` | Creates `sep41_allowances`. |
| `internal/indexer/types/types.go` | Defines `StateChangeOrdinalBaseSEP41`. |
| `internal/services/sep41/validator.go` | `ProtocolValidator`: spec match, metadata prefetch, `contract_tokens` writes. |
| `internal/services/sep41/metadata.go` | Fetches `name`, `symbol` and `decimals` by RPC simulation. |
| `internal/services/sep41/events.go` | Decodes SEP-41 contract events. |
| `internal/services/sep41/processor.go` | `ProtocolProcessor`: folds events into state changes, balances and allowances. |
| `internal/services/sep41/register.go` | Registers both factories in `init()`. |
| `internal/data/sep41/models.go` | Aggregates the SEP-41 models. |
| `internal/data/sep41/balances.go` | Balance reads and delta writes. |
| `internal/data/sep41/allowances.go` | Allowance reads, upserts and expiry sweep. |
| `internal/data/sep41/wipe.go` | Truncates current-state tables. |
| `internal/data/sep41/mocks.go` | Hand-written mocks of the model interfaces. |
| `internal/data/models.go` | Adds `SEP41` to `data.Models`. |
| `internal/ingest/ingest.go` | Blank import for live ingestion. |
| `cmd/protocol_setup.go` | Blank import for `protocol-setup`. |
| `cmd/protocol_migrate.go` | Blank import for `protocol-migrate`. |
| `internal/services/processor_registry_ext_test.go` | Blank import for the base check. |
| `internal/serve/graphql/schema/balances.graphqls` | `SEP41Balance`, `SEP41Allowance`. |
| `internal/serve/graphql/schema/account.graphqls` | `Account.sep41Allowances`. |
| `internal/serve/graphql/schema/pagination.graphqls` | Allowance connection and edge. |
| `internal/serve/graphql/schema/enums.graphqls` | `TokenType.SEP41`. |
| `internal/serve/graphql/resolvers/balance_reader.go` | Adapts the SEP-41 models to `BalanceReader`. |
| `internal/serve/graphql/resolvers/account_allowances.go` | Resolves `sep41Allowances`. |
| `internal/integrationtests/data_migration_test.go` | End-to-end setup and migration test. |
| `internal/integrationtests/infrastructure/main_setup.go` | Deploys and mints the test SEP-41 token. |

## Where in the code

| Path | Role |
| --- | --- |
| `internal/services/protocol_validator.go` | `ProtocolValidator`, `WasmCandidate`, `ContractCandidate`, WASM spec extraction. |
| `internal/services/protocol_processor.go` | `ProtocolProcessor`, `ProtocolProcessorInput`, `StagingMode`, determinism rules. |
| `internal/services/protocol_deps.go` | `ProtocolDeps` passed to every factory. |
| `internal/services/validator_registry.go` | `RegisterValidator` and the validator registry. |
| `internal/services/processor_registry.go` | `RegisterProcessor`, `BuildProcessors` and base validation. |
| `internal/services/protocol_validation_dispatch.go` | Spec extraction, first-match-wins ordering, Prefetch then Apply. |
| `internal/services/protocol_setup.go` | `protocol-setup` service and its protocol-row check. |
| `internal/db/migrations/protocols/main.go` | Runs the registration SQL files in one transaction. |
| `cmd/migrate.go` | `migrate up` applies schema migrations, then registration SQL. |
| `internal/indexer/types/types.go` | Ordinal namespace width and bases, state change categories. |
