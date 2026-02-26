---
title: Token Ingestion Subsystem
type: reference
subsystem: token-ingestion
updated: 2026-02-26
---

# Token Ingestion Subsystem

## Overview

The token ingestion subsystem manages PostgreSQL storage of **account token holdings**: native XLM balances, classic Stellar trustlines, SAC (Stellar Asset Contract) balances for contract addresses, and SEP-41 contract token relationships. It operates in two distinct modes — checkpoint population (initial snapshot) and live ingestion (per-ledger streaming) — exposed through a single `TokenIngestionService` interface.

```mermaid
flowchart TD
    subgraph "Token Ingestion Entry Points"
        Pop["PopulateAccountTokens()<br/>First-run checkpoint snapshot"]
        Proc["ProcessTokenChanges()<br/>Per-ledger live ingestion"]
    end

    subgraph "Data Sources"
        Archive["Stellar History Archive<br/>(checkpoint snapshot)"]
        Indexer["IndexerBuffer<br/>(TrustlineChange, AccountChange,<br/>ContractChange, SACBalanceChange)"]
    end

    subgraph "PostgreSQL Tables"
        TA["trustline_assets<br/>(asset metadata, FK parent)"]
        TB["trustline_balances<br/>(account × asset balances)"]
        NB["native_balances<br/>(XLM balance per account)"]
        CT["contract_tokens<br/>(SEP-41 / SAC metadata)"]
        ACT["account_contract_tokens<br/>(account × contract junction)"]
        SAC["sac_balances<br/>(SAC balances for C... holders)"]
    end

    Archive --> Pop
    Pop --> TA
    Pop --> TB
    Pop --> NB
    Pop --> CT
    Pop --> ACT
    Pop --> SAC

    Indexer --> Proc
    Proc --> TB
    Proc --> NB
    Proc --> ACT
    Proc --> SAC

    style Pop fill:#e1f5fe
    style Proc fill:#e8f5e9
    style Archive fill:#fff3e0
    style Indexer fill:#fff3e0
```

**Key source files:**
- `internal/services/token_ingestion.go` — `TokenIngestionService` interface + both modes
- `internal/services/contract_validator.go` — WASM-based SEP-41 contract spec validation
- `internal/services/contract_metadata.go` — RPC-based metadata fetcher (name, symbol, decimals)
- `internal/data/trustline_balances.go`, `native_balances.go`, `sac_balances.go` — balance models
- `internal/data/contract_tokens.go`, `account_contract_tokens.go`, `trustline_assets.go` — token models

---

## Token Type Taxonomy

The subsystem tracks three distinct token types with separate storage semantics:

| Token Type | Holder Types | Storage Table | Ingestion Mode |
|-----------|-------------|---------------|---------------|
| **Native XLM** | G... accounts only | `native_balances` | Checkpoint + Live |
| **Classic Trustlines** | G... accounts only | `trustline_balances` (FK→ `trustline_assets`) | Checkpoint + Live |
| **SAC Balances** | **C... contracts only** | `sac_balances` (FK→ `contract_tokens`) | Checkpoint + Live |
| **SEP-41 Relationships** | G... and C... | `account_contract_tokens` (junction) | Checkpoint + Live |

**Critical distinction**: G-addresses holding SAC tokens (Stellar Asset Contracts) store their balances in `trustline_balances` — not `sac_balances`. The `sac_balances` table is exclusively for contract addresses (C...) holding SAC positions. This split mirrors how Stellar's ledger represents SAC state: G-address SAC holdings appear as standard trustlines.

---

## Checkpoint Population Flow (First Run)

`PopulateAccountTokens()` runs exactly once on first start to snapshot the entire ledger state from the Stellar history archive.

```mermaid
flowchart TD
    Start["PopulateAccountTokens(checkpointLedger, initializeCursors)"]
    Start --> OpenTx["db.RunInPgxTransaction()"]

    OpenTx --> PerfHints["SET LOCAL synchronous_commit = off<br/>SET LOCAL session_replication_role = 'replica'<br/>(disable FK checks, requires superuser)"]
    PerfHints --> StreamCP["streamCheckpointData()<br/>Read CheckpointChangeReader in loop"]

    subgraph "Per-Entry Processing in streamCheckpointData()"
        direction TB
        AccEntry["LedgerEntryTypeAccount<br/>→ batch.addNativeBalance()"]
        TLEntry["LedgerEntryTypeTrustline<br/>→ batch.addTrustline()<br/>+ collect uniqueAssets"]
        CCEntry["LedgerEntryTypeContractCode<br/>→ contractValidator.ValidateFromContractCode()<br/>→ contractTypesByWasmHash"]
        CDInstance["ContractData (ScvLedgerKeyContractInstance)<br/>→ processContractInstanceChange()<br/>SAC: extract metadata from ledger<br/>non-SAC: extract wasmHash"]
        CDBalance["ContractData (other keys)<br/>→ processContractBalanceChange()<br/>SAC: batch.addSACBalance()<br/>non-SAC: contractTokensByHolderAddress"]
    end

    StreamCP --> AccEntry
    StreamCP --> TLEntry
    StreamCP --> CCEntry
    StreamCP --> CDInstance
    StreamCP --> CDBalance

    FlushCheck{"batch.count() ≥ 250,000?"}
    AccEntry --> FlushCheck
    TLEntry --> FlushCheck
    CDBalance --> FlushCheck
    FlushCheck -->|Yes| Flush["batch.flush()<br/>BatchCopy to DB (COPY protocol)"]
    Flush --> Reset["batch.reset()"]
    Reset --> FlushCheck

    StreamCP --> FetchSAC["FetchSACMetadata(sacContractsNeedingMetadata)<br/>(via RPC for SAC contracts missing code/issuer)"]
    FetchSAC --> FetchSEP41["fetchSep41Metadata()<br/>FetchSep41Metadata(sep41ContractIDs)<br/>(via RPC for SEP-41 contracts)"]
    FetchSEP41 --> Store["storeTokensInDB()<br/>BatchInsert: trustline_assets + contract_tokens<br/>+ account_contract_tokens"]
    Store --> InitCursors["initializeCursors(dbTx)<br/>(caller callback - sets ingest cursors)"]
    InitCursors --> Commit["COMMIT"]

    style Start fill:#e1f5fe
    style Flush fill:#e8f5e9
    style Commit fill:#e8f5e9
```

### Streaming Batch Architecture

To handle 30M+ trustline entries without exhausting memory, the checkpoint reader uses a streaming batch pattern:

```mermaid
flowchart LR
    Reader["CheckpointChangeReader<br/>(sequential Read() loop)"] -->|entries| Batch["batch struct<br/>capacity: 250,000 per type"]

    Batch -->|"count ≥ 250,000"| Flush["batch.flush(ctx, dbTx)<br/>1. trustlineBalanceModel.BatchCopy()<br/>2. nativeBalanceModel.BatchCopy()<br/>3. sacBalanceModel.BatchCopy()"]

    Flush -->|success| Reset["batch.reset()<br/>(slice[:0] - keeps backing array)"]
    Reset --> Reader

    Batch -->|"end of stream"| FinalFlush["Final flush<br/>(batch.count() > 0)"]

    style Flush fill:#fff3e0
    style Reset fill:#e8f5e9
```

- `flushBatchSize = 250,000` — threshold before flushing to DB
- `batch.reset()` uses `slice[:0]` semantics to reuse backing arrays and avoid GC pressure
- All three balance types are flushed together in a single `flush()` call
- `BatchCopy` uses `pgx.CopyFrom` (PostgreSQL binary COPY protocol) for maximum throughput

### FK Constraint Strategy

The checkpoint transaction disables FK checks (`session_replication_role = 'replica'`) because balance entries may appear in the ledger *before* their parent contract instance entries. The code guarantees integrity: all parent rows (`trustline_assets`, `contract_tokens`) are collected and inserted via `storeTokensInDB()` before the transaction commits.

---

## Live Ingestion Flow (Per-Ledger)

`ProcessTokenChanges()` is called by `ingest_live.go:PersistLedgerData()` inside the per-ledger atomic transaction. It processes changes emitted by the indexer's four change processors.

```mermaid
flowchart TD
    Input["ProcessTokenChanges(<br/>dbTx pgx.Tx,<br/>trustlineChanges map,<br/>contractChanges []ContractChange,<br/>accountChanges map,<br/>sacBalanceChanges map)"]

    Input --> TL["processTrustlineChanges()<br/>Separate ADD/UPDATE vs REMOVE<br/>→ TrustlineBalanceModel.BatchUpsert()"]
    TL --> CC["processContractTokenChanges()<br/>Only SEP-41 contracts<br/>→ AccountContractTokensModel.BatchInsert()"]
    CC --> NB["processNativeBalanceChanges()<br/>Separate CREATE/UPDATE vs REMOVE<br/>→ NativeBalanceModel.BatchUpsert()"]
    NB --> SAC["processSACBalanceChanges()<br/>ADD/UPDATE vs REMOVE<br/>→ SACBalanceModel.BatchUpsert()"]

    subgraph "pgx.Batch (SendBatch)"
        UpsertQ["INSERT ... ON CONFLICT DO UPDATE<br/>(upsert rows)"]
        DeleteQ["DELETE WHERE account_address = $1 AND asset_id = $2<br/>(remove rows)"]
    end

    TL -.-> UpsertQ
    TL -.-> DeleteQ
    NB -.-> UpsertQ
    NB -.-> DeleteQ

    style Input fill:#e1f5fe
    style UpsertQ fill:#fff3e0
    style DeleteQ fill:#fff3e0
```

### Change Type Handling

| Change Map | Source Processor | Semantics |
|-----------|-----------------|-----------|
| `trustlineChangesByTrustlineKey` | `TrustlinesProcessor` | `ADD`/`UPDATE` → upsert; `REMOVE` → delete |
| `contractChanges` | `SACInstanceProcessor` | Only `SEP41` type appended to `account_contract_tokens` |
| `accountChangesByAccountID` | `AccountsProcessor` | `CREATE`/`UPDATE` → upsert; `REMOVE` → delete |
| `sacBalanceChangesByKey` | `SACBalancesProcessor` | `ADD`/`UPDATE` → upsert; `REMOVE` → delete |

All four `BatchUpsert` methods use `pgx.Batch.SendBatch()` — a pipeline of multiple SQL statements sent in a single round-trip.

---

## Contract Classification Pipeline

Contract tokens require a two-stage classification before storage:

```mermaid
flowchart TD
    ContractData["LedgerEntryTypeContractData<br/>with ScvLedgerKeyContractInstance key"]

    ContractData --> SACCheck["sac.AssetFromContractData()<br/>(Stellar SDK)"]
    SACCheck -->|isSAC=true| SACMeta["Extract code:issuer from ledger<br/>Decimals = 7 (hardcoded)<br/>Type = SAC"]

    SACCheck -->|isSAC=false| WASMCheck["Extract WasmHash from<br/>ContractExecutableWasm"]
    WASMCheck --> Validator["contractValidator.ValidateFromContractCode()<br/>wazero: compile WASM, extract contractspecv0<br/>section, validate SEP-41 function signatures"]

    Validator -->|SEP-41| SEP41["Type = SEP41<br/>→ RPC fetch: name(), symbol(), decimals()"]
    Validator -->|"other/error"| Unknown["Type = UNKNOWN<br/>(silently skipped in ProcessTokenChanges)"]

    SACMeta -->|code/issuer missing| RPCFetch["FetchSACMetadata() via RPC<br/>call name() → parse 'code:issuer'"]

    style SACMeta fill:#e8f5e9
    style SEP41 fill:#e8f5e9
    style Unknown fill:#ffebee
```

### SEP-41 Validation Logic

The `ContractValidator` uses `wazero` (a pure-Go WASM runtime) to:
1. Compile the WASM bytecode
2. Extract the `contractspecv0` custom section (XDR-encoded)
3. Unmarshal `ScSpecEntry` entries
4. Verify all 10 required function signatures match SEP-41:

| Function | Required Inputs | Output |
|---------|----------------|--------|
| `balance` | `id: Address` | `i128` |
| `allowance` | `from: Address, spender: Address` | `i128` |
| `decimals` | _(none)_ | `u32` |
| `name` | _(none)_ | `String` |
| `symbol` | _(none)_ | `String` |
| `approve` | `from, spender: Address, amount: i128, expiration_ledger: u32` | _(void)_ |
| `transfer` | `from, to: Address, amount: i128` OR `from: Address, to_muxed: MuxedAddress, amount: i128` (CAP-67) | _(void)_ |
| `transfer_from` | `spender, from, to: Address, amount: i128` | _(void)_ |
| `burn` | `from: Address, amount: i128` | _(void)_ |
| `burn_from` | `spender, from: Address, amount: i128` | _(void)_ |

The `transfer` function accepts both the classic and CAP-67 `MuxedAddress` variant.

### RPC Metadata Fetching

`ContractMetadataService` fetches `name`, `symbol`, and `decimals` by simulating RPC calls:

```mermaid
sequenceDiagram
    participant CMS as ContractMetadataService
    participant Pool as pond.Pool
    participant RPC as Stellar RPC

    CMS->>Pool: fetchBatch(contractIDs)
    Note over Pool: 20 contracts per batch<br/>2s sleep between batches

    loop Per batch of 20
        Pool->>RPC: SimulateTransaction(name())
        Pool->>RPC: SimulateTransaction(symbol())
        Pool->>RPC: SimulateTransaction(decimals())
        RPC-->>Pool: ScVal results
    end

    Pool-->>CMS: []ContractMetadata
    CMS->>CMS: Convert to []*data.Contract<br/>with DeterministicContractID()
```

- `simulateTransactionBatchSize = 20` — parallel contracts per batch
- `batchSleepDuration = 2s` — delay between batches to avoid RPC overload
- A **dummy keypair** (`keypair.MustRandom()`) is used as the transaction source account since simulation does not validate signatures or balances

---

## Deterministic ID System

Both trustline assets and contract tokens use UUID v5 (SHA-1) for deterministic ID generation, enabling batch inserts without DB round-trips:

```mermaid
flowchart LR
    AssetKey["CODE:ISSUER string<br/>(e.g., 'USDC:GABCD...')"] --> AssetNS["uuid.NewSHA1(<br/>  uuid.NameSpaceDNS ⊕ 'trustline_assets',<br/>  []byte(CODE + ':' + ISSUER))"]
    AssetNS --> AssetUUID["Deterministic Asset UUID"]

    ContractAddr["Contract C... address<br/>(e.g., 'CCCC...')"] --> ContractNS["uuid.NewSHA1(<br/>  uuid.NameSpaceDNS ⊕ 'contract_tokens',<br/>  []byte(contractAddress))"]
    ContractNS --> ContractUUID["Deterministic Contract UUID"]
```

Both IDs are stable: the same input always produces the same UUID across restarts, nodes, and runs. This enables `ON CONFLICT DO NOTHING` upserts without needing to SELECT first.

---

## Database Schema

```mermaid
flowchart TD
    subgraph "Reference Tables (parents)"
        TA["trustline_assets<br/>PK: id (UUID v5)<br/>UNIQUE: (code, issuer)"]
        CT["contract_tokens<br/>PK: id (UUID v5)<br/>UNIQUE: contract_id<br/>type: SAC | SEP41 | NATIVE | UNKNOWN"]
    end

    subgraph "Balance Tables (heavy write)"
        TB["trustline_balances<br/>PK: (account_address, asset_id)<br/>FK: asset_id → trustline_assets<br/>fillfactor=80, aggressive autovacuum"]
        NB["native_balances<br/>PK: account_address<br/>fillfactor=80, aggressive autovacuum"]
        SAC["sac_balances<br/>PK: (account_address, contract_id)<br/>FK: contract_id → contract_tokens<br/>balance: TEXT (i128)<br/>fillfactor=80, aggressive autovacuum"]
    end

    subgraph "Relationship Tables"
        ACT["account_contract_tokens<br/>PK: (account_address, contract_id)<br/>FK: contract_id → contract_tokens<br/>append-only (no deletes)"]
    end

    TA -->|"FK (DEFERRABLE INITIALLY DEFERRED)"| TB
    CT -->|"FK (DEFERRABLE INITIALLY DEFERRED)"| SAC
    CT -->|"FK (DEFERRABLE INITIALLY DEFERRED)"| ACT
```

### Autovacuum Tuning

All three balance tables share identical aggressive autovacuum settings (tuned for rows updated every ledger):

| Parameter | Value | Default | Rationale |
|-----------|-------|---------|-----------|
| `fillfactor` | 80 | 100 | 20% free space enables HOT updates (no dead tuple per update) |
| `autovacuum_vacuum_scale_factor` | 0.02 | 0.20 | Trigger at 2% dead rows instead of 20% |
| `autovacuum_analyze_scale_factor` | 0.01 | 0.10 | Refresh stats at 1% change |
| `autovacuum_vacuum_cost_delay` | 0 | 2ms | No throttle between vacuum cycles |
| `autovacuum_vacuum_cost_limit` | 1000 | 200 | 5× page budget per cycle |

The FK constraints use `DEFERRABLE INITIALLY DEFERRED` so they are checked at transaction COMMIT rather than per-statement — essential for the checkpoint population flow where balance rows are inserted before their parent rows.

---

## Interface Reference

### `TokenIngestionService`

```
PopulateAccountTokens(ctx, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error
ProcessTokenChanges(ctx, dbTx pgx.Tx,
    trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange,
    contractChanges []types.ContractChange,
    accountChangesByAccountID map[string]types.AccountChange,
    sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error
```

Two constructors exist:
- `NewTokenIngestionService()` — full constructor for live ingestion (requires archive, validator, metadata service)
- `NewTokenIngestionServiceForLoadtest()` — minimal constructor for load testing (no archive/validator/metadata; supports `ProcessTokenChanges` only)

### Data Model Interfaces

| Interface | Key Methods | Table |
|----------|------------|-------|
| `TrustlineAssetModelInterface` | `BatchInsert()` | `trustline_assets` |
| `TrustlineBalanceModelInterface` | `GetByAccount()`, `BatchUpsert()`, `BatchCopy()` | `trustline_balances` |
| `NativeBalanceModelInterface` | `GetByAccount()`, `BatchUpsert()`, `BatchCopy()` | `native_balances` |
| `SACBalanceModelInterface` | `GetByAccount()`, `BatchUpsert()`, `BatchCopy()` | `sac_balances` |
| `ContractModelInterface` | `GetExisting()`, `BatchInsert()` | `contract_tokens` |
| `AccountContractTokensModelInterface` | `GetByAccount()`, `BatchInsert()` | `account_contract_tokens` |

### Write Method Comparison

| Method | Protocol | Used By | Conflict Strategy |
|--------|---------|---------|------------------|
| `BatchCopy` | PostgreSQL COPY (binary) | Checkpoint population | None (FK checks disabled) |
| `BatchUpsert` | `pgx.Batch` (`SendBatch`) | Live ingestion per-ledger | `ON CONFLICT DO UPDATE SET` |
| `BatchInsert` (assets/contracts) | UNNEST upsert | Both modes | `ON CONFLICT DO NOTHING` |
| `BatchInsert` (account_contract_tokens) | UNNEST upsert | Both modes | `ON CONFLICT DO NOTHING` |

---

## Constants

| Constant | Value | Location | Description |
|---------|-------|----------|-------------|
| `flushBatchSize` | 250,000 | `token_ingestion.go` | Max entries before batch flush during checkpoint |
| `simulateTransactionBatchSize` | 20 | `contract_metadata.go` | Parallel RPC simulations per batch |
| `batchSleepDuration` | 2s | `contract_metadata.go` | Delay between RPC simulation batches |
| `contractSpecV0SectionName` | `"contractspecv0"` | `contract_validator.go` | WASM custom section name for contract spec |

---

**Topics:** [[entries/index]] | [[entries/ingestion]] | [[entries/data-layer]]
