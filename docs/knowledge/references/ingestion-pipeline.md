---
description: Live and backfill ingestion flow, indexer processors, retry logic, and ledger event processing
type: reference
subsystem: ingestion
areas: [ingestion, indexer, timescaledb, stellar-rpc]
status: current
vault: docs/knowledge
---

# Ingestion Pipeline Architecture

## High-Level Overview

The ingestion pipeline reads Stellar ledger data and persists it into TimescaleDB. It runs as two independent modes — **live** and **backfill** — that share a common Indexer core.

```mermaid
flowchart TD
    CLI["CLI: go run main.go ingest"]
    CLI --> Mode{ingestion_mode?}

    Mode -->|live| Live["startLiveIngestion()"]
    Mode -->|backfill| Backfill["startBackfilling()"]

    Live --> Lock["Acquire advisory lock<br/>(FNV hash of network name)"]
    Lock --> CursorCheck{"latestLedgerCursor<br/>exists?"}

    CursorCheck -->|No: first run| InitFromArchive["Get latest ledger<br/>from history archive"]
    InitFromArchive --> PopulateTokens["PopulateAccountTokens()<br/>+ initialize both cursors"]
    PopulateTokens --> CatchupCheck

    CursorCheck -->|Yes: restart| LoadMetrics["Load oldest/latest<br/>cursors into Prometheus"]
    LoadMetrics --> CatchupCheck{"Fallen behind by<br/>≥ catchupThreshold?"}

    CatchupCheck -->|Yes| Catchup["startBackfilling()<br/>in BackfillModeCatchup"]
    Catchup --> UnboundedRange

    CatchupCheck -->|No| UnboundedRange["PrepareRange(unbounded)<br/>from startLedger"]
    UnboundedRange --> LiveLoop["ingestLiveLedgers()"]
    LiveLoop --> Indexer

    Backfill --> GapCalc["Calculate gaps to fill"]
    GapCalc --> SplitBatches["Split into batches<br/>(backfillBatchSize)"]
    SplitBatches --> WorkerPool["Process batches in<br/>parallel (pond pool)"]
    WorkerPool --> Indexer

    Indexer["Indexer.ProcessLedgerTransactions()"]
    Indexer --> Buffer["IndexerBuffer"]
    Buffer --> DB["TimescaleDB<br/>(transactions, operations,<br/>state_changes)"]

    style Live fill:#e1f5fe
    style Backfill fill:#fff3e0
    style Indexer fill:#e8f5e9
    style DB fill:#f3e5f5
```

**Key source files:**
- `internal/ingest/ingest.go` — Entry point, dependency wiring, config
- `internal/services/ingest.go` — Core: `Run()`, `processLedger()`, retry logic
- `internal/services/ingest_live.go` — Live: `startLiveIngestion()`, `PersistLedgerData()`
- `internal/services/ingest_backfill.go` — Backfill: `startBackfilling()`, parallel batch processing
- `internal/indexer/indexer.go` — Transaction processing, processor fan-out
- `internal/indexer/indexer_buffer.go` — Memory-efficient canonical pointer buffer

## Live Ingestion Flow

### Startup Decision Tree

Live ingestion acquires a PostgreSQL advisory lock (deterministic FNV hash of `"wallet-backend-ingest-<network>"`) to prevent concurrent instances, then determines how to initialize.

```mermaid
flowchart TD
    Start["startLiveIngestion()"] --> AcquireLock{"Acquire advisory lock<br/>(generateAdvisoryLockID)"}
    AcquireLock -->|Failed| Error["Return error:<br/>lock not acquired"]
    AcquireLock -->|Acquired| ReadCursor["Get latestLedgerCursor"]

    ReadCursor --> FirstRun{"cursor == 0?<br/>(first run)"}

    FirstRun -->|Yes| GetArchive["archive.GetLatestLedgerSequence()"]
    GetArchive --> PopulateTokens["PopulateAccountTokens()"]
    PopulateTokens --> InitCursors["Initialize both cursors<br/>(latest + oldest = startLedger)"]
    InitCursors --> SetMetrics["Set initial Prometheus<br/>metrics from startLedger"]
    SetMetrics --> PrepareRange

    FirstRun -->|No| LoadDB["Load oldest/latest cursors<br/>into Prometheus from DB"]
    LoadDB --> HealthCheck["rpcService.GetHealth()"]
    HealthCheck --> BehindCheck{"networkLatest - cursor<br/>≥ catchupThreshold?"}

    BehindCheck -->|Yes| OptCatchup["startBackfilling()<br/>BackfillModeCatchup<br/>[cursor+1, networkLatest]"]
    OptCatchup --> UpdateStart["startLedger =<br/>networkLatest + 1"]
    UpdateStart --> PrepareRange

    BehindCheck -->|No| PrepareRange["PrepareRange(unbounded<br/>from startLedger)"]
    PrepareRange --> IngestLoop["ingestLiveLedgers()"]

    style Start fill:#e1f5fe
    style Error fill:#ffebee
    style OptCatchup fill:#fff3e0
    style IngestLoop fill:#e8f5e9
```

### Per-Ledger Processing Loop

Once initialized, live ingestion enters an unbounded loop processing one ledger at a time:

```mermaid
flowchart LR
    Fetch["getLedgerWithRetry()<br/>from LedgerBackend"] --> Process["processLedger()<br/>via Indexer"]
    Process --> Persist["ingestProcessedDataWithRetry()<br/>→ PersistLedgerData()"]
    Persist --> Metrics["Record metrics:<br/>duration, tx count,<br/>op count, ledger count"]
    Metrics --> SyncCheck{"ledger %<br/>oldestLedgerSyncInterval<br/>== 0?"}
    SyncCheck -->|Yes| SyncOldest["Sync oldest cursor<br/>from DB to Prometheus"]
    SyncCheck -->|No| Next
    SyncOldest --> Next["currentLedger++"]
    Next --> Fetch
```

**Timing metrics recorded per ledger:**
- `process_ledger` — Time spent in Indexer processing
- `insert_into_db` — Time spent in DB transaction (PersistLedgerData)
- Total ingestion duration — End-to-end for the ledger

### PersistLedgerData Atomic Transaction

All data for a single ledger is persisted in one atomic database transaction. This ensures either all data for a ledger is committed or none of it is.

```mermaid
sequenceDiagram
    participant Caller as ingestProcessedDataWithRetry
    participant Persist as PersistLedgerData
    participant DB as PostgreSQL Transaction

    Caller->>Persist: ledgerSeq, buffer, cursorName
    Persist->>DB: BEGIN

    Note over DB: Step 1: Insert trustline assets
    Persist->>DB: TrustlineAsset.BatchInsert()<br/>(FK prerequisite for balances)

    Note over DB: Step 2: Insert contract tokens
    Persist->>DB: prepareNewContractTokens()<br/>→ Contract.BatchInsert()

    Note over DB: Step 3: Insert core data
    Persist->>DB: insertIntoDB()<br/>→ transactions + operations + state_changes

    Note over DB: Step 4: Unlock channel accounts
    Persist->>DB: UnassignTxAndUnlockChannelAccounts()<br/>(no-op if chAccStore is nil)

    Note over DB: Step 5: Process token changes
    Persist->>DB: ProcessTokenChanges()<br/>(trustline, contract, account, SAC)

    Note over DB: Step 6: Update cursor
    Persist->>DB: IngestStore.Update()<br/>(cursorName = ledgerSeq)

    Persist->>DB: COMMIT
    DB-->>Caller: numTxs, numOps
```

**Key design decisions:**
- **Advisory lock**: Uses `pg_try_advisory_lock` with a deterministic FNV-64a hash of `"wallet-backend-ingest-<network>"`. Different networks (testnet, pubnet) get separate locks.
- **First-run initialization**: On first run, starts from the latest history archive ledger rather than genesis. `PopulateAccountTokens()` snapshots current token holdings before ingestion begins.
- **Catchup threshold**: Configurable via `CatchupThreshold` (default: 100). If the service restarts and is more than this many ledgers behind the network tip, it uses parallel backfill instead of sequential processing.
- **Oldest ledger sync**: Every `oldestLedgerSyncInterval` (100) ledgers, the live process reads the `oldestLedgerCursor` from DB to pick up progress from concurrent backfill jobs.

## Backfill Ingestion Flow

### Mode Selection and Gap Calculation

Backfill operates in two modes with different gap calculation strategies:

```mermaid
flowchart TD
    Start["startBackfilling()<br/>(startLedger, endLedger, mode)"]
    Start --> Validate{"mode?"}

    Validate -->|Historical| HistValid["Validate: endLedger ≤<br/>latestIngestedLedger"]
    Validate -->|Catchup| CatchValid["Validate: startLedger =<br/>latestIngestedLedger + 1"]

    HistValid --> GapCalc
    CatchValid --> SingleGap["Treat entire range<br/>as one gap"]
    SingleGap --> SplitBatches

    GapCalc["calculateBackfillGaps()"]
    GapCalc --> OldestCheck{"startLedger vs<br/>oldestIngestedLedger?"}

    OldestCheck -->|"end ≤ oldest"| Case1["Gap: start → min(end, oldest-1)<br/>(entirely before existing data)"]
    OldestCheck -->|"start < oldest"| Case2["Gap: start → oldest-1<br/>+ internal gaps from GetLedgerGaps()"]
    OldestCheck -->|"start ≥ oldest"| Case3["Internal gaps only<br/>from GetLedgerGaps()"]

    Case1 --> SplitBatches
    Case2 --> SplitBatches
    Case3 --> SplitBatches

    SplitBatches["splitGapsIntoBatches()<br/>(backfillBatchSize per batch)"]
    SplitBatches --> NoGaps{"gaps empty?"}
    NoGaps -->|Yes| Done["Return: nothing to backfill"]
    NoGaps -->|No| Process["processBackfillBatchesParallel()"]

    style Start fill:#fff3e0
    style GapCalc fill:#e8f5e9
    style Process fill:#e1f5fe
```

**Gap detection SQL** (`GetLedgerGaps`):
```sql
SELECT gap_start, gap_end FROM (
    SELECT
        ledger_number + 1 AS gap_start,
        LEAD(ledger_number) OVER (ORDER BY ledger_number) - 1 AS gap_end
    FROM (SELECT DISTINCT ledger_number FROM transactions) t
) gaps
WHERE gap_start <= gap_end
ORDER BY gap_start
```

### Single Batch Processing Pipeline

Each batch is processed by a dedicated goroutine with its own `LedgerBackend` instance (backends are not thread-safe):

```mermaid
flowchart LR
    Setup["setupBatchBackend()<br/>LedgerBackendFactory()<br/>→ PrepareRange(bounded)"]
    Setup --> Loop["For each ledger<br/>in batch range"]
    Loop --> Fetch["getLedgerWithRetry()"]
    Fetch --> Process["processLedger()<br/>→ Indexer"]
    Process --> BufferCheck{"ledgersInBuffer ≥<br/>backfillDBInsertBatchSize?"}
    BufferCheck -->|Yes| Flush["flushBatchBufferWithRetry()<br/>(no cursor update)"]
    Flush --> Clear["buffer.Clear()"]
    Clear --> Loop
    BufferCheck -->|No| Loop

    Loop -->|batch complete| Final{"Remaining data<br/>in buffer?"}
    Final -->|Yes, Historical| FlushCursor["flushBatchBufferWithRetry()<br/>+ UpdateMin(oldestCursor)"]
    Final -->|Yes, Catchup| FlushNoCursor["flushBatchBufferWithRetry()<br/>(return BatchChanges)"]
    Final -->|No, Historical| CursorOnly["updateOldestCursor()"]
```

### Post-Backfill Processing

After all parallel batches complete, the post-processing diverges based on mode:

```mermaid
flowchart TD
    BatchesDone["All batches complete"]
    BatchesDone --> Analyze["analyzeBatchResults()<br/>Count failures"]

    Analyze --> ModeCheck{"mode?"}

    ModeCheck -->|Historical| Recompress["progressiveRecompressor.Wait()<br/>Compress remaining chunks"]
    Recompress --> HistDone["Done: oldest cursor<br/>updated per-batch"]

    ModeCheck -->|Catchup| FailCheck{"Any batches<br/>failed?"}
    FailCheck -->|Yes| CatchupError["Return error:<br/>catchup failed"]
    FailCheck -->|No| MergeAll["Merge all BatchChanges<br/>(trustline, account, contract,<br/>SAC balance changes)"]

    MergeAll --> AtomicTx["Single DB transaction:"]
    AtomicTx --> Step1["1. processBatchChanges()<br/>(assets, contracts, token changes)"]
    Step1 --> Step2["2. Update latestLedgerCursor<br/>to endLedger"]
    Step2 --> CatchupDone["Done: cursor jump-forward"]

    style HistDone fill:#e8f5e9
    style CatchupDone fill:#e8f5e9
    style CatchupError fill:#ffebee
```

**Key design details:**

- **Worker pool**: Uses `pond.Pool` with configurable size (`BackfillWorkers`, default: `runtime.NumCPU()`). Each goroutine gets its own `LedgerBackend` via `LedgerBackendFactory`.
- **Batch sizing**: `backfillBatchSize` (default: 250) controls how many ledgers per batch. `backfillDBInsertBatchSize` (default: 50) controls periodic DB flushes within a batch to bound memory.
- **Merge logic**: Catchup mode collects `BatchChanges` from each batch and merges them. Merge uses highest-OperationID-wins semantics with ADD→REMOVE no-op detection (if a trustline/account is created and removed within the same range, the net effect is nothing).
- **Progressive recompression**: Historical backfill inserts data uncompressed (faster writes), then the `progressiveRecompressor` compresses TimescaleDB chunks as contiguous batches complete. It uses a watermark — batch 0 must complete before any compression starts, then chunks are compressed as the watermark advances. A final verification pass catches boundary chunks.

## Indexer Architecture

### Processor Fan-Out

The Indexer processes all transactions in a ledger in parallel using a `pond.Pool`. Each transaction is processed independently, producing a per-transaction `IndexerBuffer` that is merged into the ledger-level buffer after all transactions complete.

```mermaid
flowchart TD
    LedgerMeta["LedgerCloseMeta"]
    LedgerMeta --> Extract["GetLedgerTransactions()<br/>Read all transactions"]
    Extract --> Pool["pond.Pool: parallel transaction processing"]

    Pool --> TX1["processTransaction(tx₁)"]
    Pool --> TX2["processTransaction(tx₂)"]
    Pool --> TXN["processTransaction(txₙ)"]

    TX1 --> Buf1["IndexerBuffer₁"]
    TX2 --> Buf2["IndexerBuffer₂"]
    TXN --> BufN["IndexerBufferₙ"]

    subgraph "Per-Transaction Processing"
        direction TB
        PP["ParticipantsProcessor<br/>→ tx + op participants"]
        TTP["TokenTransferProcessor<br/>→ balance state changes"]
        EP["EffectsProcessor<br/>→ effect state changes"]
        CDP["ContractDeployProcessor<br/>→ deploy state changes"]
        SACP["SACEventsProcessor<br/>→ SAC event state changes"]
        TP["TrustlinesProcessor<br/>→ trustline add/update/remove"]
        AP["AccountsProcessor<br/>→ account create/update/remove"]
        SBP["SACBalancesProcessor<br/>→ SAC balance add/update/remove"]
        SIP["SACInstanceProcessor<br/>→ SAC contract metadata"]
    end

    TX1 -.-> PP
    TX1 -.-> TTP
    TX1 -.-> EP
    TX1 -.-> CDP
    TX1 -.-> SACP
    TX1 -.-> TP
    TX1 -.-> AP
    TX1 -.-> SBP
    TX1 -.-> SIP

    Buf1 --> Merge["ledgerBuffer.Merge()"]
    Buf2 --> Merge
    BufN --> Merge
    Merge --> LedgerBuffer["Ledger-level IndexerBuffer"]

    style Pool fill:#e1f5fe
    style LedgerBuffer fill:#e8f5e9
```

### Processor Reference

| Processor | Interface | Level | Output | Description |
|-----------|-----------|-------|--------|-------------|
| `ParticipantsProcessor` | `ParticipantsProcessorInterface` | Transaction | Participant sets | Extracts Stellar addresses involved in each transaction and operation |
| `TokenTransferProcessor` | `TokenTransferProcessorInterface` | Transaction | `[]StateChange` | Produces balance-category state changes from token transfer events |
| `EffectsProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` | Extracts effects (payments, trades, etc.) as state changes |
| `ContractDeployProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` | Detects Soroban contract deployments |
| `SACEventsProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` | Processes Stellar Asset Contract events |
| `TrustlinesProcessor` | `LedgerChangeProcessor[TrustlineChange]` | Operation | `[]TrustlineChange` | Tracks trustline creation, update, and removal from ledger changes |
| `AccountsProcessor` | `LedgerChangeProcessor[AccountChange]` | Operation | `[]AccountChange` | Tracks account creation, update, and removal from ledger changes |
| `SACBalancesProcessor` | `LedgerChangeProcessor[SACBalanceChange]` | Operation | `[]SACBalanceChange` | Tracks SAC balance add/update/remove from ledger changes |
| `SACInstanceProcessor` | `LedgerChangeProcessor[*data.Contract]` | Operation | `[]*data.Contract` | Extracts SAC contract metadata from contract instance entries |

**Parallelism model**: Transactions within a ledger are processed in parallel (via `pond.Pool` with unbounded workers). Within each transaction, the 3 `OperationProcessorInterface` processors run sequentially per operation (creating a pool per-operation adds overhead with only 3 processors). The `LedgerChangeProcessor` processors also run sequentially per operation.

For comprehensive documentation of the state change subsystem — the category/reason taxonomy, producing processors, builder pattern, ordering, and schema — see [[references/state-changes]].

## IndexerBuffer Architecture

### Two-Layer Memory Architecture

The `IndexerBuffer` uses a canonical pointer pattern to avoid duplicating large XDR-heavy structs when multiple participants reference the same transaction or operation.

```mermaid
flowchart LR
    subgraph "Layer 1: Canonical Storage"
        TxByHash["txByHash<br/>map[hash]*Transaction<br/>(one pointer per unique tx)"]
        OpByID["opByID<br/>map[id]*Operation<br/>(one pointer per unique op)"]
    end

    subgraph "Layer 2: Participant Mappings"
        PByTx["participantsByToID<br/>map[toID] → Set[string]<br/>(stellar addresses per tx)"]
        PByOp["participantsByOpID<br/>map[opID] → Set[string]<br/>(stellar addresses per op)"]
    end

    subgraph "Dedup Maps (last-write-wins by OperationID)"
        TLChanges["trustlineChangesByTrustlineKey<br/>map[(accountID, trustlineID)]<br/>→ TrustlineChange"]
        AccChanges["accountChangesByAccountID<br/>map[accountID]<br/>→ AccountChange"]
        SACChanges["sacBalanceChangesByKey<br/>map[(accountID, contractID)]<br/>→ SACBalanceChange"]
    end

    subgraph "Append-Only Slices"
        SC["stateChanges<br/>[]StateChange"]
        CC["contractChanges<br/>[]ContractChange"]
    end

    subgraph "Unique Token Tracking"
        Assets["uniqueTrustlineAssets<br/>map[uuid]TrustlineAsset"]
        Contracts["uniqueSEP41ContractTokensByID<br/>map[contractID]ContractType"]
        SAC["sacContractsByID<br/>map[contractID]*Contract"]
    end

    TxByHash -.->|"referenced by"| PByTx
    OpByID -.->|"referenced by"| PByOp
```

**Design rationale:**
- **Canonical pointers**: Transaction structs contain large XDR fields (10-50+ KB each). When multiple participants interact with the same transaction, they all point to the SAME canonical pointer in `txByHash` instead of storing duplicate copies.
- **Thread safety**: All public methods use `sync.RWMutex`. Per-transaction buffers are created independently in parallel goroutines, then merged into the ledger buffer sequentially.
- **Deduplication strategy**: Dedup maps use highest-OperationID-wins semantics. ADD→REMOVE pairs within the same buffer are detected and deleted (net no-op). This applies to trustlines, accounts, and SAC balances.
- **Clear for reuse**: The `Clear()` method resets all maps and slices but preserves allocated backing arrays, avoiding GC pressure during backfill batch processing.

## Retry Logic & Cursor Management

### Retry State Machine

The pipeline has two retry loops with exponential backoff, used at different stages:

```mermaid
stateDiagram-v2
    state "Ledger Fetch Retry" as FetchRetry {
        [*] --> FetchAttempt
        FetchAttempt --> FetchSuccess: backend.GetLedger() OK
        FetchAttempt --> FetchBackoff: error
        FetchBackoff --> FetchAttempt: backoff elapsed<br/>(1s, 2s, 4s, ... max 30s)
        FetchBackoff --> FetchFailed: ctx.Done()
        FetchAttempt --> FetchFailed: attempt ≥ 10
        FetchSuccess --> [*]
        FetchFailed --> [*]

        note left of FetchAttempt
            maxLedgerFetchRetries = 10
            maxRetryBackoff = 30s
            Location: ingest.go
        end note
    }

    state "Persist Data Retry" as PersistRetry {
        [*] --> PersistAttempt
        PersistAttempt --> PersistSuccess: PersistLedgerData() OK
        PersistAttempt --> PersistBackoff: error
        PersistBackoff --> PersistAttempt: backoff elapsed<br/>(1s, 2s, 4s, ... max 10s)
        PersistBackoff --> PersistFailed: ctx.Done()
        PersistAttempt --> PersistFailed: attempt ≥ 5
        PersistSuccess --> [*]
        PersistFailed --> [*]

        note left of PersistAttempt
            maxIngestProcessedDataRetries = 5
            maxIngestProcessedDataRetryBackoff = 10s
            Location: ingest_live.go
        end note
    }
```

### Retry Constants

| Constant | Value | Location | Used By |
|----------|-------|----------|---------|
| `maxLedgerFetchRetries` | 10 | `ingest.go` | `getLedgerWithRetry()` — both live and backfill |
| `maxRetryBackoff` | 30s | `ingest.go` | `getLedgerWithRetry()` — max backoff cap |
| `maxIngestProcessedDataRetries` | 5 | `ingest_live.go` | `ingestProcessedDataWithRetry()` and `flushBatchBufferWithRetry()` |
| `maxIngestProcessedDataRetryBackoff` | 10s | `ingest_live.go` | Max backoff cap for persist retries |
| `oldestLedgerSyncInterval` | 100 | `ingest_live.go` | Sync oldest cursor metric every N ledgers |

### Cursor Operations

The `IngestStoreModel` manages cursor state in the `ingest_store` key-value table:

| Method | SQL Pattern | Used By |
|--------|-------------|---------|
| `Get(cursorName)` | `SELECT value FROM ingest_store WHERE key = $1` | Startup: check if first run; load cursor positions |
| `Update(cursorName, ledger)` | `INSERT ... ON CONFLICT DO UPDATE SET value = $2` | Live: advance latest cursor after each ledger |
| `UpdateMin(cursorName, ledger)` | `UPDATE ... SET value = LEAST(value::integer, $2)` | Backfill: move oldest cursor backward (only if new value is lower) |
| `GetLedgerGaps()` | Window function on `transactions.ledger_number` | Historical backfill: find gaps in ingested data |

### Crash Recovery

- **Live ingestion**: On restart, reads `latestLedgerCursor` and resumes from `cursor + 1`. The advisory lock prevents concurrent instances. If a crash occurs mid-ledger, no data is committed (atomic transaction), so the same ledger is simply re-processed.
- **Backfill**: Each batch atomically updates the `oldestCursor` with its final flush. Completed batches are durable. Incomplete batches are simply re-processed on next backfill run (gap detection will find them).
- **Catchup**: If any batch fails, the entire catchup fails and the `latestLedgerCursor` is NOT advanced. The next restart will attempt catchup again for the same range.

## Ledger Backend

### Backend Selection and Factory Pattern

The pipeline supports two backend types for fetching ledger data, selected via the `LedgerBackendType` config:

```mermaid
flowchart LR
    Config["Configs.LedgerBackendType"]
    Config -->|rpc| RPC["newRPCLedgerBackend()"]
    Config -->|datastore| DS["newDatastoreLedgerBackend()"]

    RPC --> RPCBackend["RPCLedgerBackend<br/>• URL: Configs.RPCURL<br/>• BufferSize: GetLedgersLimit<br/>• Real-time ledger streaming"]

    DS --> DSBackend["BufferedStorageBackend<br/>• TOML config file<br/>• Cloud storage (S3/GCS)<br/>• Schema-aware ledger reading"]

    subgraph "Backfill: Factory Pattern"
        Factory["LedgerBackendFactory<br/>func(ctx) → LedgerBackend"]
        Factory --> Instance1["Backend instance (batch 1)"]
        Factory --> Instance2["Backend instance (batch 2)"]
        Factory --> InstanceN["Backend instance (batch N)"]
    end

    note["Each backfill batch creates its own<br/>backend via LedgerBackendFactory<br/>because LedgerBackend is NOT thread-safe"]

    style Factory fill:#fff3e0
    style note fill:#fffde7
```

**LedgerBackend interface** (from `go-stellar-sdk/ingest/ledgerbackend`):
- `PrepareRange(ctx, Range)` — Initialize the backend for a bounded or unbounded ledger range
- `GetLedger(ctx, seq)` — Fetch a single ledger's `LedgerCloseMeta`
- `Close()` — Release resources

**Backend details:**

| Backend | Config | Use Case |
|---------|--------|----------|
| **RPC** (`rpc`) | `RPCURL`, `GetLedgersLimit` (buffer size) | Live ingestion (real-time streaming from Stellar RPC) |
| **Datastore** (`datastore`) | `DatastoreConfigPath` (TOML file) | Backfill from cloud storage (S3/GCS pre-exported ledgers) |

**Thread-safety constraint**: `LedgerBackend` instances are NOT thread-safe. Live ingestion uses a single instance for sequential processing. Backfill creates a new instance per batch via `LedgerBackendFactory`, which calls `NewLedgerBackend()` for each batch. This is why the factory pattern exists — it allows each goroutine in the `pond.Pool` to have its own isolated backend.


---

## Adding a New Processor

To add a new data processor to the indexer fan-out:

1. Create the processor in `internal/indexer/processors/`
2. Implement the processor interface (`OperationProcessorInterface`, `LedgerChangeProcessor[T]`, or `TokenTransferProcessorInterface` depending on data type)
3. Register it in the indexer's processor list (`internal/indexer/indexer.go`)
4. Create the corresponding DB model and migration in `internal/db/migrations/`
5. Add GraphQL schema + resolvers if the data needs to be queryable

---

**Topics:** [[entries/index]] | [[entries/ingestion]]
