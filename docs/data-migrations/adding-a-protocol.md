# Adding a New Protocol

This guide walks through everything needed to add a new protocol to wallet-backend's data migration system. A "protocol" is any Soroban smart contract standard (e.g., tokens, lending pools, AMMs) that wallet-backend can classify, track, and produce state for.

For background on the architecture, cursors, CAS convergence, and the classification pipeline, see the [Data Migrations Design Document](../feature-design/data-migrations.md).

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Step 1: Register the Protocol in the Database](#step-1-register-the-protocol-in-the-database)
- [Step 2: Create a Protocol Validator](#step-2-create-a-protocol-validator)
- [Step 3: Create the State Table and Data Model](#step-3-create-the-state-table-and-data-model)
- [Step 4: Create a Protocol Processor](#step-4-create-a-protocol-processor)
- [Step 5: Register the Validator and Processor](#step-5-register-the-validator-and-processor)
- [Step 6: Wire the Data Model into Models](#step-6-wire-the-data-model-into-models)
- [Step 7: Run the Migration Workflow](#step-7-run-the-migration-workflow)
- [Testing](#testing)
- [File Checklist](#file-checklist)

## Overview

Adding a protocol involves six code-level steps and one operational step:

```
Code changes:
  1. Protocol migration SQL       (register protocol ID in the database)
  2. Validator                    (classify contracts by WASM spec)
  3. State table + data model     (store protocol-specific state, optional and can be skipped if state tracking is not needed)
  4. Processor                    (extract state from ledgers)
  5. Registration file            (wire validator + processor into global registries)
  6. Models struct                (wire data model for dependency injection)

Operational:
  7. Run the 4-step migration workflow (setup -> live ingest + history + current-state)
```

## Prerequisites

- Familiarity with the Soroban contract model (WASM bytecode, `ScSpecEntry`, contract data entries)
- A clear definition of your protocol's required function signatures (used for classification)
- Understanding of what state your protocol produces (balances, positions, orders, etc.)

## Step 1: Register the Protocol in the Database

Create a protocol migration SQL file that inserts a row into the `protocols` table. This is the identity of your protocol in the system.

**File:** `internal/db/migrations/protocols/NNN_<protocol-name>.sql`

Use the next available sequence number. The file is executed by the `protocol-setup` command.

```sql
INSERT INTO protocols (id) VALUES ('<PROTOCOL_ID>') ON CONFLICT (id) DO NOTHING;
```

The protocol ID is a string primary key (e.g., `BLEND`, `AQUA`). It must match exactly across all code references -- the validator, processor, and registration file must all use the same string.

## Step 2: Create a Protocol Validator

A validator tells the system how to identify contracts that implement your protocol. It inspects the WASM bytecode's `contractspecv0` section and checks for required function signatures.

**File:** `internal/services/<protocol>_validator.go`

Implement the `ProtocolValidator` interface defined in `internal/services/protocol_validator.go`:

```go
type ProtocolValidator interface {
    ProtocolID() string
    Validate(specEntries []xdr.ScSpecEntry) bool
}
```

Your validator receives the parsed `ScSpecEntry` slice from a contract's WASM bytecode and returns `true` if the contract implements your protocol.
In cases where the protocol is made up of only a known set of contracts, the validator can simply match on those contract's IDs.

### Writing the Validate Method

1. Define the required function signatures for your protocol -- each with a function name, input parameter names and types, and output types.
2. Iterate through the `ScSpecEntry` slice and filter for `ScSpecEntryKindScSpecEntryFunctionV0` entries.
3. For each function entry, compare its signature against your requirements.
4. Return `true` only if **all** required functions are present with matching signatures.

```go
package services

import (
    "github.com/stellar/go-stellar-sdk/xdr"
)

type MyProtocolValidator struct{}

func NewMyProtocolValidator() *MyProtocolValidator { return &MyProtocolValidator{} }

func (v *MyProtocolValidator) ProtocolID() string { return "MY_PROTOCOL" }

func (v *MyProtocolValidator) Validate(contractSpec []xdr.ScSpecEntry) bool {
    // Define required functions for your protocol
    required := map[string]bool{
        "deposit":  false,
        "withdraw": false,
        "balance":  false,
    }

    for _, spec := range contractSpec {
        if spec.Kind != xdr.ScSpecEntryKindScSpecEntryFunctionV0 || spec.FunctionV0 == nil {
            continue
        }

        funcName := string(spec.FunctionV0.Name)
        if _, isRequired := required[funcName]; isRequired {
            // Validate input/output types match your protocol spec.
            // You should check both parameter names and types exactly.
            if validateMyProtocolSignature(spec.FunctionV0, funcName) {
                required[funcName] = true
            }
        }
    }

    for _, found := range required {
        if !found {
            return false
        }
    }
    return true
}
```

The WASM spec extraction pipeline (compile with wazero, extract `contractspecv0` custom section, unmarshal XDR `ScSpecEntry` items) is handled upstream by the system. Your validator only receives the parsed entries. See the [WASM Classification Pipeline](../feature-design/data-migrations.md) section in the design document for details.

## Step 3: Create the State Table and Data Model

Your protocol needs a database table to store its state and a Go model to interact with it.

### Database Migration

**File:** `internal/db/migrations/<date>.<sequence>-<protocol>-<description>.sql`

Follow the existing naming convention: `YYYY-MM-DD.<seq>-<descriptive-name>.sql`.

Design the schema around whatever state your protocol produces. The table structure is entirely protocol-specific -- some protocols may track per-account state, others may track pool reserves, order books, or aggregate metrics. The only common pattern is `last_modified_ledger` for tracking which ledger last wrote a row.

```sql
-- +migrate Up

CREATE TABLE <protocol>_<entity> (
    -- Define columns based on your protocol's state shape.
    -- For example, a lending protocol might have:
    --   pool_id BYTEA NOT NULL,
    --   user_address BYTEA NOT NULL,
    --   collateral TEXT NOT NULL DEFAULT '0',
    --   debt TEXT NOT NULL DEFAULT '0',
    -- While an AMM might have:
    --   pool_id BYTEA NOT NULL,
    --   reserve_a TEXT NOT NULL DEFAULT '0',
    --   reserve_b TEXT NOT NULL DEFAULT '0',

    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (...)  -- Choose a primary key that fits your state's natural identity
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);

-- Add indexes based on your query patterns

-- +migrate Down

DROP TABLE IF EXISTS <protocol>_<entity>;
```

The storage parameters (`fillfactor = 80`, aggressive autovacuum) are tuned for heavy UPSERT workloads during ingestion. Adjust based on your protocol's write pattern.

### Data Model

**File:** `internal/data/<protocol>_<entity>.go`

Define a state struct, a model interface, and a concrete implementation. The struct fields should mirror your table columns. The model interface is what the processor depends on, enabling test stubs.

```go
package data

import (
    "context"
    "fmt"
    "time"

    "github.com/jackc/pgx/v5"

    "github.com/stellar/wallet-backend/internal/db"
    "github.com/stellar/wallet-backend/internal/metrics"
)

// State struct -- represents a single row in your protocol's table.
// Fields should match your table schema.
type MyProtocolEntry struct {
    // Define fields matching your table columns.
    // For example, a lending protocol entry might have:
    //   PoolID             []byte
    //   UserAddress        []byte
    //   Collateral         string
    //   Debt               string
    LastModifiedLedger uint32
}

// Model interface -- defines the operations your processor needs.
// BatchUpsert and GetAll are the minimum required for the processor pattern.
// Add additional query methods as needed for your protocol's read paths.
type MyProtocolEntryModelInterface interface {
    BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []MyProtocolEntry, deletes []MyProtocolEntry) error
    GetAll(ctx context.Context, dbTx pgx.Tx) ([]MyProtocolEntry, error)
}

// Concrete implementation.
type MyProtocolEntryModel struct {
    DB             db.ConnectionPool
    MetricsService metrics.MetricsService
}

var _ MyProtocolEntryModelInterface = (*MyProtocolEntryModel)(nil)

func (m *MyProtocolEntryModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []MyProtocolEntry, deletes []MyProtocolEntry) error {
    if len(upserts) == 0 && len(deletes) == 0 {
        return nil
    }

    start := time.Now()
    batch := &pgx.Batch{}

    // Write upsert and delete queries matching your table schema.
    // Use ON CONFLICT ... DO UPDATE for upserts keyed on your primary key.

    for _, e := range upserts {
        batch.Queue(`INSERT INTO ... VALUES (...) ON CONFLICT (...) DO UPDATE SET ...`, /* fields from e */)
    }

    for _, e := range deletes {
        batch.Queue(`DELETE FROM ... WHERE ...`, /* primary key fields from e */)
    }

    br := dbTx.SendBatch(ctx, batch)
    for i := 0; i < batch.Len(); i++ {
        if _, err := br.Exec(); err != nil {
            _ = br.Close()
            return fmt.Errorf("batch upserting: %w", err)
        }
    }
    if err := br.Close(); err != nil {
        return fmt.Errorf("closing batch: %w", err)
    }

    m.MetricsService.ObserveDBQueryDuration("BatchUpsert", "my_protocol_entries", time.Since(start).Seconds())
    m.MetricsService.IncDBQuery("BatchUpsert", "my_protocol_entries")
    return nil
}

func (m *MyProtocolEntryModel) GetAll(ctx context.Context, dbTx pgx.Tx) ([]MyProtocolEntry, error) {
    start := time.Now()

    rows, err := dbTx.Query(ctx, `SELECT ... FROM my_protocol_entries`)
    if err != nil {
        return nil, fmt.Errorf("querying entries: %w", err)
    }
    defer rows.Close()

    var entries []MyProtocolEntry
    for rows.Next() {
        var e MyProtocolEntry
        if err := rows.Scan(/* fields matching SELECT columns */); err != nil {
            return nil, fmt.Errorf("scanning entry: %w", err)
        }
        entries = append(entries, e)
    }

    m.MetricsService.ObserveDBQueryDuration("GetAll", "my_protocol_entries", time.Since(start).Seconds())
    m.MetricsService.IncDBQuery("GetAll", "my_protocol_entries")
    return entries, nil
}
```

## Step 4: Create a Protocol Processor

The processor is the core component that extracts protocol-specific state from each ledger. It implements the `ProtocolProcessor` interface and optionally the `protocolProcessorModelBinder` interface for data model injection.

**File:** `internal/services/<protocol>_processor.go`

### The Interface

```go
type ProtocolProcessor interface {
    ProtocolID() string
    ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error
    PersistHistory(ctx context.Context, dbTx pgx.Tx) error
    PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error
    LoadCurrentState(ctx context.Context, dbTx pgx.Tx) error
}

type ProtocolProcessorInput struct {
    LedgerSequence    uint32
    LedgerCloseMeta   xdr.LedgerCloseMeta
    ProtocolContracts []data.ProtocolContracts
    NetworkPassphrase string
}
```

### Method Responsibilities

| Method | Called By | Purpose |
|--------|-----------|---------|
| `ProcessLedger` | Live ingestion, history migration, current-state migration | Extract state changes from a single ledger. Buffer results in memory. |
| `PersistHistory` | Live ingestion, history migration | Write buffered historical state change records to DB within a transaction. |
| `PersistCurrentState` | Live ingestion, current-state migration | Write buffered current state to DB within a transaction. Update in-memory state. |
| `LoadCurrentState` | Live ingestion (on migration handoff) | Load all current state from DB into processor memory. Called once when migration hands off to live ingestion via CAS. |

### Model Binding

If your processor needs a data model (almost always), implement `bindModels`:

```go
type protocolProcessorModelBinder interface {
    bindModels(models *data.Models) error
}
```

Models are bound automatically during `IngestService` initialization. If `bindModels` returns an error, the service fails to start with a descriptive message.

### Processor Structure

Follow this pattern for your processor:

```go
package services

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5"
    "github.com/stellar/go-stellar-sdk/support/log"

    "github.com/stellar/wallet-backend/internal/data"
)

type MyProtocolProcessor struct {
    // In-memory current state. Populated by LoadCurrentState, maintained by PersistCurrentState.
    currentState map[string]string
    entryModel   data.MyProtocolEntryModelInterface

    // Per-ledger buffers, cleared after each persist call.
    pendingUpserts []data.MyProtocolEntry
    pendingDeletes []data.MyProtocolEntry
}

func NewMyProtocolProcessor() *MyProtocolProcessor {
    return &MyProtocolProcessor{
        currentState: make(map[string]string),
    }
}

var _ ProtocolProcessor = (*MyProtocolProcessor)(nil)

func (p *MyProtocolProcessor) ProtocolID() string { return "MY_PROTOCOL" }

func (p *MyProtocolProcessor) bindModels(models *data.Models) error {
    if models == nil {
        return fmt.Errorf("data models not configured")
    }
    if models.MyProtocolEntry == nil {
        return fmt.Errorf("my_protocol entry model not configured")
    }
    p.entryModel = models.MyProtocolEntry
    return nil
}

func (p *MyProtocolProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
    // 1. Clear per-ledger buffers
    p.pendingUpserts = p.pendingUpserts[:0]
    p.pendingDeletes = p.pendingDeletes[:0]

    // 2. Early return if no contracts to track
    if len(input.ProtocolContracts) == 0 {
        return nil
    }

    // 3. Build set of tracked contract IDs from input
    trackedContracts := make(map[string]struct{}, len(input.ProtocolContracts))
    for _, pc := range input.ProtocolContracts {
        trackedContracts[string(pc.ContractID)] = struct{}{}
    }

    // 4. Read transactions from the ledger using ingest.NewLedgerTransactionReaderFromLedgerCloseMeta
    // 5. For each successful transaction, get changes
    // 6. Filter for ContractData changes belonging to tracked contracts
    // 7. Parse protocol-specific data from the contract entry
    // 8. Append to pendingUpserts or pendingDeletes

    return nil
}

func (p *MyProtocolProcessor) PersistHistory(_ context.Context, _ pgx.Tx) error {
    // Write historical state change records if your protocol tracks history.
    // Return nil if not applicable.
    return nil
}

func (p *MyProtocolProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
    if len(p.pendingUpserts) == 0 && len(p.pendingDeletes) == 0 {
        return nil
    }
    if err := p.entryModel.BatchUpsert(ctx, dbTx, p.pendingUpserts, p.pendingDeletes); err != nil {
        return fmt.Errorf("persisting current state: %w", err)
    }

    // Update in-memory state from buffers.
    // Build a map key from your entry's primary key fields.
    for _, e := range p.pendingUpserts {
        key := buildKey(e) // e.g., hex.EncodeToString(e.PoolID) + ":" + hex.EncodeToString(e.UserAddress)
        p.currentState[key] = "..." // store whatever value(s) you need in memory
    }
    for _, e := range p.pendingDeletes {
        key := buildKey(e)
        delete(p.currentState, key)
    }

    log.Ctx(ctx).Debugf("MY_PROTOCOL: persisted %d upserts, %d deletes", len(p.pendingUpserts), len(p.pendingDeletes))

    // Clear buffers
    p.pendingUpserts = p.pendingUpserts[:0]
    p.pendingDeletes = p.pendingDeletes[:0]
    return nil
}

func (p *MyProtocolProcessor) LoadCurrentState(ctx context.Context, dbTx pgx.Tx) error {
    entries, err := p.entryModel.GetAll(ctx, dbTx)
    if err != nil {
        return fmt.Errorf("loading current state: %w", err)
    }

    p.currentState = make(map[string]string, len(entries))
    for _, e := range entries {
        key := buildKey(e)
        p.currentState[key] = "..." // mirror the value stored in PersistCurrentState
    }

    log.Ctx(ctx).Infof("MY_PROTOCOL: loaded %d entries into current state", len(entries))
    return nil
}
```

### Key Patterns

- **Per-ledger buffering**: `ProcessLedger` clears and refills `pendingUpserts`/`pendingDeletes` each ledger. Buffers are cleared after each `Persist*` call.
- **In-memory current state**: The `currentState` map is loaded once via `LoadCurrentState` (on migration-to-live handoff) and maintained incrementally by `PersistCurrentState`.
- **Contract filtering**: Build a set of tracked contract IDs from `input.ProtocolContracts` and skip unrelated ledger entries.
- **Graceful no-ops**: Methods return `nil` early when there's nothing to do (no pending changes, no tracked contracts).
- **Transaction safety**: `PersistHistory` and `PersistCurrentState` receive a `pgx.Tx` and all writes happen within that transaction. The CAS cursor advance also happens in the same transaction, ensuring atomicity.

## Step 5: Register the Validator and Processor

Create a registration file that wires your validator and processor into the global registries via `init()`. This is how the system discovers your protocol at runtime.

**File:** `internal/services/<protocol>_register.go`

```go
package services

func init() {
    RegisterValidator("MY_PROTOCOL", NewMyProtocolValidator())
    RegisterProcessor("MY_PROTOCOL", func() ProtocolProcessor {
        return NewMyProtocolProcessor()
    })
}
```

The `init()` function runs at program startup. The processor factory (closure) creates a fresh processor instance each time one is needed.

The `RegisterValidator` and `RegisterProcessor` functions are defined in `internal/services/validator_registry.go` and `internal/services/processor_registry.go` respectively. Both are thread-safe global registries.

## Step 6: Wire the Data Model into Models

Add your data model to the `Models` struct in `internal/data/models.go` and initialize it in `NewModels`. This enables the system to inject it into your processor via `bindModels`.

Add the field to the struct:

```go
type Models struct {
    // ... existing fields ...
    MyProtocolEntry MyProtocolEntryModelInterface
}
```

And initialize it in `NewModels`:

```go
func NewModels(db db.ConnectionPool, metricsService metrics.MetricsService) (*Models, error) {
    // ...
    return &Models{
        // ... existing initializations ...
        MyProtocolEntry: &MyProtocolEntryModel{DB: db, MetricsService: metricsService},
    }, nil
}
```

## Step 7: Run the Migration Workflow

For full command reference with all flags and options, see [Running a Data Migration](./running-a-data-migration.md).

Once the code is deployed, run the 4-step protocol onboarding workflow. Steps 2, 3a, and 3b run concurrently.

```
  Step 1: Setup             Steps 2, 3a, 3b: Run Concurrently
+--------------------+     +----------------------------------------------+
| protocol-setup     |---->| Live ingestion (restart with new processor)  |
| --protocol-id X    |     | protocol-migrate history --protocol-id X    |
| Classifies         |     | protocol-migrate current-state --protocol-  |
| existing contracts |     |   id X --start-ledger N                     |
+--------------------+     +----------------------------------------------+
```

### Step 7.1: Protocol Setup

Classifies existing contracts on the network against your protocol's validator:

```bash
./wallet-backend protocol-setup --protocol-id <PROTOCOL_ID>
```

This:
- Runs your protocol migration SQL (registers the protocol in the `protocols` table)
- Fetches all unclassified WASM bytecodes via RPC
- Validates each against your protocol's validator
- Populates `protocol_wasms` and `protocol_contracts`
- Sets `classification_status = success`
- Initializes both CAS cursors

### Step 7.2: Restart Live Ingestion

Restart the ingestion service. It picks up the new processor automatically via the registry:

```bash
./wallet-backend ingest
```

Live ingestion uses CAS cursors to coordinate with the migration subcommands. It only produces state for ledgers where the migration hasn't already written data.

### Step 7.3a: History Migration

Backfills historical state changes within the retention window:

```bash
./wallet-backend protocol-migrate history --protocol-id <PROTOCOL_ID>
```

Walks forward from the oldest ingestion cursor to the latest, calling `PersistHistory` at each ledger. Converges with live ingestion via the history CAS cursor. When the migration's CAS fails (cursor already advanced by live ingestion), it sets `history_migration_status = success` and exits.

### Step 7.3b: Current-State Migration(if your protocol tracks current-state data)

Builds current state from a specified start ledger forward to the tip:

```bash
./wallet-backend protocol-migrate current-state --protocol-id <PROTOCOL_ID> --start-ledger <LEDGER>
```

The `--start-ledger` should be set to the ledger where the first contract implementing your protocol was deployed. Converges with live ingestion via the current-state CAS cursor.

## Testing

### Unit Tests

Each component should have unit tests. At minimum:

**Validator tests** (`internal/services/<protocol>_validator_test.go`) -- Verify your validator correctly identifies conforming and non-conforming contracts:

```go
func TestMyProtocolValidator_ValidContract(t *testing.T) {
    v := NewMyProtocolValidator()

    // Build a ScSpecEntry slice with all required functions and correct signatures
    validSpec := buildValidSpec()
    assert.True(t, v.Validate(validSpec))
}

func TestMyProtocolValidator_MissingFunction(t *testing.T) {
    v := NewMyProtocolValidator()

    // Build a ScSpecEntry slice missing a required function
    incompleteSpec := buildIncompleteSpec()
    assert.False(t, v.Validate(incompleteSpec))
}

func TestMyProtocolValidator_WrongSignature(t *testing.T) {
    v := NewMyProtocolValidator()

    // Build a ScSpecEntry slice with a required function but wrong parameter types
    wrongSigSpec := buildWrongSignatureSpec()
    assert.False(t, v.Validate(wrongSigSpec))
}
```

**Processor tests** (`internal/services/<protocol>_processor_test.go`) -- Verify buffer management, persistence, and state loading. Use a stub implementation of your data model interface:

```go
type myEntryModelStub struct {
    lastUpserts []data.MyProtocolEntry
    lastDeletes []data.MyProtocolEntry
    getAllRows  []data.MyProtocolEntry
    batchErr    error
    getAllErr   error
}

var _ data.MyProtocolEntryModelInterface = (*myEntryModelStub)(nil)

func (s *myEntryModelStub) BatchUpsert(_ context.Context, _ pgx.Tx, upserts []data.MyProtocolEntry, deletes []data.MyProtocolEntry) error {
    s.lastUpserts = append([]data.MyProtocolEntry(nil), upserts...)
    s.lastDeletes = append([]data.MyProtocolEntry(nil), deletes...)
    return s.batchErr
}

func (s *myEntryModelStub) GetAll(_ context.Context, _ pgx.Tx) ([]data.MyProtocolEntry, error) {
    return append([]data.MyProtocolEntry(nil), s.getAllRows...), s.getAllErr
}
```

Test cases to cover:

- `PersistCurrentState` writes buffered upserts and deletes to the model
- `PersistCurrentState` updates in-memory `currentState` (adds upserts, removes deletes)
- `PersistCurrentState` clears pending buffers after persistence
- `PersistCurrentState` is a no-op when both buffers are empty
- `LoadCurrentState` populates the in-memory map from the model's `GetAll` result
- `bindModels` returns an error when models are nil or the required field is missing

See `internal/services/protocol_processor_binding_test.go` for examples of testing model binding separately.

**Model binding tests** -- Verify your processor correctly receives and validates its data model:

```go
func TestMyProtocolProcessor_BindModels_NilModels(t *testing.T) {
    processor := NewMyProtocolProcessor()
    err := processor.bindModels(nil)
    require.Error(t, err)
}

func TestMyProtocolProcessor_BindModels_MissingField(t *testing.T) {
    processor := NewMyProtocolProcessor()
    err := processor.bindModels(&data.Models{})
    require.Error(t, err)
}

func TestMyProtocolProcessor_BindModels_Success(t *testing.T) {
    processor := NewMyProtocolProcessor()
    err := processor.bindModels(&data.Models{
        MyProtocolEntry: &myEntryModelStub{},
    })
    require.NoError(t, err)
}
```

### Integration Tests

Integration tests verify the full migration workflow end-to-end, including CAS cursor convergence between migration subcommands and live ingestion. See `internal/integrationtests/data_migration_test.go` for examples covering:

- Protocol setup classifying contracts correctly
- History migration filling state within the retention window
- Current-state migration building state from a start ledger
- CAS handoff between migrations and live ingestion
- Status lifecycle transitions (`not_started` -> `in_progress` -> `success`)

Run integration tests with:

```bash
go run main.go integration-tests
```

Integration tests require `db` and `stellar-rpc` services to be running.

## File Checklist

When adding a new protocol, you should create or modify these files:

| File | Action | Purpose |
|------|--------|---------|
| `internal/db/migrations/protocols/NNN_<protocol>.sql` | **Create** | Register protocol ID in DB |
| `internal/db/migrations/<date>.<seq>-<table>.sql` | **Create** | State table schema |
| `internal/services/<protocol>_validator.go` | **Create** | WASM contract classification |
| `internal/services/<protocol>_validator_test.go` | **Create** | Validator unit tests |
| `internal/data/<protocol>_<entity>.go` | **Create** | Data model (struct + interface + impl) |
| `internal/services/<protocol>_processor.go` | **Create** | Ledger state extraction |
| `internal/services/<protocol>_processor_test.go` | **Create** | Processor unit tests |
| `internal/services/<protocol>_register.go` | **Create** | Registry wiring via `init()` |
| `internal/data/models.go` | **Modify** | Add model field + initialization |
