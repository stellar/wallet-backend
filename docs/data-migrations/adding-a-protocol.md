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
  2. Validator                    (decide which contracts belong to your protocol — black box)
  3. State table + data model     (store protocol-specific state, optional and can be skipped if state tracking is not needed)
  4. Processor                    (extract state from ledgers)
  5. Registration file            (wire validator + processor into global registries)
  6. Models struct                (wire data model for dependency injection)

Operational:
  7. Run the migration workflow (migrate up -> restart ingest -> protocol-setup -> history + current-state)
```

> The framework treats each protocol's validator as a black box during classification. Inside `Validate` you are free to do anything you need (signature checks, RPC fetches, writes to your own protocol-specific tables). The only thing the framework cares about is the `MatchedWasms` slice your validator returns — that drives the generic `protocol_wasms.protocol_id` stamp.

## Prerequisites

- Familiarity with the Soroban contract model (WASM bytecode, `ScSpecEntry`, contract data entries)
- A clear definition of your protocol's required function signatures (used for classification)
- Understanding of what state your protocol produces (balances, positions, orders, etc.)

## Step 1: Register the Protocol in the Database

Create a protocol migration SQL file that inserts a row into the `protocols` table. This is the identity of your protocol in the system.

**File:** `internal/db/migrations/protocols/NNN_<protocol-name>.sql`

Use the next available sequence number. A full `migrate up` runs this file so the protocol is registered before live ingestion restarts; `protocol-setup` also re-runs it as an idempotent safeguard.

```sql
INSERT INTO protocols (id) VALUES ('<PROTOCOL_ID>') ON CONFLICT (id) DO NOTHING;
```

The protocol ID is a string primary key (e.g., `BLEND`, `AQUA`). It must match exactly across all code references -- the validator, processor, and registration file must all use the same string.

## Step 2: Create a Protocol Validator

The validator is your protocol's **black box during classification**. The framework hands it a batch of candidate WASMs (with parsed spec entries), the contracts referencing those WASMs, an RPC handle, and the data layer. The validator returns the wasm hashes it claims and may write anything else it needs to its own protocol-specific tables inside the supplied `dbTx`.

**File:** `internal/services/<protocol>/validator.go` (next to your processor and register files)

Implement the `services.ProtocolValidator` interface defined in `internal/services/protocol_validator.go`:

```go
type ProtocolValidator interface {
    ProtocolID() string
    Validate(ctx context.Context, dbTx pgx.Tx, input ValidationInput) (ValidationResult, error)
}

type ValidationInput struct {
    Candidates []WasmCandidate     // hash + bytecode + parsed spec entries
    Contracts  []ContractCandidate // contract_id + wasm_hash + KnownProtocolID (set if classified by an earlier batch)
    RPC        RPCService          // may be nil for offline migration
    Models     *data.Models
}

type ValidationResult struct {
    MatchedWasms []types.HashBytea
}
```

### What goes inside `Validate`

1. **Decide which candidate WASMs you claim.** Most protocols implement this as a private helper — for SEP-41 it is a signature-check against `WasmCandidate.SpecEntries` (see `matchSEP41Spec` in `internal/services/sep41/validator.go`). If your protocol is identified by a known set of contract IDs rather than a WASM signature, match on `Contracts[i].ContractID` instead.
2. **(Optional) Enrich the contracts you own.** Every contract whose `WasmHash` is in your match set, plus any `Contract` whose `KnownProtocolID == ProtocolID()` from a prior batch, is yours to act on. Common patterns: fetch on-chain metadata via `input.RPC` and write to your own tables via `dbTx`.
3. **Return the matched hashes.** The framework will stamp `protocol_wasms.protocol_id` for each returned hash inside the same transaction.

The framework guarantees:
- Spec extraction (wazero compile + `contractspecv0` parse) has already happened — `WasmCandidate.SpecEntries` is the parsed result.
- First-match-wins ordering across protocols. By the time your `Validate` runs, candidates already claimed by a higher-priority protocol have been filtered out of `input.Candidates`.
- The same `dbTx` you receive will commit `protocol_wasms.protocol_id` atomically with whatever you write — there is no separate "tail" transaction.
- A panicking or error-returning validator is logged and treated as a no-match for that protocol; it does not block the rest of classification.

### Skeleton (signature-based protocol)

```go
package myprotocol

import (
    "context"

    "github.com/jackc/pgx/v5"
    "github.com/stellar/go-stellar-sdk/xdr"

    "github.com/stellar/wallet-backend/internal/indexer/types"
    "github.com/stellar/wallet-backend/internal/services"
)

type Validator struct{}

func NewValidator() *Validator { return &Validator{} }

func newValidator(_ services.ProtocolDeps) *Validator { return NewValidator() }

func (v *Validator) ProtocolID() string { return ProtocolID }

func (v *Validator) Validate(_ context.Context, _ pgx.Tx, input services.ValidationInput) (services.ValidationResult, error) {
    var matches []types.HashBytea
    for _, cand := range input.Candidates {
        if matchesMyProtocol(cand.SpecEntries) {
            matches = append(matches, cand.Hash)
        }
    }
    return services.ValidationResult{MatchedWasms: matches}, nil
}

func matchesMyProtocol(specs []xdr.ScSpecEntry) bool {
    // Signature check goes here.
    return false
}
```

If your protocol also needs to populate metadata or other side tables, look at `internal/services/sep41/validator.go` for a worked example that uses RPC simulation inside `Validate` and writes to `contract_tokens` via `input.Models.Contract` — all inside the framework-supplied `dbTx`.

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
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);

-- Add indexes based on your query patterns

-- +migrate Down

DROP TABLE IF EXISTS <protocol>_<entity>;
```

`fillfactor = 90` is the default: it reserves 10% free space per page for HOT (Heap-Only
Tuple) updates without over-reserving for tables whose UPSERTs only touch non-indexed
columns. Use `fillfactor = 80` instead only for tables whose rows update on essentially
every ledger (e.g. AMM-style reserves that shift on every trade). `autovacuum_vacuum_cost_delay
= 0` disables cost-based vacuum throttling for this table's worker; there is no
`autovacuum_vacuum_cost_limit` to set alongside it since a delay of 0 makes any cost
limit a no-op.

The aggressive autovacuum block above is for tables whose row count grows with users or
usage (positions, balances, allowances). Skip it entirely — keep only `fillfactor = 90`
— for small, protocol-bounded current-state tables (pool configs, reserve configs) whose
row count is capped by the number of pools/reserves your protocol will ever have
deployed; default autovacuum behavior is fine for those.

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
// BatchUpsert is the write path used by PersistCurrentState. GetAll (and any other query
// methods) serve your protocol's read paths and are not part of the migration fold path.
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

    // ProcessLedger folds this ledger's state into the staged sets. It does NOT reset between
    // ledgers — the caller owns reset via Reset(). Folding a window of ledgers then persisting
    // MUST produce the same result as processing each ledger individually (batch-equivalence).
    // A protocol that cannot meet this must be migrated with --window-size=1.
    ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error

    // Reset clears the staged sets. The caller invokes it: the migration engine per window,
    // live ingestion per ledger.
    Reset()

    // PersistHistory / PersistCurrentState write the rows staged since the last Reset, using the
    // caller's transaction (committed atomically with the CAS cursor advance when it succeeds).
    PersistHistory(ctx context.Context, dbTx pgx.Tx) error
    PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error
}

type ProtocolProcessorInput struct {
    LedgerSequence    uint32
    LedgerCloseTime   int64
    ContractEvents    map[indexer.ContractEventKey][]xdr.ContractEvent
    ProtocolContracts []data.ProtocolContracts
    StagingMode       StagingMode
}
```

### Method Responsibilities

| Method | Called By | Purpose |
|--------|-----------|---------|
| `ProcessLedger` | Live ingestion, history & current-state migration | Fold a single ledger's protocol state into the staged sets. Does **not** clear between ledgers — accumulates until `Reset()`. |
| `Reset` | Migration engine (per window), live ingestion (per ledger) | Clear the staged sets after a window commits/hands off, or before the next ledger. |
| `PersistHistory` | Live ingestion, history migration | Write the history rows staged since the last `Reset` within the caller's transaction. |
| `PersistCurrentState` | Live ingestion, current-state migration | Write the current state staged since the last `Reset` within the caller's transaction. |

`input.StagingMode` tells the processor which staged sets to build (`history`, `current_state`, or both); the migration engine sets it per strategy, live ingestion sets both.

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

// stagedEntry is the latest folded write for one key within the current window. deleted
// records that the key's final state in the window is a removal rather than an upsert.
type stagedEntry struct {
    entry   data.MyProtocolEntry
    deleted bool
}

type MyProtocolProcessor struct {
    entryModel data.MyProtocolEntryModelInterface

    // Staged writes that accumulate ("fold") across ProcessLedger calls within a window and are
    // cleared only by Reset(). Keyed by the entry's primary key so repeated touches across ledgers
    // collapse to the latest write — this is what makes a folded window equivalent to processing
    // each ledger individually. The DB is the source of truth; the processor keeps no in-memory
    // mirror of current state.
    staged map[string]stagedEntry
}

func NewMyProtocolProcessor() *MyProtocolProcessor {
    p := &MyProtocolProcessor{}
    p.Reset() // initialize staged sets
    return p
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
    // Do NOT clear staged state here. ProcessLedger folds this ledger into the window that Reset()
    // owns; clearing per ledger would drop every ledger but the last under --window-size > 1.

    // 1. Early return if no contracts to track
    if len(input.ProtocolContracts) == 0 {
        return nil
    }

    // 2. Build the set of tracked contract IDs from input.ProtocolContracts
    trackedContracts := make(map[string]struct{}, len(input.ProtocolContracts))
    for _, pc := range input.ProtocolContracts {
        trackedContracts[string(pc.ContractID)] = struct{}{}
    }

    // 3. Iterate input.ContractEvents (keyed by (txIdx, opIdx); already filtered to successful
    //    transactions) and skip events from contracts you don't track.
    // 4. Derive each affected entry's primary key and fold it into p.staged with last-write-wins —
    //    a later ledger overwrites an earlier one for the same key, so the folded window matches
    //    per-ledger processing:
    //        key := buildKey(e) // e.g. hex.EncodeToString(e.PoolID) + ":" + e.UserAddress
    //        p.staged[key] = stagedEntry{entry: e}                 // upsert
    //        p.staged[key] = stagedEntry{entry: e, deleted: true}  // delete

    return nil
}

// Reset clears the staged window. The caller invokes it: the migration engine after each window
// commits or hands off, and live ingestion before each ledger.
func (p *MyProtocolProcessor) Reset() {
    p.staged = map[string]stagedEntry{}
}

func (p *MyProtocolProcessor) PersistHistory(_ context.Context, _ pgx.Tx) error {
    // Write historical state-change rows if your protocol tracks history. Those rows accumulate in
    // their own staged slice across ledgers (append-only — history never collapses) and are likewise
    // cleared by Reset(). Return nil if not applicable.
    return nil
}

func (p *MyProtocolProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
    if len(p.staged) == 0 {
        return nil
    }

    upserts := make([]data.MyProtocolEntry, 0, len(p.staged))
    deletes := make([]data.MyProtocolEntry, 0)
    for _, s := range p.staged {
        if s.deleted {
            deletes = append(deletes, s.entry)
        } else {
            upserts = append(upserts, s.entry)
        }
    }
    if err := p.entryModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
        return fmt.Errorf("persisting current state: %w", err)
    }

    // Do NOT clear p.staged here — Reset() owns clearing. Keeping it lets a failed/retried window
    // re-persist exactly the same set, and a coalesced window commits exactly once.
    log.Ctx(ctx).Debugf("MY_PROTOCOL: persisted %d upserts, %d deletes", len(upserts), len(deletes))
    return nil
}
```

### Key Patterns

- **Folding & batch-equivalence**: `ProcessLedger` accumulates across ledgers rather than processing one in isolation. Folding a window then persisting **must** equal processing each ledger individually: balances sum, last-write-wins values keep the newest, history rows append. Key the staged sets by primary key so repeated touches of the same row collapse. A protocol that cannot meet this must be migrated with `--window-size=1`.
- **Caller-owned `Reset()`**: staged sets are cleared only in `Reset()` — never inside `ProcessLedger` or `Persist*`. The migration engine calls it per window (after commit/handoff), live ingestion per ledger. This keeps a failed/retried window idempotent and a coalesced window committing exactly once.
- **Staging mode**: `input.StagingMode` selects which staged sets to build (`history`, `current_state`, or both). Gate history vs current-state staging on it so each migration strategy does only its own work.
- **Contract filtering**: build a set of tracked contract IDs from `input.ProtocolContracts` and skip events from contracts you don't track.
- **Graceful no-ops**: methods return `nil` early when there's nothing to do (no tracked contracts, nothing staged).
- **Transaction safety**: `PersistHistory` and `PersistCurrentState` receive a `pgx.Tx` and all writes happen within it. The CAS cursor advance commits in the same transaction, so writes and cursor move atomically (and roll back together on failure).

## Step 5: Register the Validator and Processor

Create a registration file inside your protocol package that wires your validator and processor into the global registries via `init()`. This is how the framework discovers your protocol at runtime — and the only place a `cmd/*` or `internal/ingest/*` binary needs to know your protocol exists is through a single blank import of this package.

**File:** `internal/services/<protocol>/register.go`

```go
package myprotocol

import (
    "github.com/stellar/wallet-backend/internal/services"
)

func init() {
    services.RegisterValidator(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolValidator {
        return newValidator(deps)
    })
    services.RegisterProcessor(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolProcessor {
        return newProcessor(deps)
    })
}
```

Both factories receive a `services.ProtocolDeps` so per-protocol packages can avoid package-level mutable state. Pull only the fields you need:

```go
type ProtocolDeps struct {
    NetworkPassphrase       string
    Models                  *data.Models
    RPCService              services.RPCService
    ContractMetadataService services.ContractMetadataService
    MetricsService          *metrics.Metrics
}
```

The `RegisterValidator` and `RegisterProcessor` functions live in `internal/services/validator_registry.go` and `internal/services/processor_registry.go`. Both are thread-safe global registries. The cmd/ingest layer calls `services.BuildValidators(deps, protocolIDs)` / `services.BuildProcessors(deps, protocolIDs)` to materialize them — no per-protocol imports beyond the blank import that triggers `init()`.

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

Once the code is deployed, run the workflow below. Restart ingestion **before** `protocol-setup` so live ingestion classifies new-protocol WASMs immediately; `protocol-setup` then drains the pre-existing backlog. Steps 7.4a and 7.4b run concurrently with each other and with live ingestion.

```
  Step 7.1         Step 7.2              Step 7.3              Step 7.4 (concurrent)
  migrate up  ──>  restart ingestion ──> protocol-setup  ──>  protocol-migrate history
  (schema +        (classifies new       (classifies           protocol-migrate current-state
   registration)    WASMs inline)         existing backlog)    (converge w/ live ingestion via CAS)
```

### Step 7.1: Schema Migration

Apply pending schema migrations. A full `migrate up` also registers your protocol in the `protocols` table, so live ingestion can classify its WASMs as soon as it restarts:

```bash
./wallet-backend migrate up
```

### Step 7.2: Restart Live Ingestion

Restart the ingestion service. It picks up the new processor automatically via the registry and, because the protocol is now registered, classifies new-protocol WASMs inline from the moment it restarts:

```bash
./wallet-backend ingest
```

Live ingestion uses CAS cursors to coordinate with the migration subcommands. It only produces state for ledgers where the migration hasn't already written data.

### Step 7.3: Protocol Setup

Classifies the existing unclassified WASMs already recorded in `protocol_wasms` by handing each protocol's validator the batch of unclassified bytecodes:

```bash
./wallet-backend protocol-setup --protocol-id <PROTOCOL_ID>
```

This:
- Fetches all unclassified WASM bytecodes via RPC
- Calls each registered validator's `Validate` (your code) inside one DB tx
- Stamps `protocol_wasms.protocol_id` from the returned matches and lets your validator write to its own tables in the same tx
- Sets `classification_status = success`
- Initializes both CAS cursors
- Re-runs your protocol registration SQL as an idempotent safeguard

### Step 7.4a: History Migration

Backfills historical state changes within the retention window:

```bash
./wallet-backend protocol-migrate history --protocol-id <PROTOCOL_ID>
```

Walks forward from the oldest ingestion cursor to the latest, calling `PersistHistory` at each ledger. Converges with live ingestion via the history CAS cursor. When the migration's CAS fails (cursor already advanced by live ingestion), it sets `history_migration_status = success` and exits.

### Step 7.4b: Current-State Migration (if your protocol tracks current-state data)

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

**Processor tests** (`internal/services/<protocol>_processor_test.go`) -- Verify folding, persistence, and reset behavior. Use a stub implementation of your data model interface:

```go
type myEntryModelStub struct {
    lastUpserts []data.MyProtocolEntry
    lastDeletes []data.MyProtocolEntry
    batchErr    error
}

var _ data.MyProtocolEntryModelInterface = (*myEntryModelStub)(nil)

func (s *myEntryModelStub) BatchUpsert(_ context.Context, _ pgx.Tx, upserts []data.MyProtocolEntry, deletes []data.MyProtocolEntry) error {
    s.lastUpserts = append([]data.MyProtocolEntry(nil), upserts...)
    s.lastDeletes = append([]data.MyProtocolEntry(nil), deletes...)
    return s.batchErr
}
```

Test cases to cover:

- `ProcessLedger` folds repeated touches of the same key across ledgers to the latest write (batch-equivalence: a coalesced window equals per-ledger processing)
- `PersistCurrentState` writes the staged upserts and deletes to the model
- `PersistCurrentState` is a no-op when nothing is staged
- `Reset` clears the staged sets so a committed window doesn't leak into the next
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
| `internal/services/<protocol>/validator.go` | **Create** | Black-box validator — signature checks + any side-effect writes during classification |
| `internal/services/<protocol>/validator_test.go` | **Create** | Validator unit tests |
| `internal/data/<protocol>_<entity>.go` | **Create** | Data model (struct + interface + impl) |
| `internal/services/<protocol>/processor.go` | **Create** | Ledger state extraction |
| `internal/services/<protocol>/processor_test.go` | **Create** | Processor unit tests |
| `internal/services/<protocol>/register.go` | **Create** | Registry wiring via `init()` (`RegisterValidator` + `RegisterProcessor`) |
| `internal/data/models.go` | **Modify** | Add model field + initialization |
| `cmd/protocol_setup.go`, `cmd/protocol_migrate.go`, `internal/ingest/ingest.go` | **Modify** | Add a single blank import `_ "internal/services/<protocol>"` so `init()` runs |
