# Data Migrations
Migrations add the ability to classify protocols and specific contracts, as well as the ability to enrich historical state and produce current state.

A migration consists of 2 responsibilities
- protocol classification
- state production

## Database Schema

The data migrations feature uses 4 tables(3 new ones):

### Protocols

The protocol registry. New protocols are added here and referenced during migration
and live ingestion processes.

```sql
CREATE TABLE protocols (
    id TEXT PRIMARY KEY,                              -- "BLEND", "SEP50", etc.
    classification_status TEXT DEFAULT 'not_started',
    history_migration_status TEXT DEFAULT 'not_started',
    current_state_migration_status TEXT DEFAULT 'not_started',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Status values for each column:
-- 'not_started'   - Initial state
-- 'in_progress'   - Process running
-- 'success'       - Process complete
-- 'failed'        - Process failed
```

**Migration Cursor Tracking** (via `ingest_store` table):

Protocol migrations track their progress using the existing `ingest_store` key-value table,
following the same pattern as the `oldest_ledger_cursor` and `latest_ledger_cursor` entries used by the ingestion service (see `internal/services/ingest.go` / `internal/ingest/ingest.go`):

```sql
-- ingest_store table (already exists)
CREATE TABLE ingest_store (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

Each protocol has two CAS cursors, one per migration subcommand. Each cursor is shared between its respective migration subcommand and live ingestion, and serves as both the convergence mechanism and crash recovery cursor — eliminating the need for a separate migration cursor.

**History Cursor** (via `ingest_store` table):

Tracks the last ledger for which protocol state changes were written:

```sql
-- History cursor example:
INSERT INTO ingest_store (key, value) VALUES ('protocol_SEP41_history_cursor', '50000');
```

The history cursor (e.g., `protocol_{PROTOCOL_ID}_history_cursor`) is **shared between history migration and live ingestion**. It is advanced atomically via compare-and-swap (CAS) within the same DB transaction that writes state change data. It also serves as the crash recovery cursor for history migration.

**Current State Cursor** (via `ingest_store` table):

Tracks the last ledger for which current state was produced:

```sql
-- Current state cursor example:
INSERT INTO ingest_store (key, value) VALUES ('protocol_SEP41_current_state_cursor', '50000');
```

The current state cursor (e.g., `protocol_{PROTOCOL_ID}_current_state_cursor`) is **shared between current-state migration and live ingestion**. It is advanced atomically via compare-and-swap (CAS) within the same DB transaction that writes current state data. It also serves as the crash recovery cursor for current-state migration.

**CAS Mechanism** (shared by both cursors):

```sql
-- CAS: only advance if the cursor is at the expected value
UPDATE ingest_store SET value = $new WHERE key = $cursor_name AND value = $expected;
-- Returns rows_affected = 1 on success, 0 if another process already advanced it
```

This requires a new `CompareAndSwap` method on `IngestStoreModel`. The existing `Update()` (`ingest_store.go:48`) is an unconditional upsert and cannot be used for this purpose.

The CAS mechanism ensures that exactly one process (migration or live ingestion) writes data for any given ledger on each cursor, enabling a seamless handoff without coordination between the two processes (see [Convergence Model](#backfill-migration)).

**Cursor Initialization** (during `protocol-setup`):

Both cursors are initialized when `classification_status` moves to `success`:
- `protocol_{ID}_history_cursor` = `oldest_ledger_cursor - 1`
- `protocol_{ID}_current_state_cursor` = 0 (or left uninitialized until current-state migration starts)

This ensures live ingestion has cursors to gate against from the start, even if migrations haven't run yet.

### protocol_contracts

Maps protocols to the contracts that make up their systems.

```sql
CREATE TABLE protocol_contracts (
    contract_id TEXT NOT NULL,        -- C... address
    protocol_id TEXT NOT NULL REFERENCES protocols(id),
    name TEXT,                        -- "pool", "factory", "token", etc.
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (contract_id, protocol_id)
);
```

### known_wasms

A cache for all known WASM blobs. This acts as a filter for the classification process
to reduce the overhead of classifying new contract instances that use the same WASM code.

```sql
CREATE TABLE known_wasms (
    wasm_hash TEXT PRIMARY KEY,
    protocol_id TEXT REFERENCES protocols(id),  -- NULL if unknown/unclassified
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Overview

Adding a new protocol requires four coordinated processes:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      PROTOCOL ONBOARDING WORKFLOW                               │
└─────────────────────────────────────────────────────────────────────────────────┘

   STEP 1: SETUP             STEP 2: LIVE INGESTION
┌──────────────────────┐    ┌──────────────────────┐
│ ./wallet-backend     │    │ Restart ingestion    │
│ protocol-setup       │───▶│ with new processor   │
│                      │    │                      │
│ Classifies existing  │    │ Produces state from  │
│ contracts            │    │ restart ledger onward│
└──────────────────────┘    └──────────┬───────────┘
                                       │
                 Steps 2, 3a, and 3b run concurrently
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
          ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
          │ Live ingestion:  │  │ STEP 3a:     │  │ STEP 3b:         │
          │ state changes    │  │ HISTORY      │  │ CURRENT-STATE    │
          │ after history    │  │ MIGRATION    │  │ MIGRATION        │
          │ convergence,     │  │              │  │                  │
          │ current state    │  │ protocol-    │  │ protocol-        │
          │ after current-   │  │ migrate      │  │ migrate          │
          │ state            │  │ history      │  │ current-state    │
          │ convergence      │  │              │  │                  │
          │                  │  │ Retention    │  │ From start-      │
          │                  │  │ window only  │  │ ledger to tip    │
          └────────┬─────────┘  └──────┬───────┘  └────────┬─────────┘
                   │                   │                   │
                   │◄── CAS handoff ──▶│                   │
                   │   (history cursor)│                   │
                   │                   │                   │
                   │◄────── CAS handoff ──────────────────▶│
                   │  (current-state cursor)               │
                   │                                       │
                   └──────────────────┬────────────────────┘
                                      │
                   Each migration CAS fails = handoff
                   Live ingestion takes over that responsibility
                                      │
                                      ▼
                   Complete coverage via two independent cursors:
                   - History cursor: state changes [retention_start → current]
                   - Current-state cursor: current state [start_ledger → current]
```

## Process Dependencies

| Step | Requires | Produces |
|------|----------|----------|
| **1. protocol-setup** | Protocol migration SQL file, protocol implementation in code | Protocol in DB, `known_wasms`, `protocol_contracts`, `classification_status = success`, both cursors initialized |
| **2. ingest (live)** | `classification_status = success`, processor registered | State changes after history convergence (history cursor). Current state after current-state convergence (current-state cursor). |
| **3a. protocol-migrate history** | `classification_status = success` | Protocol state changes within retention window, through convergence with live ingestion |
| **3b. protocol-migrate current-state** | `classification_status = success` | Current state from `start_ledger` through convergence with live ingestion |

Steps 2, 3a, and 3b run **concurrently**. Each migration subcommand converges independently with live ingestion via its own CAS cursor:
- History migration converges via `protocol_{ID}_history_cursor` — when its CAS fails, live ingestion owns state change production
- Current-state migration converges via `protocol_{ID}_current_state_cursor` — when its CAS fails, live ingestion owns current state production

The two subcommands are fully independent. They write to different tables, use different CAS cursors, and track different status columns. They can run in any order, concurrently, or only one can be run.

Both live ingestion and backfill migration need the `protocol_contracts` table populated to know which contracts to process. The `protocol-setup` command ensures this data exists before either process runs.

## Classification
Classification is the act of identifying new and existing contracts on the network and assigning a relationship to a known protocol.
This has to happen in 2 stages during the migration process:
- checkpoint population: We will use a history archive from the latest checkpoint in order to classify all contracts on the network. We will rely on the latest checkpoint available at the time of the migration.
- live ingestion: during live ingestion, we classify new WASM uploads by validating the bytecode against protocol specs, and map contract deployments/upgrades to protocols by looking up their WASM hash in `known_wasms`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CHECKPOINT POPULATION FLOW                           │
└─────────────────────────────────────────────────────────────────────────────┘

                           ┌──────────────────┐
                           │  History Archive │
                           │   (checkpoint)   │
                           └────────┬─────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │ NewCheckpointChangeReader()   │
                    │ (reads checkpoint ledger)     │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │ streamCheckpointData()        │
                    │ (iterates all ledger entries) │
                    └───────────────┬───────────────┘
                                    │
                       ┌────────────┴────────────┐
                       │                         │
                       ▼                         ▼
            ┌──────────────────┐      ┌──────────────────┐
            │ LedgerEntryType  │      │ LedgerEntryType  │
            │ ContractCode     │      │ ContractData     │
            │                  │      │ (Instance)       │
            └────────┬─────────┘      └────────┬─────────┘
                     │                         │
                     ▼                         ▼
            ┌──────────────────┐      ┌──────────────────┐
            │ Extract WASM     │      │ Check SAC?       │
            │ bytecode + hash  │      │ (AssetFromData)  │
            └────────┬─────────┘      └────────┬─────────┘
                     │                         │
                     ▼                 ┌───────┴───────┐
            ┌──────────────────┐       │               │
            │ Validate WASM    │       ▼               ▼
            │ against protocol │      YES             NO
            │ validators       │       │               │
            └────────┬─────────┘       ▼               ▼
                     │          ┌────────┐      ┌──────────────────┐
                ┌────┴────┐     │SAC     │      │ Extract wasm_ref │
                │         │     │contract│      │ (hash) from      │
              MATCH    NO MATCH └────────┘      │ instance data    │
                │         │                     └────────┬─────────┘
                ▼         ▼                              │
            ┌────────┐ ┌──────────┐                      ▼
            │Store   │ │Store     │              ┌──────────────────┐
            │hash in │ │hash in   │              │ Map contract ID  │
            │known_  │ │known_    │              │ to WASM hash     │
            │wasms   │ │wasms     │              │ (for later lookup│
            │with    │ │with NULL │              │ in known_wasms)  │
            │protocol│ │protocol  │              └──────────────────┘
            └────────┘ └──────────┘

                    ┌───────────────────────────────┐
                    │ Post-Processing:              │
                    │ 1. Store in protocol_contracts│
                    │    (contract → protocol via   │
                    │     wasm hash → known_wasms)  │
                    │ 2. Cache in known_wasms       │
                    └───────────────────────────────┘
```

Contracts are grouped by WASM hash before validation. This means we validate each unique WASM blob once, then apply the result to all contracts using that same code.
Once a WASM hash is classified, it is stored in the `known_wasms` table to avoid re-classification of future contracts using the same code.

During live ingestion, classification happens in two parts: (1) new WASM uploads are validated against protocol specs and stored in `known_wasms`, and (2) contract deployments/upgrades are mapped to protocols via their WASM hash lookup in `known_wasms`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       LIVE INGESTION CLASSIFICATION                         │
└─────────────────────────────────────────────────────────────────────────────┘

                         ┌──────────────────┐
                         │   Stellar RPC    │
                         │ (LedgerCloseMeta)│
                         └────────┬─────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ ProcessLedger()              │
                   │ iterate ledger entry changes │
                   └──────────────┬───────────────┘
                                  │
              ┌───────────────────┴───────────────────┐
              │                                       │
              ▼                                       ▼
   ┌─────────────────────┐             ┌──────────────────────────┐
   │ ContractCode        │             │ ContractData Instance    │
   │ (new WASM upload)   │             │ (deployment or upgrade)  │
   └──────────┬──────────┘             └────────────┬─────────────┘
              │                                     │
              ▼                                     ▼
   ┌─────────────────────┐             ┌──────────────────────────┐
   │ Extract WASM        │             │ Extract WASM hash        │
   │ bytecode + hash     │             │ from instance wasm_ref   │
   └──────────┬──────────┘             └────────────┬─────────────┘
              │                                     │
              ▼                                     ▼
   ┌─────────────────────┐             ┌──────────────────────────┐
   │ Validate against    │             │ Lookup hash in           │
   │ protocol validators │             │ known_wasms              │
   └──────────┬──────────┘             └────────────┬─────────────┘
              │                                     │
         ┌────┴────┐                    ┌───────────┴───────────┐
         │         │                    │                       │
       MATCH    NO MATCH              FOUND                 NOT FOUND
         │         │                    │                       │
         ▼         ▼                    ▼                       ▼
   ┌──────────┐ ┌──────────┐    ┌──────────────┐     ┌──────────────────┐
   │Store in  │ │Store in  │    │ Map contract │     │ Fetch WASM via   │
   │known_    │ │known_    │    │ to protocol  │     │ RPC, validate,   │
   │wasms with│ │wasms with│    │ from cached  │     │ then map contract│
   │protocol  │ │NULL      │    │ classification     │ (rare edge case) │
   └──────────┘ └──────────┘    └──────────────┘     └──────────────────┘
                                        │                       │
                                        └───────────┬───────────┘
                                                    │
                                                    ▼
                                    ┌──────────────────────────┐
                                    │ Insert contract mapping  │
                                    │ to protocol_contracts    │
                                    └──────────────────────────┘
```

The classifier validates WASM bytecode from ContractCode entries against protocol specifications.
This validation uses the same logic as checkpoint population:

1. Compile WASM with wazero runtime
2. Extract `contractspecv0` custom section
3. Parse XDR `ScSpecEntry` items
4. Validate function signatures against protocol requirements

### WASM Bytecode Extraction & Comparison

The `ContractValidator` service performs protocol validation by analyzing WASM bytecode.
This is the core mechanism for classifying contracts during both checkpoint population
and live ingestion.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WASM CLASSIFICATION PIPELINE                             │
│                (internal/services/contract_validator.go)                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│ Raw WASM bytes  │
│ (from ledger    │
│ ContractCode)   │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 1: WASM Compilation (wazero runtime)                        │
│                                                                  │
│   config := wazero.NewRuntimeConfig().WithCustomSections(true)   │
│   compiledModule, _ := runtime.CompileModule(ctx, wasmCode)      │
│                                                                  │
│   Purpose: Validate WASM structure + extract custom sections     │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 2: Extract "contractspecv0" Custom Section                  │
│                                                                  │
│   WASM Module Structure:                                         │
│   ┌──────────────────────────────────────┐                       │
│   │ Code Section (executable functions)  │                       │
│   ├──────────────────────────────────────┤                       │
│   │ Data Section (constants)             │                       │
│   ├──────────────────────────────────────┤                       │
│   │ Custom Sections                      │                       │
│   │   └── "contractspecv0" ◄─────────────┼── XDR-encoded spec    │
│   └──────────────────────────────────────┘                       │
│                                                                  │
│   for _, section := range compiledModule.CustomSections() {      │
│       if section.Name() == "contractspecv0" {                    │
│           specBytes = section.Data()                             │
│       }                                                          │
│   }                                                              │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 3: XDR Unmarshal → []ScSpecEntry                            │
│                                                                  │
│   reader := bytes.NewReader(specBytes)                           │
│   for reader.Len() > 0 {                                         │
│       var spec xdr.ScSpecEntry                                   │
│       xdr.Unmarshal(reader, &spec)                               │
│       specs = append(specs, spec)                                │
│   }                                                              │
│                                                                  │
│   Each ScSpecEntry represents:                                   │
│   - Function definitions (name, inputs, outputs)                 │
│   - Type definitions (structs, enums)                            │
│   - Error definitions                                            │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 4: Protocol Signature Validation                            │
│                                                                  │
│   For each function in contractSpec:                             │
│   - Extract function name                                        │
│   - Extract input parameters (name → type mapping)               │
│   - Extract output types                                         │
│   - Compare against protocol's required functions                │
│                                                                  │
│   Example (SEP-41 Token Standard):                               │
│   - Required: balance, allowance, decimals, name, symbol,        │
│               approve, transfer, transfer_from, burn, burn_from  │
│   - All parameter names and types must match exactly             │
│                                                                  │
│   foundFunctions.Add(funcName) if signature matches              │
│   MATCH = foundFunctions.Cardinality() == len(requiredSpecs)     │
└──────────────────────────────────────────────────────────────────┘
```

**SEP-41 Required Functions** (example from `contract_validator.go`):

| Function | Inputs | Output |
|----------|--------|--------|
| `balance` | `id: Address` | `i128` |
| `allowance` | `from: Address, spender: Address` | `i128` |
| `decimals` | _(none)_ | `u32` |
| `name` | _(none)_ | `String` |
| `symbol` | _(none)_ | `String` |
| `approve` | `from: Address, spender: Address, amount: i128, expiration_ledger: u32` | _(void)_ |
| `transfer` | `from: Address, to: Address, amount: i128` | _(void)_ |
| `transfer_from` | `spender: Address, from: Address, to: Address, amount: i128` | _(void)_ |
| `burn` | `from: Address, amount: i128` | _(void)_ |
| `burn_from` | `spender: Address, from: Address, amount: i128` | _(void)_ |

**Validation Logic**:
- All required function names must be present
- Parameter names must match exactly (`from`, `to`, `amount`, etc.)
- Parameter types must match (Address, i128, u32, etc.)

**known_wasms Table Usage**:

The `known_wasms` table stores classification results by WASM hash. The table stores
a `protocol_id` for each WASM hash - this is `NULL` for WASM blobs that don't match
any known protocol.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        known_wasms CACHE FLOW                              │
└────────────────────────────────────────────────────────────────────────────┘

         New Contract Deployment
                   │
                   ▼
         ┌─────────────────────┐
         │ Compute WASM hash   │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │ SELECT protocol_id  │
         │ FROM known_wasms    │
         │ WHERE wasm_hash = ? │
         └──────────┬──────────┘
                    │
           ┌────────┴────────┐
           │                 │
         FOUND           NOT FOUND
           │                 │
           ▼                 ▼
   ┌──────────────┐  ┌──────────────────┐
   │ Use cached   │  │ Run full WASM    │
   │ protocol_id  │  │ validation       │
   │              │  │                  │
   │ Skip WASM    │  │ Then INSERT INTO │
   │ validation   │  │ known_wasms      │
   └──────────────┘  └──────────────────┘
```

This optimization is critical for performance because:
- Many contracts share the same WASM code (e.g., token contracts)
- WASM compilation with wazero is CPU-intensive
- A single validation per unique WASM hash serves all contracts using that code

When a new protocol is registered, running `protocol-setup` re-validates previously unknown WASM hashes (those with `protocol_id = NULL`) against the new protocol's spec. This ensures contracts deployed before the protocol was added can still be classified correctly.
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 RE-CLASSIFICATION ON NEW PROTOCOL REGISTRATION              │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────┐
                    │ New Protocol Registered     │
                    │ (e.g., "BLEND" added to     │
                    │  protocols table)           │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │ protocol-setup              │
                    │ --protocol-id BLEND         │
                    │                             │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │ Query known_wasms for       │
                    │ unclassified entries:       │
                    │                             │
                    │ SELECT wasm_hash            │
                    │ FROM known_wasms            │
                    │ WHERE protocol_id IS NULL   │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │ For each unclassified hash: │
                    │ - Validate against NEW      │
                    │   protocol spec             │
                    └─────────────┬───────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
                MATCH                         NO MATCH
                  │                               │
                  ▼                               ▼
        ┌─────────────────┐             ┌─────────────────┐
        │ UPDATE          │             │ Leave as        │
        │ known_wasms     │             │ protocol_id     │
        │ SET protocol_id │             │ = NULL          │
        │ = 'BLEND'       │             │                 │
        │ WHERE wasm_hash │             │ (still unknown) │
        │ = ?             │             │                 │
        └────────┬────────┘             └─────────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │ Find all        │
        │ contracts with  │
        │ this WASM hash  │
        │                 │
        │ INSERT INTO     │
        │ protocol_       │
        │ contracts       │
        └─────────────────┘
```

#### Protocol-Specific Contract Validators

The current `ContractValidator` validates SEP-41 compliance. For protocol migrations, 
the validator must be split into separate validators - one per protocol type.

**Current State** (single validator):
```
ContractValidator
└── ValidateFromContractCode(wasmBytes) → ContractTypeSEP41 | ContractTypeUnknown
```

**Required State** (separate validator per protocol):
```
SEP41Validator    → validates SEP-41 token standard (balance, transfer, approve, etc.)
BlendValidator    → validates Blend lending protocol (deposit, borrow, repay, etc.)
AquaValidator     → validates Aquarius AMM protocol
SoroswapValidator → validates Soroswap DEX protocol
... (one per protocol)
```

Each validator checks whether a WASM blob's contract spec contains the required 
function signatures for that protocol. The validation logic is the same as the 
current SEP-41 validator - compile WASM, extract `contractspecv0` section, parse 
XDR spec entries, check for required functions.

When checkpoint population runs for a newly registered protocol, it validates contracts whose WASM hash is either:
1. **Not in known_wasms** (never seen before)
2. **In known_wasms with `protocol_id IS NULL`** (previously unknown)

#### When Checkpoint Classification Runs
Backfill migrations rely on checkpoint population being complete before they can produce state changes for a new protocol. If checkpoint population does not run before a backfill migration is started for a new protocol, backfill migration will fail and exit since it does not classify protocols and cannot produce state without any classification being available.

### Command

```bash
./wallet-backend protocol-setup --protocol-id SEP50 --protocol-id BLEND
```

### What It Does

1. **Runs protocol migrations** - Executes SQL migrations from `internal/data/migrations/protocols/` to register new protocols in the `protocols` table with `classification_status = not_started`
2. **Sets `classification_status`** to `in_progress` for specified protocols
3. **Reads the latest checkpoint** from the history archive
4. **Extracts all WASM code** from contract entries in the checkpoint
5. **Queries existing unclassified entries** from `known_wasms WHERE protocol_id IS NULL`
6. **Validates each WASM** against all specified protocols' validators
7. **Populates tables**:
   - `known_wasms`: Maps WASM hashes to protocol IDs
   - `protocol_contracts`: Maps contract IDs to protocols
8. **Initializes cursors**:
   - `protocol_{ID}_history_cursor` = `oldest_ledger_cursor - 1`
   - `protocol_{ID}_current_state_cursor` = 0
9. **Updates `classification_status`** to `success` for all processed protocols

### Protocol Migration Files

Protocol registrations are defined as SQL migration files in `internal/data/migrations/protocols/`:

```
internal/data/migrations/protocols/
├── 001_sep50.sql
├── 002_blend.sql
└── 003_aqua.sql
```

These migrations are tracked separately from the main schema migrations, allowing `protocol-setup` to run them independently.

### Explicit Protocol Selection

The command requires an explicit list of protocols to set up via the `--protocol-id` flag. Only specified protocols will be processed.

**Benefits:**
- Opt-in protocol support - operators control which protocols are enabled
- Clear operator intent - no accidental protocol enablement
- Consistent with `protocol-migrate` subcommand interfaces

## State Production
State produced by new protocols is split into two independent responsibilities, each handled by a dedicated migration subcommand:
- **History (state changes)**: `protocol-migrate history` writes protocol state changes (operation enrichment) for ledgers within the retention window. It starts at `oldest_ledger_cursor` and converges with live ingestion via the history cursor. Since it only processes the retention window, ALL processed ledgers produce persisted state changes — no "process but discard" logic needed.
- **Current state**: `protocol-migrate current-state` builds current state from a protocol's deployment ledger forward. It starts at `--start-ledger` and converges with live ingestion via the current-state cursor. It processes ALL ledgers from start to tip to build accurate additive state, but writes only current state — no state changes.
- **Live ingest state**: Live ingestion produces both state changes and current state, but only after converging with the respective migration subcommand for each. It gates state change writes on the history cursor and current state writes on the current-state cursor.

### Additive vs Non-Additive Current State

Protocol current state falls into two categories that affect how migration and live ingestion interact:

**Non-additive state** (e.g., collectible ownership): The current state at ledger N can be determined from the ledger data alone, without knowing the state at ledger N-1. Live ingestion can write current state immediately for any ledger, independent of migration progress.

**Additive state** (e.g., token balances): The current state at ledger N depends on the state at ledger N-1. A "transfer of 5 tokens" event at ledger N requires knowing the balance before ledger N to compute the new balance. During migration, that previous balance doesn't exist until all prior ledgers are processed.

```
Non-additive example (collectible ownership):
  Ledger N says "User A owns collectible X" → write directly, no prior state needed.

Additive example (token balance):
  Ledger N says "Transfer 5 tokens from A to B"
  → Need balance of A at ledger N-1 to compute new balance
  → That balance doesn't exist until migration processes ledgers 1 through N-1
```

This distinction drives the convergence model: migration must run to the tip (not stop at a fixed end-ledger) so that additive current state is continuously built without gaps. The shared current-state cursor with CAS ensures exactly one process produces current state for each ledger, with a seamless handoff when migration catches up to live ingestion.

### Backfill Migration

The backfill migration is split into two independent subcommands that handle different responsibilities:

#### History Migration (`protocol-migrate history`)

The history migration writes protocol state changes (operation enrichment) for ledgers within the retention window.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      HISTORY MIGRATION FLOW                                 │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌────────────────────────────┐
                    │ ./wallet-backend           │
                    │ protocol-migrate history   │
                    │ --protocol-id SEP50 SEP41  │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Start()                    │
                    │ - Validate classification  │
                    │   _status = 'success'      │
                    │ - Set history_migration    │
                    │   _status = 'in_progress'  │
                    │ - Read oldest_ledger_cursor│
                    │   from ingest_store        │
                    │ - Initialize history_cursor│
                    │   = oldest_ledger_cursor-1 │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Read latest_ledger_cursor  │
                    │ Split [start, target] into │
                    │ batches. Process in        │
                    │ parallel with ordered      │◀──────────────┐
                    │ commit.                    │               │
                    └─────────────┬──────────────┘               │
                                  │                              │
                                  ▼                              │
                    ┌────────────────────────────┐               │
                    │ Per batch commit:          │               │
                    │ - CAS-advance history      │               │
                    │   _cursor                  │               │
                    │ - Write state changes      │               │
                    │   (if CAS succeeded)       │               │
                    └─────────────┬──────────────┘               │
                                  │                              │
                         ┌────────┴────────┐                     │
                         │                 │                     │
                    CAS success       CAS failure                │
                         │                 │                     │
                         ▼                 ▼                     │
                    ┌──────────┐   ┌──────────────────┐          │
                    │ Continue │   │ Handoff detected │          │
                    │ to next  │   │ Live ingestion   │          │
                    │ batch    │   │ took over.       │          │
                    └────┬─────┘   │ Exit loop.       │          │
                         │         └────────┬─────────┘          │
                         │                  │                    │
                         ▼                  │                    │
                    ┌──────────────┐        │                    │
                    │ More batches │        │                    │
                    │ remaining?   │        │                    │
                    │              │        │                    │
                    │ YES: continue│        │                    │
                    │ NO: re-read  │────────┼────────────────────┘
                    │ latest_ledger│        │  (fetch new target,
                    │ _cursor, loop│        │   process remaining)
                    └──────────────┘        │
                                            │
                                            ▼
                    ┌────────────────────────────┐
                    │ Complete()                 │
                    │ - Set history_migration    │
                    │   _status = 'success'      │
                    │ - Clean up resources       │
                    └────────────────────────────┘
```

**Key simplification**: Since history migration starts at retention start, ALL processed ledgers are within retention. No need for the "process but discard" logic — every batch produces persisted state changes.

**Parallelization advantage**: State changes for ledger N do not depend on state changes for ledger N-1, so batches are truly independent. History migration can be more aggressively parallelized than current-state migration.

#### Current-State Migration (`protocol-migrate current-state`)

The current-state migration builds current state from a protocol's deployment ledger forward.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CURRENT-STATE MIGRATION FLOW                             │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌────────────────────────────┐
                    │ ./wallet-backend           │
                    │ protocol-migrate           │
                    │ current-state              │
                    │ --protocol-id SEP50        │
                    │ --start-ledger 1000        │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Start()                    │
                    │ - Validate classification  │
                    │   _status = 'success'      │
                    │ - Set current_state        │
                    │   _migration_status =      │
                    │   'in_progress'            │
                    │ - Initialize current_state │
                    │   _cursor = start-ledger-1 │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Read latest_ledger_cursor  │
                    │ Split [start, target] into │
                    │ batches. Process in        │
                    │ parallel with ordered      │◀──────────────┐
                    │ commit.                    │               │
                    └─────────────┬──────────────┘               │
                                  │                              │
                                  ▼                              │
                    ┌────────────────────────────┐               │
                    │ Per batch commit:          │               │
                    │ - CAS-advance current      │               │
                    │   _state_cursor            │               │
                    │ - Write current state      │               │
                    │   (if CAS succeeded)       │               │
                    │ - No state changes written │               │
                    └─────────────┬──────────────┘               │
                                  │                              │
                         ┌────────┴────────┐                     │
                         │                 │                     │
                    CAS success       CAS failure                │
                         │                 │                     │
                         ▼                 ▼                     │
                    ┌──────────┐   ┌──────────────────┐          │
                    │ Continue │   │ Handoff detected │          │
                    │ to next  │   │ Live ingestion   │          │
                    │ batch    │   │ took over.       │          │
                    └────┬─────┘   │ Exit loop.       │          │
                         │         └────────┬─────────┘          │
                         │                  │                    │
                         ▼                  │                    │
                    ┌──────────────┐        │                    │
                    │ More batches │        │                    │
                    │ remaining?   │        │                    │
                    │              │        │                    │
                    │ YES: continue│        │                    │
                    │ NO: re-read  │────────┼────────────────────┘
                    │ latest_ledger│        │  (fetch new target,
                    │ _cursor, loop│        │   process remaining)
                    └──────────────┘        │
                                            │
                                            ▼
                    ┌────────────────────────────┐
                    │ Complete()                 │
                    │ - Set current_state        │
                    │   _migration_status =      │
                    │   'success'                │
                    │ - Clean up resources       │
                    └────────────────────────────┘
```

**Processing range**: Current-state migration processes ALL ledgers from `--start-ledger` to tip. This is necessary for accurate additive state (e.g., token balances) where ledger N depends on ledger N-1.

#### Independence

The two subcommands are fully independent:
- They write to different tables (state changes vs current state)
- They use different CAS cursors (`history_cursor` vs `current_state_cursor`)
- They track different status columns (`history_migration_status` vs `current_state_migration_status`)
- They can run in any order, concurrently, or only one can be run

#### Convergence Model

Two independent convergence paths:

```
HISTORY CONVERGENCE:
┌────────────────────────────────────────────────────────────────────────────┐
│ History migration CAS-advances protocol_{ID}_history_cursor from           │
│ retention_start. Live ingestion also CAS-advances the same cursor.         │
│ When history migration CAS fails → live ingestion owns state change        │
│ production.                                                                │
│                                                                            │
│ Timeline example:                                                          │
│   T=0s:   History cursor=10004. Migration CAS 10004→10005. Success.        │
│   T=0.5s: Migration CAS 10005→10006. Success.                              │
│   T=5s:   Live ingestion processes 10008. Cursor=10007 >= 10007. YES.      │
│           Live CAS 10007→10008. Success.                                   │
│   T=5.5s: Migration tries CAS 10007→10008. FAILS. Handoff detected.        │
│                                                                            │
│ No gap: every ledger gets state changes from exactly one process.          │
└────────────────────────────────────────────────────────────────────────────┘

CURRENT STATE CONVERGENCE:
┌────────────────────────────────────────────────────────────────────────────┐
│ Current-state migration CAS-advances protocol_{ID}_current_state_cursor    │
│ from start_ledger. Live ingestion also CAS-advances the same cursor.       │
│ When current-state migration CAS fails → live ingestion owns current       │
│ state production.                                                          │
│                                                                            │
│ Same CAS mechanism as history convergence, but using a separate cursor.    │
│ No gap: every ledger gets current state from exactly one process.          │
└────────────────────────────────────────────────────────────────────────────┘
```

#### Migration Dependencies

```
MIGRATION DEPENDENCIES:
┌────────────────────────────────────────────────────────────────────────────┐
│ Both migration subcommands depend on protocol-setup,                       │
│ and run concurrently with live ingestion:                                  │
│ 1. Checkpoint population must have completed (classification_status =      │
│    'success')                                                              │
│ 2. Live ingestion should be running with the same processor                │
│ 3. History migration: retention_start → tip (until CAS fails)              | 
│ 4. Current-state migration: start-ledger → tip (until CAS fails)           │
│ 5. Live ingestion gates both state changes and current state on their      │
│    respective cursors                                                      │
│ 6. Handoff: each migration's CAS fails → live ingestion owns that          │
│    responsibility                                                          │
│                                                                            │
│ This ensures zero-gap coverage via CAS serialization on each cursor.       │
└────────────────────────────────────────────────────────────────────────────┘
```

### Live State Production

During live ingestion, two related but distinct processes run sequentially:
1. **Classification** - Identifies and classifies new contracts as they are deployed
2. **State Production** - Produces protocol-specific state using registered processors (depends on classification)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LIVE INGESTION: TWO RESPONSIBILITIES                     │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────────┐
                              │ LedgerCloseMeta  │
                              │ (from RPC)       │
                              └────────┬─────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          1. CLASSIFICATION                                  │
│                                                                             │
│  Process ledger entry changes to classify contracts:                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ContractCode entries: validate WASM, store in known_wasms           │    │
│  │ ContractData Instance entries: lookup hash in known_wasms,          │    │
│  │   map contract to protocol_contracts                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          2. STATE PRODUCTION                                │
│                                                                             │
│  Run protocol processors on transactions (using updated classifications):   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ For each protocol processor:                                        │    │
│  │   Processor.Process(ledger)                                         │    │
│  │   - Examines transactions involving protocol contracts              │    │
│  │   - Produces protocol-specific state changes                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
                        ┌──────────────────────────┐
                        │ PersistLedgerData()      │
                        │ (single DB transaction)  │
                        └──────────────┬───────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            │                          │                          │
            ▼                          ▼                          ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ New contract         │  │ Protocol-specific    │  │ Operations,          │
│ classifications      │  │ state changes        │  │ transactions,        │
│ (protocol_contracts, │  │ (from processors)    │  │ accounts, etc.       │
│  known_wasms)        │  │                      │  │                      │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    PER-PROTOCOL DUAL GATING                                 │
│                                                                             │
│  Within PersistLedgerData, for each registered protocol at ledger N:        │
│                                                                             │
│  === PROTOCOL STATE CHANGES ===                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. Read protocol_{ID}_history_cursor                                │    │
│  │                                                                     │    │
│  │ 2. If cursor >= N-1:                                                │    │
│  │    - CAS history cursor from N-1 to N                               │    │
│  │    - If CAS succeeds: WRITE state changes for N                     │    │
│  │    - If CAS fails: skip (history migration wrote them)              │    │
│  │                                                                     │    │
│  │ 3. If cursor < N-1:                                                 │    │
│  │    - SKIP state changes (history migration hasn't caught up)        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  === CURRENT STATE ===                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 4. Read protocol_{ID}_current_state_cursor                          │    │
│  │                                                                     │    │
│  │ 5. If cursor >= N-1:                                                │    │
│  │    - CAS current_state cursor from N-1 to N                         │    │
│  │    - If CAS succeeds: WRITE current state for N                     │    │
│  │    - If CAS fails: skip (current-state migration wrote it)          │    │
│  │                                                                     │    │
│  │ 6. If cursor < N-1:                                                 │    │
│  │    - SKIP current state (current-state migration hasn't caught up)  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Why gate state changes: The existing BatchCopy write path (pgx COPY        │
│  protocol) fails on duplicate records — it does not support ON CONFLICT.    │
│  Gating prevents duplicates and follows the same proven CAS pattern         │
│  already designed for current state.                                        │
│                                                                             │
│  This logic is per-protocol. Different protocols can be at different        │
│  stages — one may have history migration complete while another is still    │
│  running, and current-state migration may be at a different stage than      │
│  history migration for the same protocol.                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## known_wasms Lookup Optimization

The `known_wasms` table grows unboundedly as new contracts are deployed on the network. Since
every live ingestion lookup queries this table, optimizing lookup performance is critical.

#### Default Implementation: LRU Cache + PostgreSQL

The recommended approach is an in-memory LRU cache layered over the PostgreSQL table:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    known_wasms LOOKUP OPTIMIZATION                          │
└─────────────────────────────────────────────────────────────────────────────┘

                     New Contract Deployment
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Compute WASM hash    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Check LRU Cache      │
                    │ (in-memory, <1µs)    │
                    └──────────┬───────────┘
                               │
                   ┌───────────┴───────────┐
                   │                       │
                 HIT                     MISS
                   │                       │
                   ▼                       ▼
          ┌──────────────┐      ┌──────────────────────┐
          │ Return       │      │ Query PostgreSQL     │
          │ cached       │      │ known_wasms table    │
          │ protocol_id  │      │ (1-5ms)              │
          └──────────────┘      └──────────┬───────────┘
                                           │
                                           ▼
                                ┌──────────────────────┐
                                │ Populate LRU cache   │
                                │ with result          │
                                └──────────────────────┘
```

**Implementation Pattern**:

```go
type KnownWasmsCache struct {
    cache *lru.Cache[string, *string]  // hash -> protocol_id (nil = unknown)
    db    *sql.DB
    mu    sync.RWMutex
}

func (c *KnownWasmsCache) Lookup(ctx context.Context, hash []byte) (*string, bool, error) {
    key := hex.EncodeToString(hash)
    
    // Check LRU first (~1µs)
    if val, ok := c.cache.Get(key); ok {
        return val, true, nil
    }
    
    // Cache miss: query DB (~1-5ms)
    var protocolID *string
    err := c.db.QueryRowContext(ctx, 
        "SELECT protocol_id FROM known_wasms WHERE wasm_hash = $1", key).Scan(&protocolID)
    
    if err == sql.ErrNoRows {
        return nil, false, nil  // Not in DB at all
    }
    if err != nil {
        return nil, false, err
    }
    
    // Populate cache
    c.cache.Add(key, protocolID)
    return protocolID, true, nil
}
```

## Write-Through Current State Cache

When live ingestion first takes over current-state production for a protocol (its first successful CAS), it needs the current state to compute the next state. This is handled by a write-through in-memory cache, similar in pattern to the known_wasms LRU cache above.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WRITE-THROUGH CURRENT STATE CACHE                        │
└─────────────────────────────────────────────────────────────────────────────┘

                    Live Ingestion at Ledger N
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Check in-memory      │
                    │ state cache for      │
                    │ protocol             │
                    └──────────┬───────────┘
                               │
                   ┌───────────┴───────────┐
                   │                       │
              POPULATED                 EMPTY
                   │                       │
                   ▼                       ▼
          ┌──────────────┐      ┌──────────────────────┐
          │ Use cached   │      │ Read current state   │
          │ state to     │      │ from protocol state  │
          │ compute N    │      │ tables (one-time DB  │
          │              │      │ read at handoff)     │
          └──────────────┘      └──────────┬───────────┘
                   │                       │
                   │                       ▼
                   │            ┌──────────────────────┐
                   │            │ Populate in-memory   │
                   │            │ cache                │
                   │            └──────────┬───────────┘
                   │                       │
                   └───────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Compute new state    │
                    │ for ledger N         │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Update in-memory     │
                    │ cache + write to     │
                    │ protocol state       │
                    │ tables in DB         │
                    │ (write-through)      │
                    └──────────────────────┘
```

**Cache structure**:
```go
// Per-protocol current state cache
map[protocolID] -> {
    currentStateCursor uint32           // last ledger for which state was produced
    stateData          protocolState    // protocol-specific current state
}
```

**Lifecycle**:
- **Empty at start**: Cache is unpopulated when live ingestion starts
- **Populated from DB**: When live ingestion first successfully CAS-advances the cursor (handoff from migration), it reads current state from the protocol's state tables (one-time read)
- **Updated per ledger**: On each subsequent ledger, cache is updated in-memory and written through to DB
- **Lost on restart**: If live ingestion restarts, the cache is repopulated from DB on the next current-state production

## Backfill Migrations

Backfill migrations are split into two independent subcommands:
- `protocol-migrate history` — writes protocol state changes within the retention window
- `protocol-migrate current-state` — builds current state from a protocol's deployment ledger

Each subcommand converges independently with live ingestion via its own CAS cursor. They can run in any order, concurrently, or only one can be run.

### History Migration Command

```bash
./wallet-backend protocol-migrate history --protocol-id SEP50 SEP41
```

**Parameters**:
- `--protocol-id`: Protocol(s) to migrate (required)
- No `--start-ledger` — always reads `oldest_ledger_cursor` from `ingest_store`

The history migration runs until it converges with live ingestion. It processes batches from `oldest_ledger_cursor` toward the tip, CAS-advancing the history cursor with each batch commit. When a CAS fails (because live ingestion advanced the cursor first), the migration detects the handoff, sets `history_migration_status = 'success'`, and exits.

### Current-State Migration Command

```bash
./wallet-backend protocol-migrate current-state --protocol-id SEP50 --start-ledger 1000
```

**Parameters**:
- `--protocol-id`: Protocol(s) to migrate (required)
- `--start-ledger`: First ledger to process (required, based on protocol deployment)

The current-state migration runs until it converges with live ingestion. It processes ALL ledgers from `--start-ledger` toward the tip, CAS-advancing the current-state cursor with each batch commit. It writes only current state — no state changes. When a CAS fails, the migration detects the handoff, sets `current_state_migration_status = 'success'`, and exits.

### History Migration Workflow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HISTORY MIGRATION RUNNER WORKFLOW                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. VALIDATE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Verify protocol(s) exists in registry                                    │
│ - Verify classification_status = 'success'                                 │
│ - Set history_migration_status = 'in_progress'                             │
│ - Read oldest_ledger_cursor from ingest_store (retention window start)     │
│ - Initialize protocol_{ID}_history_cursor = oldest_ledger_cursor - 1       │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 2. PROCESS BATCHES TO TIP                                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ Loop:                                                                      │
│   a. Read latest_ledger_cursor to get target                               │
│   b. Split [cursor+1, target] into batches                                 │
│   c. Process batches in parallel with ordered commit                       │
│   d. Each batch commit:                                                    │
│      - CAS-advance protocol_{ID}_history_cursor                            │
│      - If CAS succeeds: write state changes                                │
│      - If CAS fails: handoff detected → go to step 3                       │
│   e. After all batches: re-read latest_ledger_cursor                       │
│   f. If more ledgers remain: repeat from (b)                               │
│   g. If no more ledgers: block on RPC for next ledger (~5s), repeat        │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                            CAS failure
                            (handoff)
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 3. COMPLETE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Verify cursor is at or past the ledger migration tried to write          │
│ - Set history_migration_status = 'success'                                 │
│ - Clean up migration resources                                             │
│ - Live ingestion now owns state change production for this protocol        │
└────────────────────────────────────────────────────────────────────────────┘
```

### Current-State Migration Workflow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 CURRENT-STATE MIGRATION RUNNER WORKFLOW                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. VALIDATE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Verify protocol(s) exists in registry                                    │
│ - Verify classification_status = 'success'                                 │
│ - Set current_state_migration_status = 'in_progress'                       │
│ - Initialize protocol_{ID}_current_state_cursor = start_ledger - 1         │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 2. PROCESS BATCHES TO TIP                                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ Loop:                                                                      │
│   a. Read latest_ledger_cursor to get target                               │
│   b. Split [cursor+1, target] into batches                                 │
│   c. Process batches in parallel with ordered commit                       │
│   d. Each batch commit:                                                    │
│      - CAS-advance protocol_{ID}_current_state_cursor                      │
│      - If CAS succeeds: write current state                                │
│      - If CAS fails: handoff detected → go to step 3                       │
│   e. After all batches: re-read latest_ledger_cursor                       │
│   f. If more ledgers remain: repeat from (b)                               │
│   g. If no more ledgers: block on RPC for next ledger (~5s), repeat        │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                            CAS failure
                            (handoff)
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 3. COMPLETE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Verify cursor is at or past the ledger migration tried to write          │
│ - Set current_state_migration_status = 'success'                           │
│ - Clean up migration resources                                             │
│ - Live ingestion now owns current-state production for this protocol       │
└────────────────────────────────────────────────────────────────────────────┘

ERROR HANDLING (applies to both subcommands):
┌────────────────────────────────────────────────────────────────────────────┐
│ If migration fails at any point:                                           │
│ - Set respective status column = 'failed'                                  │
│ - Log error details                                                        │
│ - Migration can be retried after fixing the issue                          │
│ - On restart: resume from the respective CAS cursor + 1                    │
│   (history_cursor for history, current_state_cursor for current-state)     │
└────────────────────────────────────────────────────────────────────────────┘

STATUS TRANSITIONS (per column):
┌────────────────────────────────────────────────────────────────────────────┐
│ classification_status:                                                     │
│   not_started → in_progress   (protocol-setup starts)                      │
│              → success        (protocol-setup completes)                   │
│              → failed         (error)                                      │
│                                                                            │
│ history_migration_status:                                                  │
│   not_started → in_progress   (protocol-migrate history starts)            │
│              → success        (CAS fails = live ingestion took over)       │
│              → failed         (error)                                      │
│                                                                            │
│ current_state_migration_status:                                            │
│   not_started → in_progress   (protocol-migrate current-state starts)      │
│              → success        (CAS fails = live ingestion took over)       │
│              → failed         (error)                                      │
└────────────────────────────────────────────────────────────────────────────┘
```

## Parallel Backfill Optimization

Protocol backfill migrations can process millions of ledgers. Sequential processing (ledger-by-ledger)
is slow because each ledger must wait for the previous to complete. This section describes how to
parallelize backfill migrations while preserving correctness.

The two migration subcommands have different parallelization characteristics:
- **History migration** (`protocol-migrate history`): State changes for ledger N do not depend on state changes for ledger N-1, so batches are truly independent. History migration can be more aggressively parallelized — batches can be processed and committed in any order without affecting correctness.
- **Current-state migration** (`protocol-migrate current-state`): Current state is order-dependent (see below), so batches must be committed in order even though they can be processed in parallel.

### Order-Dependent Current State Tracking

Current state tracking is **order-dependent** - the final state depends on the sequence of updates:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  ORDER DEPENDENCY EXAMPLE                                   │
└─────────────────────────────────────────────────────────────────────────────┘

Ledger 100: Contract CABC... added to protocol_contracts
Ledger 200: Contract CABC... metadata updated
Ledger 300: Contract CABC... removed from protocol_contracts

If current state written out of order:
  Ledger 300 first → contract removed from current state
  Ledger 100 next  → contract added back ← WRONG! Should be removed!

The final current state must reflect ledger 300's removal, not ledger 100's addition.
```

### Parallel Processing with Ordered Commit

The solution uses a **streaming ordered commit** pattern (required for current-state migration; history migration can use simpler unordered commit since state changes are independent):

1. **PARALLEL PHASE**: Process ledger batches concurrently (each batch gets isolated state)
2. **ORDERED COMMIT**: A committer goroutine writes completed batches to the database **in order**
3. **CURSOR TRACKING**: Each batch commit CAS-advances the respective cursor (history or current-state). If a CAS fails during any batch commit, migration detects that live ingestion has taken over and exits.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              PARALLEL BACKFILL WITH STREAMING ORDERED COMMIT                │
└─────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────────────────┐
                          │  Ledger Range        │
                          │  [1 ... 100,000]     │
                          └──────────┬───────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │ Split into batches of 1000     │
                    └────────────────┼────────────────┘
                                     │
         ┌──────────────┬────────────┼────────────┬──────────────┐
         │              │            │            │              │
         ▼              ▼            ▼            ▼              ▼
    ┌─────────┐   ┌─────────┐  ┌─────────┐  ┌─────────┐   ┌─────────┐
    │ Batch 1 │   │ Batch 2 │  │ Batch 3 │  │  ...    │   │Batch 100│
    │ [1-1000]│   │[1001-   │  │[2001-   │  │         │   │[99001-  │
    │         │   │ 2000]   │  │ 3000]   │  │         │   │ 100000] │
    └────┬────┘   └────┬────┘  └────┬────┘  └────┬────┘   └────┬────┘
         │              │            │            │              │
         ▼              ▼            ▼            ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    WORKER POOL (PARALLEL)                               │
│  Each worker:                                                           │
│  1. Creates isolated LedgerBackend                                      │
│  2. Creates isolated BatchBuffer                                        │
│  3. Processes ledgers sequentially within batch                         │
│  4. Generates output per subcommand:                                    │
│     - History: state changes for each ledger                            │
│     - Current-state: current state running totals                       │
│  5. Sends BatchResult to results channel                                │
└─────────────────────────────────────────────────────────────────────────┘
         │              │            │            │              │
         ▼              ▼            ▼            ▼              ▼
    ┌─────────┐   ┌─────────┐  ┌─────────┐  ┌─────────┐   ┌─────────┐
    │ Result  │   │ Result  │  │ Result  │  │ Result  │   │ Result  │
    │    1    │   │    2    │  │    3    │  │   ...   │   │   100   │
    └────┬────┘   └────┬────┘  └────┬────┘  └────┬────┘   └────┬────┘
         │              │            │            │              │
         └──────────────┴────────────┴────────────┴──────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │     ORDERED COMMIT BUFFER      │
                    │  (holds out-of-order results   │
                    │   until ready to commit)       │
                    │                                │
                    │  nextToCommit = 1              │
                    │  buffer[2] = Result 2 ✓        │
                    │  buffer[3] = Result 3 ✓        │
                    │  buffer[1] = waiting...        │
                    └────────────────┬───────────────┘
                                     │
                    When batch 1 arrives:
                    ┌────────────────┼────────────────┐
                    │ Commit 1, then 2, then 3        │
                    │ (sequential, in order)          │
                    └────────────────┼────────────────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         ▼                           ▼                           ▼
┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
│ COMMIT Batch 1   │       │ COMMIT Batch 2   │       │ COMMIT Batch 3   │
│ CAS cursor→1000  │  ──▶  │ CAS cursor→2000  │  ──▶  │ CAS cursor→3000  │
│ + batch data     │       │ + batch data     │       │ + batch data     │
│ (atomic tx)      │       │ (atomic tx)      │       │ (atomic tx)      │
└──────────────────┘       └──────────────────┘       └──────────────────┘
         │                           │                           │
         ▼                           ▼                           ▼
    CAS fail?                   CAS fail?                   CAS fail?
    No → continue              No → continue              No → continue
    Yes → handoff              Yes → handoff              Yes → handoff
```

**Crash Recovery**: If the process crashes after committing batch 2, the respective CAS cursor is at ledger 2000.
On restart, processing resumes from ledger 2001 — no work is lost. Each subcommand uses its own CAS cursor for crash recovery, eliminating the need for a separate migration cursor.

**Example**:

| Scenario | Behavior |
|----------|----------|
| Batches complete in order (1,2,3) | Each commits immediately after previous |
| Batch 3 finishes before batch 2 | Batch 3 buffered; when 2 arrives, both commit |
| Crash after batch 5 committed | Cursor at ledger 5000; resume from 5001 |
| Same entity in batch 1 and batch 3 | Accumulator merges using OperationID |

## API
There are currently 2 classes of APIs that will include data produced by migrations, history and current state APIs.

### History
Our history API will have access to enriched operations as a result of data migrations. Operations that are classified as belonging to a protocol will include a reference to the protocol(s) identified and may include state changes recorded as a result of the migration's state production.

While a migration is in progress, history may include classification and state changes for some operations and not others, this is not avoidable because any deployment that is live before a migration will already be serving history data. In this case, clients can accept the partial migration state or they can choose to not display enriched data for the migration until it has completed.

```
type Operation {
  id: Int64!
  operationType: OperationType!
  operationXdr: String!
  resultCode: String!
  successful: Boolean!
  ledgerNumber: UInt32!
  ledgerCreatedAt: Time!
  ingestedAt: Time!

  # Existing relationships
  transaction: Transaction! @goField(forceResolver: true)
  accounts: [Account!]! @goField(forceResolver: true)
  stateChanges(first: Int, after: String, last: Int, before: String): StateChangeConnection

  # NEW: Protocol invocations within this operation
  protocols: [OperationProtocol!]! @goField(forceResolver: true)
}

type OperationProtocol {
  protocol: Protocol!
  contractId: String!
}

type Protocol {
  id: String!
  classificationStatus: String!
  historyMigrationStatus: String!
  currentStateMigrationStatus: String!
}
```

### Current State
Some migrations will write to new tables that will represent the current state produced by a protocol in relation to accounts.
An example of this is SEP-50 Collectibles, where we will track collectible mints/transfers in order to maintain a table of collectibles owned by accounts.

The API exposes per-process status fields so clients can independently check whether each migration responsibility is complete. This pushes the responsibility to clients, keeping queries cleaner and faster.

**Client responsibility**:
- Check `historyMigrationStatus = 'success'` before relying on enriched operation history
- Check `currentStateMigrationStatus = 'success'` before relying on current state completeness
- Clients that query data during an in-progress migration may receive incomplete results

The `Operation.protocols` field exposes which protocols were involved in an operation.
The query path uses existing tables without requiring a dedicated mapping table:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Operation.protocols QUERY PATH                         │
└─────────────────────────────────────────────────────────────────────────────┘

GraphQL Query:
┌──────────────────────────────────────────────────────────────────────────┐
│ query {                                                                  │
│   operation(id: "12345") {                                               │
│     id                                                                   │
│     protocols {                                                          │
│       protocol { id, classificationStatus,                               │
│                  historyMigrationStatus,                                 │
│                  currentStateMigrationStatus }                           │
│       contractId                                                         │
│     }                                                                    │
│   }                                                                      │
│ }                                                                        │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           DATABASE QUERY                                 │
└──────────────────────────────────────────────────────────────────────────┘

SELECT DISTINCT p.id, pc.contract_id
FROM operations o
JOIN operations_accounts oa ON oa.operation_id = o.id
JOIN protocol_contracts pc ON pc.contract_id = oa.account_id
JOIN protocols p ON p.id = pc.protocol_id
WHERE o.id = $1;

                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           JOIN VISUALIZATION                             │
└──────────────────────────────────────────────────────────────────────────┘

┌────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────┐
│ operations │────▶│ operations_      │────▶│ protocol_         │────▶│ protocols │
│            │     │ accounts         │     │ contracts         │     │           │
├────────────┤     ├──────────────────┤     ├───────────────────┤     ├───────────┤
│ id         │     │ operation_id (FK)│     │ contract_id (PK)  │     │ id (PK)   │
│ ...        │     │ account_id       │     │ protocol_id (FK)  │     │ classifi- │
│            │     │                  │     │ name              │     │ cation_   │
│            │     │                  │     │                   │     │ status,   │
│            │     │                  │     │                   │     │ history_  │
│            │     │                  │     │                   │     │ migration │
│            │     │                  │     │                   │     │ _status,  │
│            │     │                  │     │                   │     │ current_  │
│            │     │                  │     │                   │     │ state_    │
│            │     │                  │     │                   │     │ migration │
│            │     │                  │     │                   │     │ _status   │
└────────────┘     └──────────────────┘     └───────────────────┘     └───────────┘
       │                    │                        │                      │
       │                    │                        │                      │
       ▼                    ▼                        ▼                      ▼
   operation.id    accounts involved          if account is a         protocol info
                   in this operation          protocol contract       for display
```

**Join Path Explanation**:

1. `operations` - The operation we're querying
2. `operations_accounts` - Contains all account IDs touched by the operation (sources, destinations, contract addresses)
3. `protocol_contracts` - Join on `account_id = contract_id` to find any protocol contracts involved
4. `protocols` - Get protocol display information

### Query Complexity & Performance

The `Operation.protocols` query has the following characteristics:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      QUERY PERFORMANCE ANALYSIS                             │
└─────────────────────────────────────────────────────────────────────────────┘

INDEXES REQUIRED:
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  operations_accounts:                                                      │
│    PRIMARY KEY (account_id, operation_id)  -- fast lookup by account       │
│    INDEX on (operation_id)                 -- fast lookup by operation     │
│                                                                            │
│  protocol_contracts:                                                       │
│    PRIMARY KEY (contract_id, protocol_id)  -- fast lookup by contract      │
│                                                                            │
│  protocols:                                                                │
│    PRIMARY KEY (id)                        -- fast lookup by id            │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘

QUERY COST BREAKDOWN (per operation):
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  Step 1: Find operation_id in operations_accounts                          │
│          Cost: O(log n) index scan                                         │
│          Rows: ~1-10 accounts per operation                                │
│                                                                            │
│  Step 2: For each account, check protocol_contracts                        │
│          Cost: O(log m) index scan per account                             │
│          Rows: 0-1 protocol contracts per account (usually 0)              │
│                                                                            │
│  Step 3: For each match, fetch protocol                                    │
│          Cost: O(1) primary key lookup                                     │
│          Rows: 1 per match                                                 │
│                                                                            │
│ Total: O(k * log m) where k = accounts in operation, m = protocol contracts│
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Client Handling of Migration Status

The API exposes per-process status fields on `Protocol` to allow clients to handle in-progress migrations appropriately.

**For historical data** (enriched operations with state changes):

1. **Accept partial data**: Display enriched data where available
2. **Wait for completion**: Check `historyMigrationStatus = 'success'` and defer display until complete

**For current state data**:

Current state is **progressively available** during migration — the current-state cursor advances incrementally as migration processes each ledger. However, until `currentStateMigrationStatus = 'success'`, the current state only reflects ledgers up to the cursor position and may not include recent activity.

- `in_progress`: Current state exists but may lag behind live activity. The cursor indicates how far the migration has progressed.
- `success`: Live ingestion has fully taken over current-state production. Current state is up-to-date and will stay current going forward.

Clients should check `currentStateMigrationStatus = 'success'` before relying on current state queries for completeness. Clients that can tolerate partial data may use current state during `in_progress` with the understanding that it reflects state up to the migration cursor, not necessarily the latest ledger.

Example query to check migration status:

```graphql
query {
  protocols {
    id
    classificationStatus
    historyMigrationStatus
    currentStateMigrationStatus
  }
}
```