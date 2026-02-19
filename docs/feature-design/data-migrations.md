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
    migration_status TEXT DEFAULT 'not_started',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- migration_status values:
-- 'not_started'               - Initial state after registration
-- 'classification_in_progress' - Checkpoint classification running
-- 'classification_success'    - Checkpoint classification complete
-- 'backfilling_in_progress'   - Historical state migration running
-- 'backfilling_success'       - Migration complete, data is complete
-- 'failed'                    - Migration failed
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

-- Protocol migration cursor example:
INSERT INTO ingest_store (key, value) VALUES ('protocol_SEP41_migration_cursor', '50000');
```

Each protocol migration has its own cursor key (e.g., `protocol_{PROTOCOL_ID}_migration_cursor`).
This cursor is updated atomically with each batch commit for crash recovery and can be deleted after the migration completes.

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

Adding a new protocol requires three coordinated processes:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      PROTOCOL ONBOARDING WORKFLOW                               │
└─────────────────────────────────────────────────────────────────────────────────┘

   STEP 1: SETUP             STEP 2: LIVE INGESTION         STEP 3: BACKFILL
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ ./wallet-backend     │    │ Restart ingestion    │    │ ./wallet-backend     │
│ protocol-setup       │───▶│ with new processor   │───▶│ protocol-migrate     │
│                      │    │                      │    │                      │
│ Classifies existing  │    │ Note the restart     │    │ Backfills historical │
│ contracts            │    │ ledger number        │    │ state                │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
       ▲                            │                           │
       │                            │                           │
       │                            ▼                           │
       │                    ┌──────────────────┐                │
       │                    │ Live ingestion   │                │
       │                    │ produces state   │                │
       │                    │ from restart     │                │
       │                    │ ledger onward    │                │
       │                    └──────────────────┘                │
       │                                                        │
       └────────────────────────────────────────────────────────┘
                    Complete coverage: [first_block → current]
```

## Process Dependencies

| Step | Requires | Produces |
|------|----------|----------|
| **1. protocol-setup** | Protocol migration SQL file, protocol implementation in code | Protocol in DB, `known_wasms`, `protocol_contracts`, status = `classification_success` |
| **2. ingest (live)** | Status = `classification_success`, processor registered | State from `restart_ledger` onward |
| **3. protocol-migrate** | `protocol_contracts` populated, status = `classification_success` | Historical state from `first_block` to `restart_ledger - 1` |

Both live ingestion and backfill migration need the `protocol_contracts` table populated to know which contracts to process. The `protocol-setup` command ensures this data exists before either process runs.

## Classification
Classification is the act of identifying new and existing contracts on the network and assigning a relationship to a known protocol.
This has to happen in 2 stages during the migration process:
- checkpoint population: We will use a history archive from the latest checkpoint in order to classify all contracts on the network. We will rely on the latest checkpoint available at the time of the migration.
- live ingestion: during live ingestion, we classify new contracts by watching for contract deployments/upgrades and comparing the wasm blob to the known protocols.

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
           ┌────────────────────────┼────────────────────────┐
           │                        │                        │
           ▼                        ▼                        ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────────┐
│ LedgerEntryType  │   │ LedgerEntryType  │   │ LedgerEntryType          │
│ ContractCode     │   │ ContractData     │   │ ContractData             │
│                  │   │ (Instance)       │   │ (Balance)                │
└────────┬─────────┘   └────────┬─────────┘   └────────────┬─────────────┘
         │                      │                          │
         ▼                      ▼                          ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────────┐
│ Extract WASM     │   │ Check SAC?       │   │ Extract holder address   │
│ bytecode + hash  │   │ (AssetFromData)  │   │ from Balance key         │
└────────┬─────────┘   └────────┬─────────┘   └────────────┬─────────────┘
         │                      │                          │
         ▼                      │                          ▼
┌──────────────────┐            │             ┌──────────────────────────┐
│ Group contracts  │    ┌───────┴───────┐     │ Track in                 │
│ by WASM hash     │    │               │     │ contractTokensByHolder   │
│                  │    ▼               ▼     │ Address map              │
└────────┬─────────┘   YES             NO     └──────────────────────────┘
         │              │               │
         ▼              ▼               ▼
┌──────────────────┐ ┌────────┐  ┌──────────────┐
│ Validate WASM    │ │SAC     │  │Compare WASM  │
│ against protocol │ │contract│  │to known      │
│ spec             │ │        │  │protocol spec │
└────────┬─────────┘ └────────┘  └──────────────┘
         │
    ┌────┴────┐
    │         │
  MATCH    NO MATCH
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│Insert  │ │Skip      │
│to      │ │(unknown) │
│protocol│ │          │
│_contracts└──────────┘
└────────┘

                    ┌───────────────────────────────┐
                    │ Post-Processing:              │
                    │ 1. Store in protocol_contracts│
                    │ 2. Cache in known_wasms       │
                    └───────────────────────────────┘
```

Contracts are grouped by WASM hash before validation. This means we validate each unique WASM blob once, then apply the result to all contracts using that same code.
Once a WASM hash is classified, it is stored in the `known_wasms` table to avoid re-classification of future contracts using the same code.

During live ingestion, new contracts are classified when they appear in ledger changes. The key difference from checkpoint population is that live ingestion watches for contract deployments/upgrades and compares the WASM blob to known protocols.

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
                   │ (iterate transaction changes)│
                   └──────────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ Watch for:                   │
                   │ - Contract deployments       │
                   │ - Contract upgrades          │
                   └──────────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ Extract WASM bytecode        │
                   │ from deployment/upgrade      │
                   └──────────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ Check known_wasms table:     │
                   │ Is this WASM hash known?     │
                   └──────────────┬───────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
                  ▼                               ▼
         ┌──────────────┐                ┌──────────────────┐
         │ WASM hash    │                │ WASM hash        │
         │ already known│                │ NOT known        │
         │              │                │                  │
         │ Use cached   │                │ Compare WASM to  │
         │classification│                │ protocol specs   │
         └──────┬───────┘                └────────┬─────────┘
                │                                  │
                │                         ┌───────┴───────┐
                │                         │               │
                │                       MATCH         NO MATCH
                │                         │               │
                │                         ▼               ▼
                │                 ┌──────────────┐  ┌──────────────┐
                │                 │ Insert to    │  │ Mark as      │
                │                 │ known_wasms  │  │ unknown in   │
                │                 │ + protocol_  │  │ known_wasms  │
                │                 │ contracts    │  │              │
                │                 └──────┬───────┘  └──────────────┘
                │                        │
                └────────────────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ Insert contract mapping to   │
                   │ protocol_contracts table     │
                   └──────────────────────────────┘
```

The classifier compares the WASM blob from new deployments/upgrades against known protocol specifications. 
This comparison uses the same validation logic as checkpoint population:

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
│                                                                   │
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
│                                                                   │
│   reader := bytes.NewReader(specBytes)                           │
│   for reader.Len() > 0 {                                         │
│       var spec xdr.ScSpecEntry                                   │
│       xdr.Unmarshal(reader, &spec)                               │
│       specs = append(specs, spec)                                │
│   }                                                              │
│                                                                   │
│   Each ScSpecEntry represents:                                   │
│   - Function definitions (name, inputs, outputs)                 │
│   - Type definitions (structs, enums)                            │
│   - Error definitions                                            │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 4: Protocol Signature Validation                            │
│                                                                   │
│   For each function in contractSpec:                             │
│   - Extract function name                                        │
│   - Extract input parameters (name → type mapping)               │
│   - Extract output types                                         │
│   - Compare against protocol's required functions                │
│                                                                   │
│   Example (SEP-41 Token Standard):                               │
│   - Required: balance, allowance, decimals, name, symbol,        │
│               approve, transfer, transfer_from, burn, burn_from  │
│   - All parameter names and types must match exactly             │
│                                                                   │
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

When a new protocol is registered, previously unknown WASM hashes (those with `protocol_id = NULL`) must be re-validated against the new protocol's spec. This ensures contracts deployed before the protocol was added can still be classified correctly.
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
                    │ Restart Live Ingestion      │
                    │ (triggers checkpoint        │
                    │ population for new protocol)│
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
Both backfill migrations and live ingestion will rely on checkpoint population being complete before they can produce state changes for a new protocol. 

If checkpoint population does not run before live ingestion is processing a new protocol, live ingestion will potentially experience more pressure from the additional classification it has to do due to the missing seed of `protocol_contracts`.

If checkpoint population does not run before a backfill migration is started for a new protocol, backfill migration will fail and exit since it does not classify protocols and cannot produce state without any classification being available.

### Command

```bash
./wallet-backend protocol-setup --protocol-id SEP50 --protocol-id BLEND
```

### What It Does

1. **Runs protocol migrations** - Executes SQL migrations from `internal/data/migrations/protocols/` to register new protocols in the `protocols` table with status `not_started`
2. **Sets status** to `classification_in_progress` for specified protocols
3. **Reads the latest checkpoint** from the history archive
4. **Extracts all WASM code** from contract entries in the checkpoint
5. **Queries existing unclassified entries** from `known_wasms WHERE protocol_id IS NULL`
6. **Validates each WASM** against all specified protocols' validators
7. **Populates tables**:
   - `known_wasms`: Maps WASM hashes to protocol IDs
   - `protocol_contracts`: Maps contract IDs to protocols
8. **Updates status** to `classification_success` for all processed protocols

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
- Consistent with `protocol-migrate` command interface

## State Production
State produced by new protocols is done through dual processes in order to cover historical state and new state production during live ingestion.
- Historical state: A backfill style migration will run for all ledgers that are needed to produce historical state enrichment, as well as current state tracking.
- Live ingest state: live ingestion will produce state defined by a protocol, this state can be an enrichment for an operation(richer data for history) and/or can be an update to the tracking of the current-state of a protocol as it relates to a user(which collectibles does a user own?).

### Backfill Migration
The migration runner processes historical ledgers to enrich operations with protocol state and produce state changes/current state.
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BACKFILL MIGRATION FLOW                              │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌────────────────────────────┐
                    │ ./wallet-backend           │
                    │ protocol-migrate           │
                    │ --protocol-id SEP50 ...    │
                    │ --start-ledger 1           │
                    │ --end-ledger 5             │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ MigrationRunner.Run()      │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Start()                    │
                    │ - Validate protocol exists │
                    │ - Set status = backfilling │
                    │   _in_progress             │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ For each ledger in range:  │
                    │(start-ledger to end-ledger)│
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Use processor to:          │
                    │ - Find operations involving│
                    │   protocol contracts       │
                    │ - Produce state            │
                    │ - Enrich historical data   │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Complete()                 │
                    │ - Set status =             │
                    │   backfilling_success      │
                    └────────────────────────────┘


MIGRATION DEPENDENCIES:
┌────────────────────────────────────────────────────────────────────────────┐
│ The migration has an explicit dependency on protocol-setup,                │
│ and an implicit dependency on live-ingestion                               |                                            
│ 1. Live ingestion must be running with the same processor                  │
│ 2. Checkpoint population must have completed for the protocol              │
│ 3. Migration processes: start-ledger → (live ingestion start - 1)          │
│ 4. Live ingestion continues from its start point onward                    │
│                                                                            │
│ This ensures no ledger gap between backfill and live ingestion.            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Live State Production

During live ingestion, two related but distinct processes run:
1. **Classification** - Identifies and classifies new contracts as they are deployed
2. **State Production** - Produces protocol-specific state using registered processors

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LIVE INGESTION: TWO RESPONSIBILITIES                     │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────────┐
                              │ LedgerCloseMeta  │
                              │ (from RPC)       │
                              └────────┬─────────┘
                                       │
                  ┌────────────────────┴────────────────────┐
                  │                                         │
                  ▼                                         ▼
┌─────────────────────────────────────┐   ┌─────────────────────────────────────┐
│         1. CLASSIFICATION           │   │        2. STATE PRODUCTION          │
│                                     │   │                                     │
│  Watch for contract deployments/    │   │  Run protocol processors on         │
│  upgrades in ledger changes         │   │  transactions in ledger             │
└─────────────────┬───────────────────┘   └─────────────────┬───────────────────┘
                  │                                         │
                  ▼                                         ▼
┌─────────────────────────────────────┐   ┌─────────────────────────────────────┐
│  For each new contract:             │   │  For each protocol processor:       │
│  ┌───────────────────────────────┐  │   │  ┌───────────────────────────────┐  │
│  │ 1. Check known_wasms cache    │  │   │  │ Processor.Process(ledger)     │  │
│  │ 2. If not known → validate    │  │   │  │                               │  │
│  │    WASM against protocol specs│  │   │  │ - Examines transactions       │  │
│  │ 3. Update known_wasms +       │  │   │  │ - Produces protocol-specific  │  │
│  │    protocol_contracts         │  │   │  │   state changes               │  │
│  └───────────────────────────────┘  │   │  └───────────────────────────────┘  │
└─────────────────┬───────────────────┘   └─────────────────┬───────────────────┘
                  │                                         │
                  └────────────────────┬────────────────────┘
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

## Backfill Migrations

Backfill migrations build current state and/or write state changes according to the logic defined in the processor for the protocol being migrated.

The `protocol-migrate` command accepts a set of protocol IDs for an explicit signal to migrate those protocols. Each protocol migration requires a specific range, which may not be exactly what other migrations need even if they are implemented at the same time. Migrations that do share a ledger range can run in one process.

### Migration Command

```bash
./wallet-backend protocol-migrate --protocol-id SEP50 SEP41 --start-ledger 1 --end-ledger 5
```

**Parameters**:
- `--protocol-id`: The protocol(s) to migrate (must exist in `protocols` table)
- `--start-ledger`: First ledger to process
- `--end-ledger`: Last ledger to process (should be the ledger before live ingestion started)

### Migration Workflow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       MIGRATION RUNNER WORKFLOW                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. VALIDATE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Verify protocol(s) exists in registry                                    │                                     
│ - Verify migration_status = 'classification_success'                       │
│ - Set migration_status = 'backfilling_in_progress'                         │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 2. PROCESS LEDGER RANGE                                                    │
├────────────────────────────────────────────────────────────────────────────┤
│ For ledger = start-ledger to end-ledger:                                   │
│   - Fetch ledger data (from archive or RPC)                                │
│   - Run processor to find protocol operations                              │
│   - Produce state changes / current state                                  │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 3. COMPLETE                                                                │
├────────────────────────────────────────────────────────────────────────────┤
│ - Set migration_status = 'backfilling_success'                             │
│ - Current state APIs now serve this protocol's data                        │
└────────────────────────────────────────────────────────────────────────────┘

ERROR HANDLING:
┌────────────────────────────────────────────────────────────────────────────┐
│ If migration fails at any point:                                           │
│ - Set migration_status = 'failed'                                          │
│ - Log error details                                                        │
│ - Migration can be retried after fixing the issue                          │
└────────────────────────────────────────────────────────────────────────────┘
```

## Parallel Backfill Optimization

Protocol backfill migrations can process millions of ledgers. Sequential processing (ledger-by-ledger)
is slow because each ledger must wait for the previous to complete. This section describes how to
parallelize backfill migrations while preserving the correctness of order-dependent current state tracking.

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

The solution uses a **streaming ordered commit** pattern:

1. **PARALLEL PHASE**: Process ledger batches concurrently (each batch gets isolated state)
2. **ORDERED COMMIT**: A committer goroutine writes completed batches to the database **in order**
3. **CURSOR TRACKING**: Each batch commit updates the migration cursor for crash recovery

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
│  4. Sends BatchResult to results channel                                │
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
                    │ Commit 1, then 2, then 3       │
                    │ (sequential, in order)          │
                    └────────────────┼────────────────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         ▼                           ▼                           ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ COMMIT Batch 1  │       │ COMMIT Batch 2  │       │ COMMIT Batch 3  │
│ cursor = 1000   │  ──▶  │ cursor = 2000   │  ──▶  │ cursor = 3000   │
│ (atomic tx)     │       │ (atomic tx)     │       │ (atomic tx)     │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │                           │                           │
         ▼                           ▼                           ▼
      Crash?                      Crash?                      Crash?
    Resume @ 1                  Resume @ 1001               Resume @ 2001
```

**Crash Recovery**: If the process crashes after committing batch 2, the cursor is at ledger 2000.
On restart, processing resumes from ledger 2001 - no work is lost.

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
  displayName: String!
}
```

### Current State
Some migrations will write to new tables that will represent the current state produced by a protocol in relation to accounts.
An example of this is SEP-50 Collectibles, where we will track collectible mints/transfers in order to maintain a table of collectibles owned by accounts.

Current state APIs should use `protocols.migration_status` in order to reject queries before the migration for that data type has completed. Some protocols require a complete view of the protocols history in order to correctly represent current state.

Example error for in-progress migration:

```json
{
  "errors": [
    {
      "message": "BLEND protocol data is being migrated; please try again later",
      "extensions": {
        "code": "PROTOCOL_NOT_READY",
        "protocol": "BLEND",
        "migration_status": "backfilling_in_progress"
      }
    }
  ],
  "data": null
}
```

The `Operation.protocols` field exposes which protocols were involved in an operation.
The query path uses existing tables without requiring a dedicated mapping table:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Operation.protocols QUERY PATH                         │
└─────────────────────────────────────────────────────────────────────────────┘

GraphQL Query:
┌──────────────────────────────────────────────────────────────────────────┐
│ query {                                                                   │
│   operation(id: "12345") {                                               │
│     id                                                                   │
│     protocols {                                                          │
│       protocol { id, displayName }                                       │
│       contractId                                                         │
│     }                                                                    │
│   }                                                                      │
│ }                                                                        │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           DATABASE QUERY                                  │
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
│                           JOIN VISUALIZATION                              │
└──────────────────────────────────────────────────────────────────────────┘

┌────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────┐
│ operations │────▶│ operations_      │────▶│ protocol_         │────▶│ protocols │
│            │     │ accounts         │     │ contracts         │     │           │
├────────────┤     ├──────────────────┤     ├───────────────────┤     ├───────────┤
│ id         │     │ operation_id (FK)│     │ contract_id (PK)  │     │ id (PK)   │
│ ...        │     │ account_id       │     │ protocol_id (FK)  │     │ migration │
│            │     │                  │     │ name              │     │ _status   │
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
│    INDEX on (migration_status)             -- filter by status             │
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

During migration, historical data may be partially enriched. Clients can:

1. **Accept partial data**: Display enriched data where available
2. **Wait for completion**: Check `protocols.migration_status` and defer display until `'backfilling_success'`

For current state APIs, queries should return an error if the protocol migration is not complete:

```json
{
  "errors": [
    {
      "message": "BLEND protocol data is being migrated; please try again later",
      "extensions": {
        "code": "PROTOCOL_NOT_READY",
        "protocol": "BLEND",
        "migration_status": "backfilling_in_progress"
      }
    }
  ],
  "data": null
}
```