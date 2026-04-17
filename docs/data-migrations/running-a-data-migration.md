# Running a Data Migration

This guide covers the commands for running the data migration workflow after a new protocol has been added to the codebase and deployed. For how to write the code for a new protocol, see [Adding a Protocol](./adding-a-protocol.md).

## Prerequisites

- A running PostgreSQL database with schema migrations applied
- A running Stellar RPC instance
- The protocol code (validator, processor, registration) is compiled into the binary

## Operator Flags

Infrastructure config (`--database-url`, `--rpc-url`, `--network-passphrase`, etc.) is provided via environment variables or deploy configuration. The operator-facing flags for migration commands are:

| Flag | Description | Example |
|------|-------------|---------|
| `--protocol-id` | Protocol ID to operate on (required, repeatable) | `--protocol-id SEP41` |
| `--log-level` | Log verbosity (optional) | `DEBUG` |

## Migration Workflow

The migration runs in two phases: a setup step, then three concurrent processes.

```
Phase 1 (sequential)          Phase 2 (concurrent)
┌─────────────────────┐       ┌──────────────────────────────────────────────┐
│ 1. protocol-setup   │──────>│ 2.  Restart live ingestion                  │
│                     │       │ 3a. protocol-migrate history                │
│                     │       │ 3b. protocol-migrate current-state          │
└─────────────────────┘       └──────────────────────────────────────────────┘
```

### Step 1: Protocol Setup

Classifies existing contracts on the network against your protocol's validator. This must complete before starting the other steps.

```bash
go run main.go protocol-setup --protocol-id <PROTOCOL_ID>
```

What it does:
- Runs protocol migration SQL (registers the protocol in the `protocols` table)
- Fetches all unclassified WASM bytecodes from the network via RPC
- Validates each WASM against registered protocol validators
- Populates `protocol_wasms` and `protocol_contracts` tables
- Initializes CAS cursors for both history and current-state migrations

You can set up multiple protocols at once:

```bash
go run main.go protocol-setup --protocol-id SEP41 --protocol-id BLEND
```

### Step 2: Restart Live Ingestion

Restart the ingestion service so it picks up the new protocol processor from the registry. No special flags are needed -- just restart the existing `ingest` process with its current configuration.

Live ingestion uses CAS cursors to coordinate with the migration subcommands. It only produces state for ledgers where the migration hasn't already written data. Once the history and current-state migrations converge with live ingestion (their CAS operations start failing because live ingestion has already advanced the cursor), they exit automatically.

### Step 3a: History Migration

Backfills historical state changes within the retention window:

```bash
go run main.go protocol-migrate history --protocol-id <PROTOCOL_ID>
```

What it does:
- Walks forward from the oldest ingestion cursor to the latest
- Calls `PersistHistory` for each ledger
- Converges with live ingestion via the history CAS cursor
- When its CAS fails (cursor already advanced by live ingestion), sets `history_migration_status = success` and exits

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--latest-ledger-cursor-name` | `latest_ingest_ledger` | Name of the latest ledger cursor in the ingest store. Must match the value used by the ingest service. |
| `--oldest-ledger-cursor-name` | `oldest_ingest_ledger` | Name of the oldest ledger cursor in the ingest store. Must match the value used by the ingest service. |

### Step 3b: Current-State Migration

Builds current state from a specified start ledger forward to the tip. Only needed if your protocol tracks current-state data.

```bash
go run main.go protocol-migrate current-state \
  --protocol-id <PROTOCOL_ID> \
  --start-ledger <LEDGER>
```

The `--start-ledger` should be set to the ledger where the first contract implementing your protocol was deployed. This avoids processing ledgers that have no relevant data.

What it does:
- Processes ledgers from `--start-ledger` forward to the current tip
- Calls `PersistCurrentState` for each ledger
- Converges with live ingestion via the current-state CAS cursor
- Exits when CAS convergence is reached

## Monitoring

All three concurrent processes (live ingestion, history migration, current-state migration) log their progress. Key things to watch:

- **CAS convergence**: When a migration process logs that its CAS operation failed, it means live ingestion has caught up to that point. The migration will exit with a success status.
- **Protocol status transitions**: Each protocol tracks `history_migration_status` and `current_state_migration_status` in the `protocols` table. These transition from `not_started` -> `in_progress` -> `success`.
- **Error recovery**: If a migration process crashes, it can be safely restarted. It will resume from the last CAS cursor position.

## Database Schema Migration

Before running the data migration workflow, ensure the database schema is up to date:

```bash
go run main.go migrate up
```

This applies any pending schema migrations from `internal/db/migrations/`. The `protocol-setup` command handles protocol-specific migrations (from `internal/db/migrations/protocols/`) automatically.
