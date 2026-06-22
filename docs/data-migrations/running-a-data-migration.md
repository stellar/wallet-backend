# Running a Data Migration

This guide covers the commands for running the data migration workflow after a new protocol has been added to the codebase and deployed. For how to write the code for a new protocol, see [Adding a Protocol](./adding-a-protocol.md).

## Prerequisites

- A running PostgreSQL database
- A running Stellar RPC instance
- The protocol code (validator, processor, registration) is compiled into the binary

## Operator Flags

Infrastructure config (`--database-url`, `--rpc-url`, `--network-passphrase`, etc.) is provided via environment variables or deploy configuration. The operator-facing flags for migration commands are:

| Flag | Description | Example |
|------|-------------|---------|
| `--protocol-id` | Protocol ID to operate on (required, repeatable) | `--protocol-id SEP41` |
| `--log-level` | Log verbosity (optional) | `DEBUG` |

## Migration Workflow

The workflow runs in order: apply schema migrations, restart ingestion, then classify the backlog and backfill state. Restarting ingestion **before** `protocol-setup` is deliberate — live ingestion starts classifying new-protocol WASMs the moment it restarts, so no deployment can slip through the window between `protocol-setup`'s backlog snapshot and ingestion picking up the new validator.

```
Step 1             Step 2                Step 3                Step 4 (concurrent)
migrate up   ───>  restart ingestion ──> protocol-setup  ───>  protocol-migrate history
(schema +          (classifies new       (classifies            protocol-migrate current-state
 registration)      WASMs inline)         existing backlog)     (converge with live ingestion via CAS)
```

### Step 1: Schema Migration

Apply pending schema migrations. A full `migrate up` also registers the new protocol in the `protocols` table, so live ingestion can classify the protocol's WASMs as soon as it restarts.

```bash
go run main.go migrate up
```

This applies pending schema migrations from `internal/db/migrations/` and runs the idempotent protocol registration SQL from `internal/db/migrations/protocols/`. Partial runs (`migrate up N`) skip protocol registration, since the `protocols` table may not exist yet.

### Step 2: Restart Live Ingestion

Restart the ingestion service so it picks up the new protocol processor from the registry. No special flags are needed -- just restart the existing `ingest` process with its current configuration. Because Step 1 registered the protocol, ingestion classifies new-protocol WASMs inline from the moment it restarts.

Live ingestion uses CAS cursors to coordinate with the migration subcommands. It only produces state for ledgers where the migration hasn't already written data. Once the history and current-state migrations converge with live ingestion (their CAS operations start failing because live ingestion has already advanced the cursor), they exit automatically.

### Step 3: Protocol Setup

Classifies the existing unclassified WASMs already recorded in `protocol_wasms` -- the backlog ingestion captured before the new validator existed. Running it after the ingestion restart makes the two classification windows overlap: live ingestion covers everything from the restart forward, `protocol-setup` covers the backlog up to its snapshot, and there is no gap between them.

```bash
go run main.go protocol-setup --protocol-id <PROTOCOL_ID>
```

What it does:
- Fetches all unclassified WASM bytecodes from the network via RPC
- Validates each WASM against registered protocol validators
- Populates `protocol_wasms` and `protocol_contracts` tables
- Initializes CAS cursors for both history and current-state migrations
- Re-runs the protocol registration SQL as an idempotent safeguard, so the command remains self-sufficient if run standalone

You can set up multiple protocols at once:

```bash
go run main.go protocol-setup --protocol-id SEP41 --protocol-id BLEND
```

### Step 4a: History Migration

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
| `--oldest-ledger-cursor-name` | `oldest_ingest_ledger` | Name of the oldest ledger cursor in the ingest store. Must match the value used by the ingest service. |

### Step 4b: Current-State Migration

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
