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

## Rebuilding a Protocol

When a bug fix changes what a protocol derives, the rows already in the database stay wrong: current-state columns like SEP-41 balances are running totals folded from events, never read back from contract state. `--rebuild` discards the protocol's rows and replays them through the fixed code.

```bash
go run main.go protocol-migrate current-state --protocol-id <PROTOCOL_ID> --start-ledger <LEDGER> --rebuild
go run main.go protocol-migrate history       --protocol-id <PROTOCOL_ID> --rebuild
```

Both commands are **destructive** and run the same sequence: validate, take the lock, wipe, then run the normal migration. Live ingestion keeps running throughout; the protocol serves empty or partial data until the rebuild reaches the live frontier and hands off.

### What each wipe clears

Each wipe covers only the protocol named by `--protocol-id`. Rebuilding SEP41 clears the `sep41_*` tables and touches no other protocol's rows.

| Command | Wipes, for that protocol only | Cursor reset to |
|---------|-------------------------------|-----------------|
| `current-state --rebuild` | its own current-state tables — `sep41_balances` and `sep41_allowances` for SEP41 | `--start-ledger` − 1 |
| `history --rebuild` | its `state_changes` rows across the retained window | oldest retained ledger − 1 |

`contract_tokens`, `protocol_wasms` and `protocol_contracts` are never touched. Nothing rebuilds classification, and the re-migration needs it to know which contracts belong to the protocol. Re-run `protocol-setup` if classification itself is wrong.

The cursor row is updated, never deleted — live ingestion treats a missing cursor row as a fatal incident.

### Ordering

The two rebuilds differ in when the wipe happens relative to the cursor reset, because the constraint differs:

- **Current-state**: reset and wipe commit in one transaction, so live can never fold onto a half-wiped table. The cursor row that transaction holds is the one live's per-ledger CAS needs, and live writes all protocols in a single transaction per ledger — so this briefly stalls ingestion for every protocol. The wipe truncates, which keeps that stall independent of how many rows are being discarded.
- **History**: the reset commits *first*. That makes live's CAS fail, so live stops writing the protocol's history and the deletes that follow race nothing. The deletes then run in 10k-ledger slices, one transaction each, because `state_changes` is compressed and a full-window delete would decompress unbounded data.

### Safety

- A protocol marked `in_progress` is refused: that is residue from a dead run, which should be investigated rather than wiped under.
- One advisory lock per protocol per strategy. A current-state rebuild and a history rebuild for the same protocol may run concurrently; two of the same kind may not, and a plain migration takes the same lock.
- Reruns after a failure are safe. The wipes are idempotent and re-derived rows land at deterministic `state_change_id`s.
- Rebuild time grows with the chain — expect hundreds of ledgers per second from the datastore.

## Hiding a token

Add a Soroban token contract to the hidden-contracts list when the API should not return it. Ingestion is not affected: the contract's current state and history are still written, `serve` just leaves them out. Hidden means the API returns none of the current state or state changes that the entry's protocol wrote for the contract. A transaction or operation that touched the account only through the hidden token is still listed, with no state changes under it. There are two reasons to do this:

- It matches a protocol's interface, but its balances cannot be derived from its events the way the processor derives them. The Comet LP pool `CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM` is on the built-in list for this reason.
- It is a malicious token you do not want shown.

To add one, put an entry in your list and restart `serve`. The list is read at startup only.

The binary ships with a built-in list. Setting `HIDDEN_CONTRACTS_FILE` (or `--hidden-contracts-file`) points at your own file, which replaces the built-in list entirely. Copy any built-in entries you want to keep.

To remove one, delete the entry and restart `serve`. Everything written while it was hidden is already there, so nothing needs rebuilding.

File format:

```yaml
hidden_contracts:
  - contract_id: C...
    protocol: SEP41
    reason: why it is hidden, and what would let it be shown again
    added: 2026-10-07
```

`contract_id`, `protocol` and `reason` are required; `added` is informational. An entry hides the rows that one protocol's processor wrote for the contract: that protocol's current state and the state changes in its range. `SEP41` is the only protocol today. When a dedicated processor later takes the token over, its rows show while the old processor's rows stay hidden, with nothing to delete. A token tracked by two processors at once, for example during a backfill handover, gets one entry per protocol. A bad address, a missing or unknown protocol, an empty reason, the same contract and protocol listed twice, an unknown field or a missing `hidden_contracts` key stops `serve` at startup.

## Monitoring

All three concurrent processes (live ingestion, history migration, current-state migration) log their progress. Key things to watch:

- **CAS convergence**: When a migration process logs that its CAS operation failed, it means live ingestion has caught up to that point. The migration will exit with a success status.
- **Protocol status transitions**: Each protocol tracks `history_migration_status` and `current_state_migration_status` in the `protocols` table. These transition from `not_started` -> `in_progress` -> `success`.
- **Error recovery**: If a migration process crashes, it can be safely restarted. It will resume from the last CAS cursor position.
