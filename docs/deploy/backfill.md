# Backfill

Step 5 of [deploying wallet-backend](README.md), optional. After it the database holds every ledger from the one you choose up to the ledger live ingestion started from, with no holes.

## Contents

- [Backfill history](#backfill-history)
- [What a run does](#what-a-run-does)
- [Sizing and tuning](#sizing-and-tuning)
- [What to watch](#what-to-watch)
- [Failures and reruns](#failures-and-reruns)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](configuration.md).

## Backfill history

Live ingestion starts at the checkpoint ledger it bootstrapped from. To serve older history, backfill the range below it.

Live ingestion must have run once before you start. The backfill reads `latest_ingest_ledger` and `oldest_ingest_ledger` from `ingest_store` and refuses to start when either is missing.

1. Read the current range.

   ```sql
   SELECT key, value FROM ingest_store WHERE key IN ('oldest_ingest_ledger', 'latest_ingest_ledger');
   ```

2. Check the limits the run enforces.

   - `DB_MAX_CONNS` is at least 5 + `BACKFILL_COMPRESS_CONCURRENCY` + 1. The default pool of 12 covers the default concurrency of 5.
   - The retention policy keeps the whole range. The run refuses to start when `RETENTION_PERIOD` would drop `START_LEDGER`. Raise retention on the live ingester first.
   - No other backfill is running for the same network.

3. Run a backfill alongside the live ingester. Set `START_LEDGER` and no end. The run always fills up to the history the live ingester already holds, and it rejects `END_LEDGER`.

   ```bash
   INGESTION_MODE=backfill \
   START_LEDGER=<FIRST_LEDGER> \
   LEDGER_BACKEND_TYPE=datastore \
   DATASTORE_BUCKET_PATH=<BUCKET_PATH> \
   INGEST_SERVER_PORT=<FREE_PORT> \
     wallet-backend ingest
   ```

   Use the datastore source for any range outside the RPC's retention window. Give the backfill its own `INGEST_SERVER_PORT` when it shares a host with the live ingester.

4. Verify. Each committed window logs one line, and a finished run exits with status 0. `wallet_ingestion_oldest_ledger` on the backfill's `/ingest-metrics` drops toward `START_LEDGER` as windows commit, and `wallet_ingestion_backfill_windows_remaining` counts down to 0.

Measure the backfill rate on your hardware. The per-window log line reports ledgers per second. As a floor for comparison: live pubnet ingest on 2026-10-01 averaged 0.16 s per ledger (p99 0.44 s) with 4 CPU / 8 GiB for the ingest process, against PostgreSQL 17.6 and TimescaleDB 2.28.2 on 8 CPU / 32 GiB hosts.

## What a run does

A backfill makes every ledger from `START_LEDGER` to `latest_ingest_ledger` present. It works in this order.

1. **Holes above the cursor.** It fills any hole between `oldest_ingest_ledger` and the live tip first.
2. **History below the cursor.** It then extends history one window at a time, newest first. A window is one chunk of the history tables: the chunk interval (`CHUNK_INTERVAL`, 1 day by default) aligned the way TimescaleDB aligns chunks. The first window ends at the ledger just below `oldest_ingest_ledger`. The last window stops at `START_LEDGER`.
3. **Cursor.** `oldest_ingest_ledger` drops to a window's first ledger only after every row of that window is committed. Everything from the cursor to the tip is always present, so a stop at any point leaves a consistent database.

Inside a window, one fetcher reads ledgers in order, `BACKFILL_WORKERS` workers process them, and one writer flushes every `BACKFILL_FLUSH_SIZE` ledgers. Each flush writes the five history tables in parallel, one transaction per table. The lookup rows in `transaction_hashes` go in the same transaction as `transactions`.

The backfill also owns compression for the history it writes:

- It compresses the chunks of the five history tables and of `transaction_hashes` once it has finished them, on its own connections. `BACKFILL_COMPRESS_CONCURRENCY` chunks compress at once.
- It pauses every compression policy for the run and schedules them again when it exits. Live ingestion schedules them again at every start as well.
- It leaves the chunks that also hold live rows to the live policies. These are the newest window and any hole above the cursor.
- The live ingester's recent chunks therefore stay uncompressed while a backfill runs. Leave room for them on disk.

A backfill takes its own advisory lock, so two backfills for the same network cannot run at once. It does not block the live ingester.

## Sizing and tuning

The three options are in [backfill tuning](configuration.md#backfill-tuning). The run logs both estimates below when it starts.

| Resource | Estimate | Reduce it with |
| --- | --- | --- |
| Memory | `(DATASTORE_BUFFER_SIZE + BACKFILL_WORKERS + BACKFILL_FLUSH_SIZE) × 30 MB` | Lower any of the three terms |
| Disk | `(ceil(BACKFILL_COMPRESS_CONCURRENCY / 5) + 1) × 15 GB` of uncompressed windows, on top of the compressed history | Lower `BACKFILL_COMPRESS_CONCURRENCY` |
| Database connections | `5 + BACKFILL_COMPRESS_CONCURRENCY + 1` | Lower `BACKFILL_COMPRESS_CONCURRENCY` |

Both estimates are for mainnet at current density. A decoded mainnet ledger takes about 30 MB. One uncompressed day of mainnet rows takes about 15 GB with its indexes. A quieter network uses less.

- `BACKFILL_WORKERS` defaults to one per CPU. It sets how many ledgers process at once.
- `BACKFILL_FLUSH_SIZE` defaults to 100. A larger value means fewer, larger transactions and more memory.
- `BACKFILL_COMPRESS_CONCURRENCY` defaults to 5. The run lets at most `ceil(value / 5) + 1` windows sit uncompressed. When compression falls behind, the run waits before it starts the next window. A higher value compresses faster, costs more disk and needs more connections.

## What to watch

| Signal | Meaning |
| --- | --- |
| Start line | The range, the number of windows, and the memory and disk estimates |
| `window [<first>-<last>] <start>..<end>: <N> ledgers in <T> (<X> ledgers/s)` | One window committed. The cursor is at `<first>` |
| `Compressed chunk <name> in <T>` | A chunk finished compressing |
| `<window> failed (attempt <n>/3): ...` | A transient error. The window is redone |
| `wallet_ingestion_backfill_windows_remaining` | Windows left to fill |
| `wallet_ingestion_oldest_ledger` | The cursor |
| `wallet_ingestion_phase_duration_seconds{phase=...}` | `fetch`, `process_ledger`, `insert_into_db` and `compress` timings |

Read the phase timings to find the slow stage. A high `fetch` points at the ledger source. A high `process_ledger` means more `BACKFILL_WORKERS` help. A high `insert_into_db` means the database is the limit. When the run often waits for a window slot, compression is the limit. Raise `BACKFILL_COMPRESS_CONCURRENCY`, or check that the disk has room for the extra windows.

Metric names are in [observability](observability.md#ingestion).

## Failures and reruns

Any fetch, process, write or compression failure ends the run with a non-zero exit status. A window that hits a transient error is redone up to 3 attempts first. A permanent error ends the run at once. A dead datastore buffer is permanent: the window fails and so does the run.

A rerun with the same `START_LEDGER` resumes from the cursor and fills the remaining holes. Each flush commits on its own, so a failed window can leave rows behind. Before it writes a window, the run deletes that window's rows from the five history tables. Duplicate rows in `transaction_hashes` are tolerated.

| Message | Cause | Action |
| --- | --- | --- |
| `--end-ledger does not apply to backfill: ...` | `END_LEDGER` is set | Unset it |
| `no ingestion cursors yet: run live ingestion once before backfilling` | The live ingester has not written its cursors | Run live ingestion once |
| `another backfill holds the lock for this network` | A second backfill is running | Wait for it, or stop it |
| `db-max-conns is <n>, below the <m> connections a backfill with compress concurrency <k> requires` | Pool too small | Raise `DB_MAX_CONNS` or lower `BACKFILL_COMPRESS_CONCURRENCY` |
| `start ledger closed at <time>, before the retention policy's earliest kept time <time>; raise retention before backfilling that far` | Retention would drop the range | Raise `RETENTION_PERIOD` on the live ingester |

If a run is killed, the compression policies stay paused until live ingestion starts again. Restart the live ingester, or schedule the jobs again with `alter_job(<job_id>, scheduled => true)`.

## Where in the code

| Path | Role |
| --- | --- |
| `cmd/ingest.go` | `ingest` options and mode validation |
| `internal/services/ingest_backfill.go` | Run setup, window fill, retries, cursor |
| `internal/services/backfill_planner.go` | Window placement and gap windows |
| `internal/services/backfill_compressor.go` | Chunk compression and policy pause |
| `internal/ingest/ledger_backend.go` | `rpc` and `datastore` ledger sources |
