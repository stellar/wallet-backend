---
context: synchronous_commit=off means WAL flushes are async; a crash within the wal_writer_delay window (~200ms) could lose committed transactions without the crash recovery mechanism detecting the gap
type: question
created: 2026-02-24
---

# whether synchronous_commit=off on the backfill pool creates a risk window where a crash leaves partially-committed ledger data

The backfill pool sets `synchronous_commit = off`, which allows PostgreSQL to acknowledge transaction commits before WAL records are durably written to disk. If the server crashes within the `wal_writer_delay` window (typically ~200ms), recently committed transactions may be lost — they existed in memory, were acknowledged to the client, but were not yet flushed to disk.

The crash recovery mechanism for backfill relies on gap detection: on restart, `GetLedgerGaps()` scans the `transactions` table for missing ledger sequences and re-queues incomplete ranges. This mechanism assumes that a committed ledger batch is either fully present or fully absent in the database.

The risk: with `synchronous_commit = off`, a partial ledger batch that was acknowledged by PostgreSQL could be partially flushed to disk before a crash. Some ledgers in the batch survive, others do not. If the committed-but-not-flushed ledgers happen to be at ledger boundaries that gap detection uses as anchors, the gap detection might incorrectly conclude that a range is complete when it is not.

**This is an open question** — the reference documentation does not analyze this scenario. The gap detection implementation uses a window function on the `transactions` table to find missing sequences; whether this correctly detects mid-batch partial failures under `synchronous_commit = off` is unclear. The answer depends on the exact batch boundaries and the PostgreSQL WAL flushing behavior at crash time.

---

Relevant Notes:
- [[backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints]] — the setting that creates this risk
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — the crash recovery mechanism whose correctness is in question

Areas:
- [[entries/data-layer]]
