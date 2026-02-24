---
context: SQL uses LEAD() window function on DISTINCT ledger_number from transactions table; only finds internal gaps not leading/trailing missing ranges
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# backfill uses gap detection via window function on transactions ledger_number

Historical backfill determines what needs to be fetched by finding gaps in already-ingested data. The SQL pattern is:

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

This finds contiguous ranges of missing ledger numbers between already-ingested ledgers. The window function `LEAD()` looks at the next row's `ledger_number` — if there is a gap between the current and next ingested ledger, that gap range is returned.

An important limitation: `GetLedgerGaps()` only finds internal gaps. It cannot detect that ledgers are missing before the earliest ingested ledger or after the latest. The gap calculation logic in `calculateBackfillGaps()` handles this: it has three cases — entirely before existing data, overlapping the start of existing data (prepends the missing prefix before calling GetLedgerGaps), or entirely within existing data (internal gaps only).

For catchup mode, this query is bypassed entirely — the entire range from `latestIngestedLedger + 1` to `networkLatest` is treated as a single gap to fill.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup threshold triggers parallel backfill instead of sequential catchup after restart]] — catchup mode doesn't use gap detection
- [[UpdateMin cursor pattern ensures oldest cursor only moves backward not forward]] — gap detection and cursor management work together

Areas:
- [[entries/ingestion]]
