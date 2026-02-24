---
context: PopulateAccountTokens snapshots current token holdings before ingestion so state changes are coherent from that point; starting from genesis is impractical
type: decision
status: current
subsystem: ingestion
areas: [ingestion, stellar-rpc]
created: 2026-02-24
---

# first-run live ingestion starts from history archive tip not genesis

On first run (when `latestLedgerCursor` is 0), live ingestion does not start from ledger 1. It calls `archive.GetLatestLedgerSequence()` to find the current tip of the history archive and starts from there.

The reason is practical: starting from genesis would require ingesting tens of millions of ledgers before the service becomes useful. The trade-off is that historical data before the start ledger is absent — backfill exists to fill that history later if needed.

The critical companion step is `PopulateAccountTokens()`, which runs immediately after getting the archive tip. This snapshots the current state of token holdings for all accounts before any new ledgers are processed. Without this snapshot, the service would be missing the accumulated balance history from all ledgers before the start point — subsequent state changes would have no baseline to apply against.

This means the initialization order matters: get archive tip → snapshot token holdings → set both cursors to startLedger → begin ingesting from startLedger + 1. Reversing the order (starting ingestion before snapshotting) would create a race condition where new events arrive before the initial state is captured.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup threshold triggers parallel backfill instead of sequential catchup after restart]] — on subsequent restarts, a different startup path applies

Areas:
- [[entries/ingestion]]
