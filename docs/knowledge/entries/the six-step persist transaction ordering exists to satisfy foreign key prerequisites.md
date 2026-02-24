---
context: Trustline assets before balances (FK); channel account unlock inside same tx for safety; cursor update last to mark ledger complete only after all data commits
type: insight
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# the six-step persist transaction ordering exists to satisfy foreign key prerequisites

The specific ordering of steps in `PersistLedgerData()` is not arbitrary — it reflects dependency relationships between the data being written:

**Step 1 (trustline assets) before Step 5 (token changes):** Trustline balance records have a foreign key to trustline asset records. If step 5 ran before step 1, the balance insert would fail with a FK violation for any new trustline asset introduced in this ledger.

**Step 4 (channel account unlock) inside the transaction:** Unlocking channel accounts happens inside the same atomic transaction as the transaction data insert (step 3). This means the unlock is only committed if the data insert also commits. If data insert fails, channel accounts remain locked (correct behavior — the transaction that used them did not successfully land in the DB).

**Step 6 (cursor update) last:** The cursor advance is the last operation. It serves as a commit marker — if anything before it fails, the cursor does not advance and the same ledger is re-processed on next run. Only after all data for a ledger is successfully written does the cursor acknowledge that ledger as complete.

The constraint ordering essentially forms a dependency graph: assets → balances, data → unlock, all data → cursor. This graph must be respected to maintain referential integrity and safe restart semantics.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[live ingestion persists each ledger in a single atomic database transaction]] — the parent pattern describing the full six-step transaction
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — cursor-last ordering is what enables safe re-processing
- [[entries/signing]] — step 4 (channel account unlock) means ingestion directly invokes signing subsystem state transitions; channel accounts remain locked if the data insert fails, preserving signing correctness
- [[ingestion-based channel account release ties unlock to on-chain confirmation by running inside the same DB transaction as ledger commit]] — the full GraphQL mutation lifecycle perspective: BuildAndSignTransaction locks the channel account; PersistLedgerData releases it atomically with the ledger commit
- [[channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]] — the crash-safety fallback when on-chain confirmation never arrives

Areas:
- [[entries/ingestion]] | [[entries/signing]]
