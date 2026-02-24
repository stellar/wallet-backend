---
context: Six ordered steps in one transaction: trustline assets, contract tokens, core data, channel account unlock, token changes, cursor update — all or nothing
type: pattern
status: current
subsystem: ingestion
areas: [ingestion, data-layer]
created: 2026-02-24
---

# live ingestion persists each ledger in a single atomic database transaction

`PersistLedgerData()` wraps all database writes for a single ledger in one PostgreSQL transaction. The six steps are:

1. `TrustlineAsset.BatchInsert()` — insert trustline assets (FK prerequisite for step 5)
2. `prepareNewContractTokens()` → `Contract.BatchInsert()` — insert contract tokens
3. `insertIntoDB()` — insert transactions, operations, and state_changes
4. `UnassignTxAndUnlockChannelAccounts()` — unlock channel accounts used by this ledger's transactions
5. `ProcessTokenChanges()` — process trustline, contract, account, and SAC balance changes
6. `IngestStore.Update()` — advance the cursor to this ledger sequence

If any step fails, the entire transaction rolls back and nothing is committed. The same ledger can then be safely re-processed.

The ordering matters for correctness: step 1 must precede step 5 because trustline balances have a foreign key to trustline assets. Step 4 (channel account unlock) being inside the same transaction as step 3 (transaction data) is a key safety property — the unlock only happens if the transaction data commits successfully.

Since [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]], this atomicity is what makes re-processing safe: there is no possible state where a ledger is half-committed.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] — why the steps are in this specific order
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — crash recovery depends on this atomicity guarantee

Areas:
- [[entries/ingestion]]
