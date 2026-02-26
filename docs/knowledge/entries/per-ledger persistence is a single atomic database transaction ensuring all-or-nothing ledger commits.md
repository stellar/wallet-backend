---
description: PersistLedgerData runs 6 steps (assets→contracts→core data→unlock channels→token changes→cursor update) in one DB transaction; partial ledger commits are impossible
type: decision
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, postgresql, atomicity, persistence, channel-accounts]
---

# Per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits

## Context

A ledger contains multiple data types (transactions, operations, state changes, token balance updates) that must be stored together. A partial commit — where some data is written but the cursor is not advanced — would leave the database in an inconsistent state.

## Detail

`PersistLedgerData(ctx, ledgerSeq, buffer, cursorName)` opens a single PostgreSQL transaction and executes these steps in order:

1. `TrustlineAsset.BatchInsert()` — FK prerequisite for balance tables
2. `prepareNewContractTokens()` → `Contract.BatchInsert()`
3. `insertIntoDB()` — transactions + operations + state_changes (hypertable writes)
4. `UnassignTxAndUnlockChannelAccounts()` — release channel accounts locked by this ledger's txs
5. `ProcessTokenChanges()` — trustline, contract, account, SAC balance updates
6. `IngestStore.Update(cursorName, ledgerSeq)` — advance cursor

All six steps commit together or not at all. If the process crashes between steps 1 and 6, PostgreSQL rolls back the transaction. On restart, the cursor still points to the previous ledger, and the same ledger is re-processed cleanly.

## Implications

- The advisory lock + atomic transaction combination makes crash recovery trivial: no manual state cleanup needed.
- Adding a new persistence step must go inside this transaction to maintain the all-or-nothing guarantee.
- The order of steps is intentional: FK prerequisites (assets) before rows that reference them (balances), channel unlock before cursor advance.

## Source

`internal/services/ingest_live.go:PersistLedgerData()`

## Related

The atomicity guarantee complements [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — the lock ensures only one writer, and the atomic transaction ensures that writer commits complete ledgers or nothing at all, making crash recovery trivial.

Step 4 of PersistLedgerData is [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — the channel unlock is embedded in this transaction precisely so release cannot happen before the cursor advances.

The cursor advance in step 6 is the synchronization primitive described in [[the database is the sole integration point between the serve and ingest processes]] — `serve` learns about a new ledger only when this transaction commits.

relevant_notes:
  - "[[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — synthesizes with this: advisory lock guarantees single-writer, atomic transaction guarantees complete-or-nothing; together they make ingestion crash-safe"
  - "[[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — grounds this: channel release is step 4 in this transaction, proving the atomicity guarantee applies to signing infrastructure too"
  - "[[the database is the sole integration point between the serve and ingest processes]] — grounds this: the cursor advance in step 6 is the event that makes ingested data visible to serve; atomicity guarantees serve never sees partial ledgers"
  - "[[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — grounds this: the upsert-based current-state design of balance tables is what makes them steps 1 and 5 in PersistLedgerData — FK prerequisite first, then in-place updates after all hypertable event writes"
