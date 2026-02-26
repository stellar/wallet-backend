---
description: UnassignTxAndUnlockChannelAccounts runs inside PersistLedgerData DB transaction; channel release is tied to confirmed on-chain inclusion, not to submission receipt
type: decision
status: current
confidence: proven
subsystem: signing
areas: [signing, channel-accounts, ingestion, atomicity, postgresql]
---

# Channel account is released atomically with the confirming ledger commit in PersistLedgerData

## Context

A channel account must be released after its transaction is confirmed on the Stellar network. Releasing too early (on submission receipt) risks the account being reused before confirmation. Releasing only via a time-based expiry wastes capacity.

## Detail

`UnassignTxAndUnlockChannelAccounts()` is called inside `PersistLedgerData` within the same database transaction that commits the ledger data. The call sets `locked_tx_hash=NULL`, `locked_at=NULL`, `locked_until=NULL` for all channel accounts whose `locked_tx_hash` matches a transaction in the just-committed ledger.

Because this runs in the same transaction as the cursor update, the release is atomic with ledger confirmation: either the ledger is committed AND the account is released, or neither happens.

A secondary time-expiry mechanism (`locked_until < NOW()`) in `GetAndLockIdleChannelAccount` handles accounts whose transactions were never submitted — these expire after 30 seconds and are reclaimed by the next acquisition attempt.

Unlock keys on inner transaction hashes — for fee-bumped transactions, `locked_tx_hash` matches the inner transaction, not the outer fee-bump.

## Implications

- Channel account release is driven by ingestion, not submission. There will always be a delay between submission and release equal to the ledger ingestion latency.
- The time-expiry fallback ensures accounts are never permanently stuck if a client receives XDR but crashes before submitting.
- Do not call `UnassignTxAndUnlockChannelAccounts` outside `PersistLedgerData` — breaking the atomicity guarantee could release accounts before their transaction is confirmed.

## Source

`internal/services/ingest_live.go:PersistLedgerData()` — atomic commit including channel unlock
`internal/signing/store/channel_accounts_model.go:UnassignTxAndUnlockChannelAccounts()`

## Related

The release ties to [[channel accounts enable concurrent transaction submission by serving as source accounts]] — pool capacity is restored only when the network confirms, not when the client submits, because unconfirmed transactions haven't actually consumed a sequence number slot.

The release happens inside [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — step 4 of PersistLedgerData, making the unlock atomic with the cursor advance and preventing accounts from becoming available before their ledger is committed.

The acquisition pair for this release is [[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] — together these two entries define the full lifecycle of a channel account across a transaction request.

relevant_notes:
  - "[[channel accounts enable concurrent transaction submission by serving as source accounts]] — grounds this: releasing on-chain confirmation (not submission) is what makes the pool semantics correct; premature release would allow sequence number reuse"
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — grounds this: release is inside PersistLedgerData's single transaction, making it atomic with ledger commit — cannot release before cursor advance"
  - "[[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] — extends this: acquisition and release together define the channel account lifecycle; this entry is the release half"
