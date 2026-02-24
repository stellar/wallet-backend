---
context: The channel account's locked_tx_hash stores the inner transaction hash; ingestion must match on the inner hash not the outer fee-bump hash for unlock to work
type: insight
status: active
subsystem: signing
areas: [signing, channel-accounts, ingestion]
created: 2026-02-24
---

Channel account unlock is keyed on inner transaction hashes so fee-bumped transactions release correctly. When a channel account is locked for a transaction, `locked_tx_hash` is set to the hash of the inner (non-fee-bump) transaction. When a fee-bumped transaction lands on-chain, the ledger records a fee-bump envelope with a different outer hash. The ingestion path's `UnassignTxAndUnlockChannelAccounts` must extract and match on the inner transaction hash to correctly identify and release the channel account. Matching on the outer fee-bump hash would leave channel accounts permanently locked after fee-bump transactions.
