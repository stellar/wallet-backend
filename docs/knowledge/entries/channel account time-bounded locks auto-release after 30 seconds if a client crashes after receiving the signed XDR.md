---
context: lockedUntil = now + 30s is set at lock time; the ingestion path releases it on-chain confirmation; crash recovery relies on the timeout expiry
type: insight
created: 2026-02-24
---

When a channel account is locked for a `BuildAndSignTransaction` mutation, `lockedUntil` is set to `now + 30 seconds`. If the client receives the signed XDR but crashes before submitting the transaction, the channel account remains locked until `lockedUntil` elapses, after which it becomes available again. The normal unlock path is on-chain confirmation via `UnassignTxAndUnlockChannelAccounts` inside `PersistLedgerData` (see [[ingestion-based channel account release ties unlock to on-chain confirmation by running inside the same DB transaction as ledger commit]]). The 30-second timeout is a safety valve for the crash case, not the normal release path.
