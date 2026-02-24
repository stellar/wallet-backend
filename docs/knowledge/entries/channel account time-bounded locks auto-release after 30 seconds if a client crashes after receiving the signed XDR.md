---
context: lockedUntil = now + timeoutInSecs (default 30s from BuildAndSignTransaction, or 60s when called without opts); ingestion releases on-chain confirmation; crash recovery relies on the timeout expiry
type: insight
created: 2026-02-24
---

When a channel account is locked for a `BuildAndSignTransaction` mutation, `lockedUntil` is set to `now + 30 seconds`. If the client receives the signed XDR but crashes before submitting the transaction, the channel account remains locked until `lockedUntil` elapses, after which it becomes available again. The normal unlock path is on-chain confirmation via `UnassignTxAndUnlockChannelAccounts` inside `PersistLedgerData` (see [[ingestion-based channel account release ties unlock to on-chain confirmation by running inside the same DB transaction as ledger commit]]). The 30-second timeout is a safety valve for the crash case, not the normal release path. The lock duration is controlled by `opts[0]` in `GetAccountPublicKey` — `BuildAndSignTransactionWithChannelAccount` passes `timeoutInSecs` (capped at `MaxTimeoutInSeconds = 300`) as the lock duration; direct calls without opts default to 60 seconds. The acquisition also retries up to `DefaultRetryCount = 6` times with `DefaultRetryInterval = 1s` sleep between attempts before returning `ErrNoIdleChannelAccountAvailable`.
