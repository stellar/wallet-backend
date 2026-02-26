---
description: One channel account per in-flight transaction; distribution account sponsors their creation; concurrency ceiling equals pool size (default 15); exhaustion returns ErrUnavailableChannelAccounts
type: decision
status: current
confidence: proven
subsystem: signing
areas: [signing, channel-accounts, concurrency, stellar, transaction-building]
---

# Channel accounts enable concurrent transaction submission by serving as source accounts

## Context

Stellar transactions require a source account with an incrementing sequence number. Using a single distribution account as source limits submission to one transaction at a time (sequence numbers must be strictly sequential). To support concurrent clients, a pool of independent source accounts is needed.

## Detail

Each "channel account" is a full Stellar account whose keypair is stored encrypted in the `channel_accounts` table. When `buildTransaction` is called, the service acquires one idle channel account (via `FOR UPDATE SKIP LOCKED`), uses it as the transaction source account, and returns the signed XDR.

The concurrency ceiling equals the channel account pool size (`NUM_CHANNEL_ACCOUNTS`, default 15). When all accounts are locked:
1. `GetAccountPublicKey` retries `DefaultRetryCount = 6` times with a 1-second interval.
2. After 6 retries, returns `ErrUnavailableChannelAccounts`.

Channel accounts are sponsored by the distribution account (`BeginSponsoringFutureReserves` / `CreateAccount` / `EndSponsoringFutureReserves`), so they require no XLM reserves from the operator beyond what the distribution account holds.

## Implications

- Pool size determines maximum concurrent transactions. Operators must tune `NUM_CHANNEL_ACCOUNTS` to expected peak concurrency.
- Running out of channel accounts causes client-visible errors (after ~6 seconds of retries), not silent drops — callers should handle `CHANNEL_ACCOUNT_UNAVAILABLE` errors.
- Channel accounts are released when the confirming ledger is ingested, not when the client submits.

## Source

`internal/signing/channel_account_db_signature_client.go` — acquisition and retry logic
`internal/services/channel_account_service.go:EnsureChannelAccounts()` — provisioning
`internal/signing/store/channel_accounts_model.go:GetAndLockIdleChannelAccount()` — locking SQL

## Related

The pool is acquired via [[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] — this entry defines the concurrency design; that entry implements it at the SQL level.

Pool capacity is restored by [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — released on-chain confirmation rather than submission, because the account remains encumbered until the sequence number is definitively consumed.

relevant_notes:
  - "[[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] — extends this: SKIP LOCKED is the implementation of the concurrent acquisition this entry describes"
  - "[[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — extends this: on-chain-confirmation release is the complement of the pool design; capacity is only restored when the transaction is definitively committed"
