---
context: Stellar signer changes (add/remove/weight) modify account but not balance; AccountsProcessor uses AccountChangedExceptSigners filter to skip them; no account_balance row is written
type: gotcha
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# AccountChangedExceptSigners skip in AccountsProcessor means signer-only operations produce no account balance entry

`AccountsProcessor` uses the `AccountChangedExceptSigners()` predicate from the Stellar SDK's change helper to filter ledger changes before processing. This predicate returns `true` only when the account entry changed in ways other than signer list modifications.

As a result: operations that exclusively modify an account's signers (`SetOptions` with only signer add/remove/weight changes) will not generate an `AccountChange` record. No row is written to the `account_balances` hypertable for these operations.

This is intentional — `AccountsProcessor` tracks account balance state (XLM balance, subentry counts, reserve requirements), not signer configuration. Signer changes don't affect any of the tracked fields and writing an `AccountChange` record with identical values to the previous record would be wasteful.

However, this creates a coverage gap if code attempts to use `account_balances` history to reconstruct the full timeline of account modifications. Signer-only operations leave no trace in `account_balances`. A separate signer history table would be required to fill this gap — one does not currently exist.

---

Relevant Notes:
- [[AccountsProcessor calculates minimum balance from numSubEntries numSponsoring and numSponsored to track reserve requirements]] — what AccountsProcessor does track
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — how AccountsProcessor reads changes it does process

Areas:
- [[entries/ingestion]]
