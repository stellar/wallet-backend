---
context: Passing a different account to SignStellarTransaction returns an error — even the simplest provider guards against accidentally signing for arbitrary accounts
type: pattern
status: active
subsystem: signing
areas: [signing, security]
created: 2026-02-24
---

The ENV provider validates that `stellarAccounts` matches the distribution account on every sign call to prevent misuse. Before signing, `SignStellarTransaction` checks that the requested account matches the configured distribution account public key. This guard exists even in the simplest provider to ensure callers cannot accidentally (or intentionally) route signing requests for non-distribution accounts through this client. Passing a different account returns an error immediately without any signing attempt.
