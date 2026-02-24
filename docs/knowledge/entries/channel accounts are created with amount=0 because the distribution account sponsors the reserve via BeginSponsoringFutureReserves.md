---
context: Creating a Stellar account normally requires funding it with the minimum XLM reserve; the sponsorship mechanism allows amount=0 because the distribution account sponsors the reserve
type: insight
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

Channel accounts are created with `amount=0` because the distribution account sponsors the reserve via `BeginSponsoringFutureReserves`. Normally, a `CreateAccount` operation must fund the new account with at least the base reserve (currently 1 XLM). The channel account provisioning flow wraps `CreateAccount` between `BeginSponsoringFutureReserves` and `EndSponsoringFutureReserves` operations from the distribution account, which causes the distribution account to sponsor the base reserve. This eliminates the need to fund each channel account individually, reducing provisioning cost to just transaction fees.

Since each sponsored account creation requires 3 operations, [[MaximumCreateAccountOperationsPerStellarTx equals 19 because sponsored account creation requires 3 operations and Stellar imposes a 100-operation payload cap]] — the batch size is directly derived from this 3-operation structure.

Areas: [[entries/signing]]
