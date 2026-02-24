---
context: BeginSponsoringFutureReserves + CreateAccount + EndSponsoringFutureReserves = 3 ops per account; 19 × 3 = 57 ops, well within Stellar's cap while also accounting for signature accumulation limits
type: fact
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

`MaximumCreateAccountOperationsPerStellarTx = 19` because sponsored account creation requires 3 operations and Stellar imposes a 100-operation payload cap. Each channel account creation via the sponsorship model requires three operations: `BeginSponsoringFutureReserves`, `CreateAccount`, and `EndSponsoringFutureReserves`. Batching 19 accounts yields 57 operations, leaving comfortable headroom below Stellar's 100-operation limit while also accounting for the fact that each additional signer in the transaction increases signature count. This constant controls the batch size in `CreateChannelAccountsOnChain`.
