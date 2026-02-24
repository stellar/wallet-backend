---
context: The fee-bump service only adds the distribution account's fee-bump signature; if the inner tx has no signatures, the service rejects it — a prerequisite GraphQL clients must satisfy
type: insight
status: active
subsystem: signing
areas: [signing, graphql]
created: 2026-02-24
---

`createFeeBumpTransaction` requires the inner transaction to already be signed before wrapping, and returns `ErrNoSignaturesProvided` otherwise. The fee-bump flow assumes the inner transaction carries at least one signature (typically from the channel account that built it). The distribution account only contributes the outer fee-bump envelope signature — it does not sign the inner payload. If a client calls `WrapTransaction` with an unsigned inner transaction, the service returns `ErrNoSignaturesProvided` immediately. GraphQL clients must ensure they return signed XDR from `BuildAndSignTransaction` before submitting to `FeeBumpTransaction`.
