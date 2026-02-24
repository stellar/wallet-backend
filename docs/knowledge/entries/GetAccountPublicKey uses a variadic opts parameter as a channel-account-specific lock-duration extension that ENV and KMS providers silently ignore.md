---
context: The opts ...int is a leaky abstraction — it carries channel-account-specific behavior (lock duration in seconds) through a generic interface that other providers ignore without error
type: insight
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

`GetAccountPublicKey` uses a variadic `opts` parameter as a channel-account-specific lock-duration extension that ENV and KMS providers silently ignore. The `SignatureClient` interface defines `GetAccountPublicKey(ctx, stellarAccounts, opts ...int)` where `opts[0]`, when present, overrides the channel account lock duration in seconds. The ENV and KMS providers accept but ignore this parameter since they don't manage lock durations. This is a leaky abstraction — behavior that belongs only to the channel account implementation bleeds through a shared interface signature.
