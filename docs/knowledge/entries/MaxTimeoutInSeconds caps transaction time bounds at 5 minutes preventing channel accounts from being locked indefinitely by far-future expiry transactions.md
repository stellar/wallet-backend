---
context: The 5-minute cap is enforced regardless of what the caller requests — bounds the worst-case lock window even if a client submits a tx with a far-future expiry
type: pattern
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

`MaxTimeoutInSeconds` caps transaction time bounds at 5 minutes preventing channel accounts from being locked indefinitely by far-future expiry transactions. When building a transaction, the service sets the time bounds to `[now, now + min(requestedTimeout, 300)]`. Without this cap, a caller could request a transaction with a time bound expiring hours or days in the future, causing the channel account to remain locked for that entire duration (until the time-based auto-release fires). The 5-minute cap ensures the worst-case lock window is bounded regardless of caller behavior. Connects to [[channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]].
