---
context: Distribution always uses ENV or KMS; channel accounts always use CHANNEL_ACCOUNT backed by DB — these roles are never swapped or cross-used
type: decision
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

The distribution account and channel accounts are two independent signing identities with different providers. The distribution account uses the `ENV` or `KMS` signing provider and represents the operator's primary Stellar account. Channel accounts use the `CHANNEL_ACCOUNT` provider backed by encrypted private keys in the database. The two clients are constructed separately and are never interchangeable — the fee-bump service uses the distribution client, and transaction building uses channel account clients.
