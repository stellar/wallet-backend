---
context: Startup doesn't block on channel account provisioning — the API is immediately available, but the service may briefly have 0 channel accounts available after a cold start
type: pattern
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

`ensureChannelAccounts` runs in a goroutine at startup so the HTTP server becomes ready before channel account provisioning completes. Rather than provisioning channel accounts synchronously before starting the HTTP listener, the serve command launches `ensureChannelAccounts` in a background goroutine. The HTTP server becomes available immediately. The implication is that on a cold start (empty `channel_accounts` table), the service may briefly return `ErrNoChannelAccountConfigured` for transaction-building requests until the background provisioning goroutine completes and the on-chain channel accounts are created and stored.
