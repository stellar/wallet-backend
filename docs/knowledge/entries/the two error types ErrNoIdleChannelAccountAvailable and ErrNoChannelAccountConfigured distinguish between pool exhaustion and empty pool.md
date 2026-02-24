---
context: ErrNoIdleChannelAccounts means all existing accounts are locked (add more accounts or reduce concurrency); ErrNoChannelAccountConfigured means the table is empty (run ensure command)
type: insight
status: active
subsystem: signing
areas: [signing, channel-accounts]
created: 2026-02-24
---

The two error types `ErrNoIdleChannelAccountAvailable` and `ErrNoChannelAccountConfigured` distinguish between pool exhaustion and an empty pool. When the acquisition query finds no idle accounts because all are currently locked by active transactions, it returns `ErrNoIdleChannelAccountAvailable` — the operator response is to increase `NUM_CHANNEL_ACCOUNTS` or reduce concurrency. When the `channel_accounts` table is completely empty (never provisioned or deleted), the service returns `ErrNoChannelAccountConfigured` — the operator response is to run the `ensure` command. Conflating these two conditions would obscure the root cause and suggest the wrong remediation.
