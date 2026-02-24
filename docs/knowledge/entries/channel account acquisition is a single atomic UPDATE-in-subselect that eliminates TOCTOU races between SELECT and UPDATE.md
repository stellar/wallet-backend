---
context: The locking SQL performs UPDATE ... WHERE public_key = (SELECT ... FOR UPDATE SKIP LOCKED) — a single statement that atomically identifies and locks an account
type: pattern
status: active
subsystem: signing
areas: [signing, channel-accounts, database]
created: 2026-02-24
---

Channel account acquisition is a single atomic UPDATE-in-subselect that eliminates TOCTOU races between SELECT and UPDATE. Rather than first selecting an idle account and then updating it (which creates a time-of-check-to-time-of-use window where another worker could grab the same account), the acquisition uses `UPDATE channel_accounts SET locked_at=NOW(), locked_until=... WHERE public_key = (SELECT public_key FROM channel_accounts WHERE ... FOR UPDATE SKIP LOCKED LIMIT 1)`. The SELECT and UPDATE happen atomically within one database statement, making it impossible for two concurrent callers to acquire the same account.
