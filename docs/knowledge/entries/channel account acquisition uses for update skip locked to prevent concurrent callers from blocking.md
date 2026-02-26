---
description: ORDER BY random() distributes load; SKIP LOCKED means concurrent callers skip rows held by another transaction rather than blocking; single atomic UPDATE+RETURNING statement
type: pattern
status: current
confidence: proven
subsystem: signing
areas: [signing, channel-accounts, postgresql, concurrency, select-for-update]
---

# Channel account acquisition uses FOR UPDATE SKIP LOCKED to prevent concurrent callers from blocking

## Context

Multiple GraphQL request handlers may try to acquire a channel account simultaneously. A naive `SELECT FOR UPDATE` would cause each request to queue behind the others, creating lock contention.

## Detail

`GetAndLockIdleChannelAccount` uses a single atomic `UPDATE ... WHERE public_key = (subquery FOR UPDATE SKIP LOCKED) RETURNING *`:

```sql
UPDATE channel_accounts
SET locked_at = NOW(), locked_until = NOW() + INTERVAL '<N> seconds'
WHERE public_key = (
    SELECT public_key
    FROM channel_accounts
    WHERE locked_until IS NULL OR locked_until < NOW()
    ORDER BY random()
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

`SKIP LOCKED` makes concurrent callers skip rows that are already locked by another transaction. Combined with `ORDER BY random()`, this distributes acquisition across the pool rather than always hitting the same account.

`FOR UPDATE SKIP LOCKED` in the subquery paired with `UPDATE ... WHERE public_key = (subquery)` forms an atomic select-and-lock — no separate select and update is needed.

## Implications

- `SKIP LOCKED` requires PostgreSQL 9.5+, which TimescaleDB builds on top of.
- `ORDER BY random()` adds slight overhead on large pools but prevents hot-spot contention on the first idle account. For pools ≤100 accounts, this overhead is negligible.
- If all accounts are locked, the subquery returns NULL, the UPDATE affects 0 rows, and the caller sees no row returned — indicating no idle accounts available.

## Source

`internal/signing/store/channel_accounts_model.go:GetAndLockIdleChannelAccount()`

## Related

The SKIP LOCKED pattern is the DB-level mechanism that makes [[channel accounts enable concurrent transaction submission by serving as source accounts]] work under load — without it, concurrent callers would serialize on lock waits rather than distributing across the pool.

The acquired lock is held until [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — the acquisition pattern here is the entry point; the atomic release entry describes the exit.

relevant_notes:
  - "[[channel accounts enable concurrent transaction submission by serving as source accounts]] — grounds this: this entry implements the concurrency mechanism that makes the pool design work; SKIP LOCKED is what prevents lock contention"
  - "[[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — extends this: acquisition (SKIP LOCKED) and release (atomic with ledger commit) are two halves of the channel account lifecycle"
