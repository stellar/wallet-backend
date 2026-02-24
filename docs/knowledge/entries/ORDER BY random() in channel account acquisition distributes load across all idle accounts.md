---
context: Without random(), the same account would always be selected first (lowest PK or similar) — random selection prevents hot-spots and distributes lock contention evenly
type: pattern
status: active
subsystem: signing
areas: [signing, channel-accounts, database]
created: 2026-02-24
---

`ORDER BY random()` in channel account acquisition distributes load across all idle accounts. Without randomization, the database would consistently return the same account (lowest primary key or similar ordering) on every acquisition attempt. Over time this concentrates all usage on a small subset of accounts while others sit idle. The `ORDER BY random()` clause ensures each idle account has an equal probability of selection, distributing lock contention evenly across the entire pool and utilizing all provisioned channel accounts.

This randomization only matters because [[FOR UPDATE SKIP LOCKED on channel accounts means concurrent callers never block they skip held rows and acquire different accounts]] — with blocking `FOR UPDATE`, order would not matter since callers would queue regardless. Both clauses together make the acquisition query non-blocking and load-balanced.

Areas: [[entries/signing]]
