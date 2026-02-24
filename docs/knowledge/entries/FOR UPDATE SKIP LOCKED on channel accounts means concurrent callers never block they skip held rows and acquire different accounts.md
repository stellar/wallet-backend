---
context: SKIP LOCKED makes busy accounts invisible to other callers rather than blocking them — this is the core enabler of concurrent transaction building across the pool
type: insight
status: active
subsystem: signing
areas: [signing, channel-accounts, database]
created: 2026-02-24
---

`FOR UPDATE SKIP LOCKED` on channel accounts means concurrent callers never block — they skip held rows and acquire different accounts. The standard `FOR UPDATE` would cause a second caller trying to select an account that's already locked to wait until the first caller releases it. `SKIP LOCKED` changes the behavior: any row that is currently locked by another transaction is silently excluded from the result set. Multiple concurrent goroutines can each execute the acquisition query simultaneously, and each will find a different idle account without any blocking or serialization overhead.

SKIP LOCKED is one component of the full acquisition statement; since [[channel account acquisition is a single atomic UPDATE-in-subselect that eliminates TOCTOU races between SELECT and UPDATE]], the SKIP LOCKED clause runs inside the subselect of an atomic UPDATE, not as a standalone SELECT. [[ORDER BY random() in channel account acquisition distributes load across all idle accounts]] completes the picture by ensuring the skipped-over accounts are not systematically the same ones on every call.

Areas: [[entries/signing]]
