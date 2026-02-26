---
description: Knowledge map for the signing subsystem — distribution account providers (ENV/KMS), channel account lifecycle, key encryption, and transaction signing flows
type: reference
status: current
subsystem: signing
areas: [navigation, signing]
---

# Signing & Channel Accounts — Knowledge Map

Two independent signing identities: the **distribution account** (sponsoring reserves, fee-bump payer) and **channel accounts** (source accounts for concurrent transaction building). They use different providers and different storage strategies.

## Provider Selection

- [[env signature client is appropriate for development kms for production distribution account]] — ENV is in-memory and simple; KMS is zero-persistence and production-safe; channel accounts always use DB
- [[kms provider decrypts private key on every signing call with no caching by design]] — Intentional security design: no in-memory key material; DB read + AWS KMS call per signature

## Channel Account Concurrency

- [[channel accounts enable concurrent transaction submission by serving as source accounts]] — How channel accounts multiply transaction throughput; pool size = concurrency ceiling
- [[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] — The atomic UPDATE pattern with SKIP LOCKED and ORDER BY random()
- [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — Why release is tied to on-chain confirmation, not submission
- [[buildTransaction mutation returns signed xdr to the client rather than submitting it]] — The server never submits; clients control submission timing

## Key Security

- [[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] — AES-128-GCM with SHA-256 passphrase derivation; nonce prepended to ciphertext; per-sign decryption

## Tensions

- Channel account pool size: too few causes ErrUnavailableChannelAccounts after 6s; too many wastes Stellar reserves. Default 15 may need tuning for high-concurrency deployments.
- KMS adds latency to every fee-bump signature. High fee-bump workloads should account for the DB read + KMS API call cost.
- Per-call decrypt (both KMS and AES-GCM) prevents key caching attacks but adds DB read overhead to every signing operation. At high throughput, this accumulates.

---

## Synthesis Opportunities

- [[channel accounts enable concurrent transaction submission by serving as source accounts]], [[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]], and [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] together imply: "the channel account pool uses PostgreSQL as its concurrency primitive — both acquisition and release are DB transactions, meaning pool liveness is tied to DB health rather than in-process coordination." Not yet captured as a standalone entry.

Agent Notes:
- 2026-02-26: the channel account lifecycle has exactly three entries — [[channel accounts enable concurrent transaction submission by serving as source accounts]] (design), [[channel account acquisition uses for update skip locked to prevent concurrent callers from blocking]] (acquire), [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] (release). Follow in that order for the complete picture.
- 2026-02-26: KMS and AES-GCM (channel account) providers share the per-call decrypt philosophy despite different mechanisms — [[kms provider decrypts private key on every signing call with no caching by design]] and [[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] are worth reading together.
