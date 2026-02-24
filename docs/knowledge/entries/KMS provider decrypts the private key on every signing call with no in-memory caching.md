---
context: Unlike ENV which caches the keypair at startup, KMS reads from DB and calls the AWS KMS API on every SignStellarTransaction — intentional, the key never lives in process memory longer than one call
type: decision
status: active
subsystem: signing
areas: [signing, security]
created: 2026-02-24
---

The KMS provider decrypts the private key on every signing call with no in-memory caching. Unlike the ENV provider which loads and caches the keypair at startup, the KMS provider fetches the encrypted key from the database and calls the AWS KMS Decrypt API on every invocation of `SignStellarTransaction`. This is an intentional security trade-off: the private key never persists in process memory beyond a single signing operation, at the cost of added per-call AWS KMS API latency.
