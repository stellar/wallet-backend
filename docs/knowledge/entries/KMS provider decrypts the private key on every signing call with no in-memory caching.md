---
context: Unlike ENV which caches the keypair at startup, KMS reads from DB and calls the AWS KMS API on every SignStellarTransaction — intentional, the key never lives in process memory longer than one call
type: decision
status: active
subsystem: signing
areas: [signing, security]
created: 2026-02-24
---

The KMS provider decrypts the private key on every signing call with no in-memory caching. Unlike the ENV provider which loads and caches the keypair at startup, the KMS provider fetches the encrypted key from the database and calls the AWS KMS Decrypt API on every invocation of `SignStellarTransaction`. This is an intentional security trade-off: the private key never persists in process memory beyond a single signing operation, at the cost of added per-call AWS KMS API latency.

Each decryption call includes the encryption context; since [[KMS encryption context binds the ciphertext to a specific Stellar public key preventing cross-account decryption]], the decrypt call fails if the context does not match — making the per-call KMS API round-trip both a security check and a key retrieval. This also means the KMS provider only operates on the distribution account's key; [[the distribution account and channel accounts are two independent signing identities with different providers]] — channel account keys are decrypted locally using AES-128-GCM, not via KMS.

Areas: [[entries/signing]]
