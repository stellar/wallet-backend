---
context: DecryptInput always includes {"pubkey": distributionAccountPublicKey} — AWS KMS rejects decryption if the context doesn't match the one used during encryption, making ciphertext account-bound
type: insight
status: active
subsystem: signing
areas: [signing, security]
created: 2026-02-24
---

KMS encryption context binds the ciphertext to a specific Stellar public key preventing cross-account decryption. When encrypting the distribution account private key, the KMS provider supplies an encryption context `{"pubkey": <distributionAccountPublicKey>}`. AWS KMS requires this exact context to be provided again during decryption, and will reject calls where the context does not match what was used at encryption time. This means a ciphertext encrypted for account A cannot be decrypted while presenting account B's context — the ciphertext is cryptographically bound to a specific account identity.
