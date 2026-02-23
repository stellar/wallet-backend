---
description: Signing providers (ENV/KMS), channel account lifecycle, encryption, and transaction signing
type: reference
subsystem: signing
areas: [signing, channel-accounts, kms, encryption]
status: partial
vault: docs/knowledge
---

# Signing & Channel Accounts Architecture

## SignatureClient Interface

<!-- TODO: Read internal/signing/signature_client.go.
     Document:
     - The SignatureClient interface methods
     - The three implementations: ENV, KMS, ChannelAccount
     - How the provider is selected based on DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER config
     - Show the interface definition and key method signatures -->

## ENV Provider

<!-- TODO: Read internal/signing/env.go.
     Document how it reads the private key from environment and signs transactions. -->

## KMS Provider

<!-- TODO: Read internal/signing/kms.go.
     Document:
     - AWS KMS integration
     - How signing requests are made to KMS
     - Key ARN configuration
     - Any caching or performance considerations -->

## Channel Account Lifecycle

<!-- TODO: Read internal/signing/store/ and internal/services/channel_account_service.go.
     Create ASCII diagram:
       CLI ensure → Create Keypairs → Encrypt Private Keys → Store in DB
       Transaction → Acquire Channel → Decrypt Key → Sign → Submit → Release Channel
     Document:
     - How channel accounts are created (ensure command)
     - How they're acquired for transaction submission (locking mechanism)
     - How they're released after submission
     - Sequence number management per channel account -->

## Encryption

<!-- TODO: Read internal/signing/utils/encrypter.go.
     Document:
     - Encryption algorithm used
     - How CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE is used
     - Key derivation (if any)
     - The encrypt/decrypt flow for channel account private keys -->


---

**Topics:** [[entries/index]] | [[entries/signing]]
