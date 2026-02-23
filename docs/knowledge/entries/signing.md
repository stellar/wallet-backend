---
description: Knowledge map for the signing and channel accounts subsystem
type: reference
subsystem: signing
areas: [knowledge-map, signing, channel-accounts, kms, encryption]
vault: docs/knowledge
---

# Signing & Channel Accounts Knowledge Map

The signing subsystem provides pluggable signature providers (ENV for dev, KMS for production) and manages channel accounts — pre-funded Stellar accounts used to parallelize transaction submission without sequence number conflicts.

**Key code:** `internal/signing/`, `internal/services/channel_account_service.go`

## Reference Doc

[[references/signing-and-channels]] — SignatureClient interface, ENV/KMS providers, channel account lifecycle, encryption scheme

## Decisions

<!-- - [[entries/why-channel-accounts]] -->
<!-- - [[entries/why-nacl-encryption]] -->

## Insights

<!-- - [[entries/channel-account-locking-mechanism]] -->

## Patterns

<!-- - [[entries/pluggable-signing-provider]] -->

## Gotchas

<!-- - [[entries/channel-account-sequence-number-drift]] -->

## Topics

[[entries/index]] | [[references/signing-and-channels]]
