---
description: CheckForForbiddenSigners inspects simulation results auth entries; channel account public key is the forbidden signer; SourceAccount and Address credentials both checked; FORBIDDEN_SIGNER error code returned
type: insight
status: current
confidence: proven
subsystem: auth
areas: [auth, soroban, signing, channel-accounts, security]
---

# Soroban auth check prevents channel accounts from being used as contract signers

## Context

Soroban contract invocations include authorization entries specifying which accounts must sign for the contract call to succeed. A channel account appearing as a required signer would expose the wallet-backend's internal infrastructure as a contract auth signer — a security violation.

## Detail

`adjustParamsForSoroban()` in `TransactionService` calls `sorobanauth.CheckForForbiddenSigners(simulationResults, opSourceAccount, channelAccountPublicKey)`.

`CheckForForbiddenSigners` iterates all auth entries in the simulation response and checks each against the forbidden signers list:

- **`SorobanCredentialsSourceAccount`**: the source account of the operation is the signer. Checks `opSourceAccount` against `forbiddenSigners ∪ {""}` (the empty string catches cases where the source account is implicitly the transaction source).
- **`SorobanCredentialsAddress`**: an explicit signer address is specified. Checks that address directly against `forbiddenSigners`.

If any auth entry requires the channel account's public key as a signer, `ErrForbiddenSigner` is returned and mapped to the `FORBIDDEN_SIGNER` GraphQL error code.

`AuthorizeEntry()` exists but is not called server-side — the server returns unsigned auth entries to clients, who must sign them with their own keys.

## Implications

- Clients must not construct transactions where the channel account is a required Soroban signer — the server will reject them.
- This check prevents a class of attacks where a client could trick the server into using its channel account key to authorize arbitrary contract calls.
- The guard runs before transaction building completes — the error is returned before any DB writes.

## Source

`pkg/sorobanauth/sorobanauth.go:CheckForForbiddenSigners()`
`internal/services/transaction_service.go:adjustParamsForSoroban()`

## Related

Since [[channel accounts enable concurrent transaction submission by serving as source accounts]], the channel account pool is the reason this guard is necessary — the server reuses those keys across many requests, and any contract that could co-opt them would gain persistent signing authority.

relevant_notes:
  - "[[channel accounts enable concurrent transaction submission by serving as source accounts]] — grounds this entry: the pool design is precisely why channel accounts must be blocked as Soroban signers"
