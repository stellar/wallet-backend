---
description: wallet-backend never submits on behalf of clients; never holds user private keys; clients receive signed XDR and submit to Stellar RPC directly; enables client-controlled submission timing
type: decision
status: current
confidence: proven
subsystem: services
areas: [graphql, services, transaction-building, security, stellar]
---

# buildTransaction mutation returns signed XDR to the client rather than submitting it

## Context

The wallet-backend manages channel account keypairs but never has access to user private keys. A design decision was needed for who controls transaction submission to the Stellar network.

## Detail

The `buildTransaction` GraphQL mutation calls `TransactionService.BuildAndSignTransactionWithChannelAccount()`, which:
1. Assigns a channel account as source account.
2. Fetches the channel account's sequence number from Stellar RPC.
3. Builds and signs the transaction with the channel account keypair.
4. Returns the signed `*txnbuild.Transaction` to the resolver.

The resolver calls `tx.Base64()` and returns `BuildTransactionPayload{success: true, transactionXdr: xdrString}` to the client. **No submission to the network occurs.** The client is responsible for submitting the XDR to Stellar RPC directly.

This is intentional: the server never holds user private keys, so it cannot sign on behalf of users anyway. Returning the XDR also allows clients to inspect, multi-sign, or time the submission as needed.

## Implications

- Channel accounts remain locked until the transaction is submitted to the network AND the ledger is ingested. If a client receives XDR but never submits, the lock expires after `DefaultTimeoutInSeconds = 30`.
- Clients must handle the full submission flow: catch RPC errors, retry on network issues, wait for confirmation.
- The `createFeeBumpTransaction` mutation also returns XDR — same pattern applies.

## Source

`internal/serve/graphql/resolvers/mutations.resolvers.go:buildTransaction()`
`internal/services/transaction_service.go:BuildAndSignTransactionWithChannelAccount()`

## Related

The mutation acquires a channel account via [[channel accounts enable concurrent transaction submission by serving as source accounts]] — one channel account is consumed per buildTransaction call; the pool size is the concurrency ceiling for simultaneous builds.

The acquired channel account remains locked until [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — because the server never submits, the lock persists until a client submits AND the confirming ledger is ingested, or the 30-second timeout expires.

relevant_notes:
  - "[[channel accounts enable concurrent transaction submission by serving as source accounts]] — grounds this: the mutation's concurrency is bounded by the channel account pool; every buildTransaction call acquires one account from the pool"
  - "[[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — extends this: the XDR-only return model means lock duration is determined by client submission timing plus ingestion lag, not by server control"
