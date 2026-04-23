# H002: Checkpoint bootstrap aborts if a balance-only fake SAC contract returns an invalid `name()`

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: availability / bootstrap halt
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Checkpoint bootstrap should not let one attacker-controlled contract block initialization of a fresh node. If a contract was only heuristically identified from balance storage, metadata lookup failures should be isolated to that contract instead of aborting the entire checkpoint transaction.

## Mechanism

During checkpoint processing, balance-shaped contract-data entries create placeholder `contract_tokens` rows of type `SAC` even when no validated SAC instance metadata was seen yet. `finalize()` then calls `FetchSACMetadata()` for every such placeholder, and `FetchSACMetadata()` is explicitly fail-fast: if one contract traps, returns a non-string, or returns a malformed `name()` value, the entire checkpoint transaction aborts, preventing fresh ingest startup or rebuild from completing.

## Trigger

1. Deploy a custom contract that writes SAC-looking balance entries so checkpoint adds it to `uniqueContractTokens` with `Type: SAC` but no code/issuer.
2. Implement `name()` to return a non-string, a malformed string without `code:issuer`, or a value that causes simulation to fail.
3. Start a fresh ingest node so `PopulateFromCheckpoint()` runs and observe checkpoint finalization abort when metadata fetch reaches that contract.

## Target Code

- `internal/services/checkpoint.go:349-358` — balance-only contract-data path creates placeholder `SAC` contracts with nil code/issuer
- `internal/services/checkpoint.go:408-419` — `finalize()` fetches metadata for every placeholder SAC contract and fails the transaction on error
- `internal/services/contract_metadata.go:83-129` — `FetchSACMetadata()` joins any per-contract error into a batch-fatal failure
- `internal/services/contract_metadata.go:137-159` — malformed or non-`code:issuer` `name()` values are rejected

## Evidence

The checkpoint path treats balance-derived SAC detection as sufficient to enqueue RPC metadata lookup later, even before a validated instance row is seen. The metadata service does not downgrade or quarantine bad contracts: any single `fetchSACMetadataForContract()` failure bubbles out through `FetchSACMetadata()` and then through `checkpointProcessor.finalize()`.

## Anti-Evidence

If the balance-only placeholder path is unreachable for mainnet-valid ledger data, this becomes moot. If it is reachable, the remaining uncertainty is operational frequency: the bug primarily affects empty-DB bootstrap and rebuild flows, not already-synced steady-state nodes.
