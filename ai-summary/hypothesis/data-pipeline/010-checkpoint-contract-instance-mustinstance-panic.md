# H010: Checkpoint Cold Start Can Panic on Contract-Instance Union Mismatch

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: Availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Checkpoint population should defensively reject or skip semantically inconsistent contract-data entries instead of crashing the process. A malformed or unusual contract-data row should surface as a handled error, leaving the node able to continue startup or fail gracefully.

## Mechanism

`processContractInstanceChange` only checks that the key union says `ScvLedgerKeyContractInstance` before dereferencing `contractDataEntry.Val.MustInstance()`. If a checkpoint contains a contract-data entry whose key is `ContractInstance` but whose value union is not an instance, `MustInstance()` will panic and take down cold-start ingestion on a DB-empty node.

## Trigger

Start an ingest node against an empty database so it executes `PopulateFromCheckpoint`, and feed it a checkpoint containing a `LedgerEntryTypeContractData` row with `Key.Type == ScvLedgerKeyContractInstance` but `Val.Type != ScvInstance`. The service should panic during checkpoint processing instead of skipping the bad row.

## Target Code

- `internal/services/checkpoint.go:325-341` — routes contract-instance keys into `processContractInstanceChange`
- `internal/services/checkpoint.go:503-548` — calls `contractDataEntry.Val.MustInstance()` without an `ok` check
- `internal/indexer/processors/protocol_contracts.go:66-73` — same semantic boundary handled safely in the live indexer with `GetInstance()`

## Evidence

The live indexer already documents that `Key.Type` and `Val.Type` are independent XDR unions and intentionally uses `GetInstance()` to avoid panicking on a key/value mismatch. The checkpoint path does not apply the same guard, even though it parses the same contract-instance concept from raw ledger data.

## Anti-Evidence

Stellar Core may never emit this inconsistency in normal practice, so the trigger may require a rare-but-valid ledger shape or an edge-case archive artifact rather than a common contract. The bug is therefore strongest as a hardening hypothesis until a concrete checkpoint sample proves reachability.
