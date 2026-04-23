# H001: Balance-only fake SAC entries can halt live and catchup ingestion at the SAC-balance foreign key

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: availability / ingestion halt
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Live ingest and catchup backfill should reject non-SAC contract-data balance entries before they reach the `sac_balances` table, or they should ensure a matching `contract_tokens` row is created in the same transaction. A ledger containing attacker-controlled contract storage should not be able to make the ingestion loop roll back the ledger repeatedly and stop advancing cursors.

## Mechanism

`SACBalancesProcessor` can stage SAC balance changes from contract-data entries independently of `SACInstanceProcessor`, but `PersistLedgerData` / `processBatchChanges` only create `contract_tokens` rows from `buffer.GetSACContracts()` (instance-derived metadata). If an attacker writes balance-shaped storage under a non-SAC contract so the balance path fires without a matching instance-derived contract row, `tokenIngestionService.ProcessTokenChanges` upserts `sac_balances` against a deferred foreign key to `contract_tokens(id)`, causing the whole transaction to fail and the live/catchup loop to retry the same ledger until ingest stops.

## Trigger

1. Deploy a custom Soroban contract whose storage includes valid-looking `["Balance", <contract-address>] -> {amount, authorized, clawback}` entries but no SAC instance entry that `sac.AssetFromContractData` would accept.
2. Cause a ledger to touch that contract data so the indexer sees a SAC balance change.
3. Let live ingest or catchup process the ledger and observe `sac_balances` upserted for `DeterministicContractID(change.ContractID)` without a corresponding `contract_tokens` row, rolling back the transaction and wedging ingestion on that ledger.

## Target Code

- `internal/indexer/indexer.go:229-242` — stages SAC balance changes before SAC contract metadata and treats them as independent pipelines
- `internal/services/ingest_live.go:96-107` — inserts only `buffer.GetSACContracts()` into `contract_tokens`
- `internal/services/ingest_live.go:136-143` — applies SAC balance changes afterward inside the same transaction
- `internal/services/ingest_backfill.go:625-659` — catchup merge path repeats the same contract-then-SAC-balance ordering with only instance-derived contracts
- `internal/services/token_ingestion.go:145-167` — converts staged SAC balance changes directly into `sac_balances` upserts
- `internal/db/migrations/2026-01-16.0-sac-balances.sql:9-19` — deferred FK from `sac_balances.contract_id` to `contract_tokens(id)`

## Evidence

The live and catchup persistence paths never synthesize contract metadata from SAC balance changes; they only insert rows returned by `GetSACContracts()`, which comes from `SACInstanceProcessor`. In contrast, the checkpoint path explicitly creates placeholder SAC contracts from balance-only entries at `internal/services/checkpoint.go:349-358`, which strongly suggests the codebase already expects the balance parser to succeed even when no instance-derived contract row is present.

## Anti-Evidence

If `sac.ContractBalanceFromContractData` is impossible to satisfy without a genuine SAC instance entry being processed in the same ledger, the missing-contract FK failure would not occur. The main uncertainty is therefore not the FK behavior itself, but whether attacker-controlled balance-only contract data can reach `SACBalancesProcessor` on mainnet-valid ledgers.

---

## Review

**Verdict**: VIABLE
**Severity**: Medium
**Date**: 2026-04-23
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated

### Trace Summary

The SDK's `sac.ContractBalanceFromContractData` validates only the structural shape of contract-data entries (`["Balance", <C-address>]` key + `{amount, authorized, clawback}` value map) but does NOT verify the hosting contract is a genuine Stellar Asset Contract. In contrast, `sac.AssetFromContractData` (used by `SACInstanceProcessor`) re-derives the SAC contract ID from the asset info and compares it — rejecting non-SAC contracts. This asymmetry means a custom Soroban contract that writes balance-shaped persistent storage will pass the balance processor but produce no matching `contract_tokens` row, causing a deferred FK violation that rolls back the entire ledger transaction and halts ingestion after 5 retries.

### Code Paths Examined

- `go-stellar-sdk@v0.5.0/ingest/sac/contract_data.go:ContractBalanceFromContractData` — validates data shape only (durability, key vector `["Balance", ScAddress]`, value map `{amount:i128, authorized:bool, clawback:bool}`); does NOT derive or compare the SAC contract ID. Any non-native contract with this storage shape passes.
- `go-stellar-sdk@v0.5.0/ingest/sac/contract_data.go:AssetFromContractData` — in contrast, re-derives `asset.ContractID(passphrase)` and compares against the entry's contract ID. Only genuine SACs pass.
- `internal/indexer/processors/sac_balances.go:processSACBalanceChange:96-104` — calls `ContractBalanceFromContractData`; on `ok`, extracts `contractID` from the entry's `Contract` field and emits a `SACBalanceChange`.
- `internal/indexer/processors/sac_instances.go:ProcessOperation:57-60` — calls `AssetFromContractData`; rejects non-SAC contracts, so no `contract_tokens` row is produced.
- `internal/services/ingest_live.go:PersistLedgerData:87-143` — within one `RunInTransaction`: inserts `contract_tokens` from `buffer.GetSACContracts()` (instance-derived only, step 2), then calls `ProcessTokenChanges` which upserts `sac_balances` (step 4). The deferred FK on `sac_balances.contract_id → contract_tokens(id)` is checked at commit.
- `internal/services/ingest_live.go:ingestProcessedDataWithRetry:467-501` — retries `PersistLedgerData` up to 5 times with exponential backoff; on exhaustion returns error.
- `internal/services/ingest_live.go:ingestLiveLedgers:363-366` — on retry exhaustion, returns error to caller, which propagates to `log.Fatalf` (process exit). Restart hits the same ledger → infinite crash loop.
- `internal/services/checkpoint.go:349-358` — the checkpoint path explicitly creates placeholder `contract_tokens` rows from balance-only entries, confirming the codebase knows this scenario is possible.
- `internal/db/migrations/2026-01-16.0-sac-balances.sql:17-19` — `CONSTRAINT fk_contract_token FOREIGN KEY (contract_id) REFERENCES contract_tokens(id) DEFERRABLE INITIALLY DEFERRED`.

### Findings

1. **Root cause confirmed**: `ContractBalanceFromContractData` is a shape-only validator with no SAC-identity check. Any custom Soroban contract can write persistent data in the `["Balance", <C-addr>] → {amount, authorized, clawback}` shape, and the indexer will accept it.
2. **FK violation path confirmed**: `PersistLedgerData` inserts `contract_tokens` only from `SACInstanceProcessor` output (genuine SACs). The subsequent `sac_balances` upsert references a `DeterministicContractID` for the attacker's contract, which has no `contract_tokens` row. The deferred FK fails at commit.
3. **Retry exhaustion → crash loop confirmed**: After 5 failed attempts, the error propagates up and the process exits. On restart, it resumes from the same ledger (cursor was never advanced), creating an infinite crash loop.
4. **Checkpoint path handles this correctly**: `checkpoint.go:349-358` creates placeholder `contract_tokens` rows from balance-only entries — the live and catchup paths omit this step.
5. **Attacker capability is concrete**: Deploy a Soroban contract that writes the specific storage shape. This requires only a standard Soroban transaction — no privileged access.

### PoC Guidance

- **Test file**: `internal/services/ingest_test.go` (append new test)
- **Setup**: Use the existing test infrastructure for `PersistLedgerData`. Build a mock `IndexerBuffer` where `GetSACBalanceChanges()` returns a change with a `ContractID` that does NOT appear in `GetSACContracts()`. Set up the real migration schema (with the FK constraint) using `dbtest`.
- **Steps**:
  1. Create a `SACBalanceChange` with `Operation=SACBalanceOpAdd`, `ContractID="C_ATTACKER..."`, `AccountID="C_HOLDER..."`.
  2. Ensure `GetSACContracts()` returns an empty map (no matching instance entry).
  3. Call `PersistLedgerData` within a real DB transaction.
- **Assertion**: The transaction must fail with a foreign key violation error on `fk_contract_token`. Verify the cursor is NOT advanced (the ledger was not persisted). Then verify that calling `PersistLedgerData` again for the same ledger produces the same error (demonstrating the wedge).
