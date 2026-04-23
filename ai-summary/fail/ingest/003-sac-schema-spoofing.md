# H003: SAC indexing trusts storage shape without verifying the contract ID matches the embedded asset

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: High
**Impact**: balance integrity / wrong-asset attribution
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Ingest should only classify a contract as a Stellar Asset Contract when the contract's address matches the deterministic asset-contract ID derived from the embedded classic asset. Merely matching the SAC storage schema should not be enough to create `contract_tokens` or `sac_balances` rows for a contract.

## Mechanism

Both `SACInstanceProcessor` and `SACBalancesProcessor` trust the SDK helpers as sufficient proof that a ledger entry belongs to an SAC, but neither cross-checks `contractID == asset.ContractID(networkPassphrase)`. A custom contract that mimics SAC instance/balance storage for a victim `code:issuer` pair can therefore be indexed as a real SAC and populate attacker-controlled balances under an unrelated contract address, poisoning wallet data for that fake asset contract.

## Trigger

1. Deploy a custom Soroban contract at an attacker-controlled contract ID.
2. Write contract instance and balance entries that satisfy the SAC helpers' expected schema for a victim asset, e.g. `USDC:G...`.
3. Let ingest process the ledger and observe it inserting `contract_tokens`/`sac_balances` rows for the attacker contract as if it were the real asset contract.

## Target Code

- `internal/indexer/processors/sac_instances.go:47-85` — classifies any matching instance entry as `ContractTypeSAC`
- `internal/indexer/processors/sac_balances.go:96-137` — accepts matching balance storage and persists it as SAC balance data
- `internal/services/checkpoint.go:349-360` — checkpoint path also trusts SAC-shaped balance storage
- `internal/indexer/processors/contracts/sac.go:isSACContract:292-299` — shows the stricter contract-ID equality check used elsewhere

## Evidence

The SAC ingest paths call `sac.AssetFromContractData` / `sac.ContractBalanceFromContractData` and immediately persist derived metadata, but do not derive the expected asset contract ID and compare it to the actual contract being indexed. In contrast, `SACEventsProcessor` explicitly performs that equality check before treating an event as SAC-specific.

## Anti-Evidence

The SDK helpers already reject malformed schemas, the native asset contract, and G-address holders for balance entries. The exploit therefore depends on crafting a contract whose storage looks SAC-valid enough to pass those helpers.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

The hypothesis claims neither `SACInstanceProcessor` nor `SACBalancesProcessor` cross-checks the contract ID against the deterministic asset contract ID. This is factually incorrect for the instance path. The SDK function `sac.AssetFromContractData` (go-stellar-sdk@v0.1.0, `ingest/sac/contract_data.go:170-176`) explicitly derives the expected contract ID from the embedded asset and rejects entries where `expectedID != *contractData.Contract.ContractId`. The balance path (`ContractBalanceFromContractData`) does lack this check, but balance entries carry no asset identity to derive an expected ID from, and any mislabeling is cosmetic with no corruption of real SAC data.

### Code Paths Examined

- `go-stellar-sdk@v0.1.0/ingest/sac/contract_data.go:AssetFromContractData:170-176` — **performs the exact contract ID equality check the hypothesis claims is missing**: `expectedID, err := asset.ContractID(passphrase); ... if expectedID != *(contractData.Contract.ContractId) { return false }`
- `go-stellar-sdk@v0.1.0/ingest/sac/contract_data.go:ContractBalanceFromContractData:203-275` — validates balance schema (key=["Balance", address], value={amount, authorized, clawback}) and rejects native asset, but does NOT verify contract ID against a derived asset contract ID (balance entries don't carry asset identity)
- `internal/indexer/processors/sac_instances.go:ProcessOperation:40-88` — calls `sac.AssetFromContractData` which includes the contract ID check; forged instances are rejected
- `internal/indexer/processors/sac_balances.go:processSACBalanceChange:74-138` — calls `sac.ContractBalanceFromContractData`; stores balance under the entry's own contract ID (not a victim's)
- `internal/services/checkpoint.go:316-371` — checkpoint balance path labels contract as SAC (line 356) if `ContractBalanceFromContractData` passes and no instance entry was processed first; but instance path at line 331 unconditionally overwrites, and balance path at line 352 only writes if not exists
- `internal/indexer/processors/contracts/sac.go:isSACContract:292-299` — `SACEventsProcessor` also checks contract ID; this is the check the hypothesis cites as "stricter" but the same defense exists in the SDK's `AssetFromContractData`

### Why It Failed

The hypothesis's core mechanism is disproven. `sac.AssetFromContractData` already performs the `contractID == asset.ContractID(passphrase)` check (SDK lines 170-176), making the instance classification path immune to spoofing by a custom contract. The balance path (`ContractBalanceFromContractData`) does lack a contract ID check, but: (a) balance entries inherently cannot carry asset identity to derive an expected contract ID from, (b) any mislabeling of contract type in the checkpoint path is either prevented (instance processed first) or overwritten (instance processed after), and (c) no real SAC's balance data is corrupted since the fake contract has its own distinct contract ID. The worst-case impact is a cosmetic mislabeling of an attacker's own contract as SAC type with no code/issuer/name, which is Informational severity — below the Medium filing threshold.

### Lesson Learned

Always read the actual SDK source for helper functions before claiming they lack validation. `AssetFromContractData`'s doc comment explicitly states it "will ignore forged asset info entries by deriving the Stellar Asset Contract ID from the asset info entry and comparing it to the contract ID found in the ledger entry." The hypothesis missed this because it only examined the wallet-backend callers, not the SDK implementation.
