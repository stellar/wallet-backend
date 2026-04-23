# H005: Contract Deploy History Is Derived From Auth Trees Instead Of Executed Ledger Effects

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: High
**Impact**: integrity / persistent state corruption
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Contract deployment state changes should only be emitted for contracts that were actually deployed by the executed host function and confirmed by ledger effects. Merely mentioning a `CreateContract*` preimage in a Soroban authorization tree should not be enough to create persistent deploy history for a contract/account address.

## Mechanism

`ContractDeployProcessor` recursively walks every `invokeHostOp.Auth[*].RootInvocation` and emits a deployment state change for each `CreateContractHostFn` or `CreateContractV2HostFn` it encounters, even though those invocations come from authorization metadata rather than the executed operation body or ledger diff. If Soroban accepts auth trees that are broader than the executed invocation tree, an attacker can get the backend to record phantom contract deployments for deterministic contract IDs that were never created on-chain.

## Trigger

1. Craft a successful Soroban transaction whose executed host function does **not** deploy a contract, but whose auth entry tree contains a `CreateContractHostFn`/`CreateContractV2HostFn` branch with a chosen `ContractIdPreimage`.
2. Submit the transaction on-chain and wait for ingestion.
3. Query state changes or contract/account history for the derived contract ID.
4. Observe a create/deploy record even though no corresponding deployment ledger effect exists.

## Target Code

- `internal/indexer/processors/contract_deploy.go:31-127` — emits deploy state changes from auth-root and subinvocation traversal.
- `internal/indexer/processors/contract_operations.go:51-105` — shows the same recursive trust in auth-tree subinvocations.
- `internal/serve/graphql/resolvers/account.resolvers.go:108-172` — exposes persisted state changes for arbitrary account/contract addresses.
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-82` — allows callers to pivot directly to the chosen contract/account address.

## Evidence

The processor's `walkInvocation` function only inspects `SorobanAuthorizedInvocation` values and never consults the actual operation result meta before emitting a create state change. The main loop calls `walkInvocation(auth.RootInvocation)` for every auth entry on every successful invoke-host-function operation.

## Anti-Evidence

Soroban may require the authorization tree to match the executed invocation tree exactly, in which case extra create-contract branches would be rejected before the transaction reaches ingestion. If that invariant holds at the protocol layer, this specific corruption path closes.

---

## Review

**Verdict**: NEEDS_REFINEMENT
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Failed At**: reviewer

### What's Wrong

The hypothesis's specific trigger requires a **successful** Soroban transaction whose auth tree contains `CreateContractHostFn` branches that don't correspond to actual executed invocations. This is blocked by the Soroban protocol: Stellar Core enforces that auth entry invocation trees must match the actual execution trace. An auth entry with extra `CreateContractHostFn` subinvocations that were never executed would fail auth matching, causing the transaction to fail. For successful transactions, the auth tree and execution tree are consistent, so every `CreateContractHostFn` in the auth tree corresponds to a real deployment.

However, the **underlying code deficiency is real**: `ContractDeployProcessor.ProcessOperation` (contract_deploy.go:32-127) has no `Transaction.Successful()` guard. The indexer's `processTransaction` (indexer.go:155) processes ALL transactions including failed ones. `GetOperationsParticipants` (participants.go:125) iterates ALL operations regardless of success. For failed Soroban transactions, `GetOperationParticipants` returns early at line 182 with just the source account (non-empty), so the operation IS included in the `opsParticipants` map. `getTransactionStateChanges` (indexer.go:287) then calls `ProcessOperation` on each processor for every operation in that map — including failed-transaction operations.

### Alternative Angle

The viable attack path is through **failed transactions**, not successful ones:

1. Submit a Soroban `InvokeHostFunction` transaction with auth entries containing `CreateContractHostFn`/`CreateContractV2HostFn` invocations for chosen `ContractIdPreimage` values.
2. Ensure the transaction FAILS (e.g., invoke a nonexistent contract, cause host function abort, insufficient resources). The transaction is still included in the ledger with fees charged.
3. The `Auth` field is in the transaction envelope (not the result), so it's always present regardless of outcome.
4. The indexer processes the failed transaction: `processTransaction` → `getTransactionStateChanges` → `ContractDeployProcessor.ProcessOperation` reads `invokeHostOp.Auth` from the envelope and walks invocations.
5. Phantom contract deploy state changes (`StateChangeCategoryAccount`, `StateChangeReasonCreate`) are emitted and persisted via `buffer.PushStateChange`.
6. Clients querying state changes or account history see phantom contract deployments that never occurred on-chain.

This constitutes persistent state corruption (phantom deploy records in the indexer that don't reflect on-chain reality). Per the severity scale, "persistent state corruption from malicious ledger data" maps to **High**.

Note: The `participantsFromInvocationAndSubInvocations` function in `contract_operations.go:51-105` has the same pattern — it trusts auth tree entries without a success check — which would also produce phantom participant records for the same failed transactions.

### Additional Code Paths

- `internal/indexer/processors/contract_deploy.go:116-120` — the `for _, auth := range invokeHostOp.Auth` loop with NO success guard; confirm this processes failed-tx auth entries.
- `internal/indexer/processors/participants.go:165-190` — `GetOperationParticipants` returns early for failed Soroban txs at line 182, but the source-account-only result is non-empty, so the operation still enters the `opsParticipants` map.
- `internal/indexer/indexer.go:287-300` — `getTransactionStateChanges` iterates ALL `opsParticipants` and calls `ProcessOperation` on each processor with no success filter.
- `internal/indexer/processors/contract_operations.go:108-127` — `participantsForAuthEntries` also walks auth entries without a success check, creating phantom participant associations.
- Verify that `ingest.LedgerTransaction` includes failed Soroban txs and that their envelope `Auth` field is populated with submitter-chosen entries.
