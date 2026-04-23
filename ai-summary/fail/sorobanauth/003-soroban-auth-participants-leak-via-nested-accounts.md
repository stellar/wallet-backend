# H003: Soroban Auth Participants Leak Through Nested Transaction And Operation Accounts

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: High
**Impact**: confidentiality / cross-tenant Soroban auth metadata disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Nested GraphQL `accounts` fields should only expose true ledger participants that the caller is authorized to see. Soroban authorization metadata such as signer addresses and nested contract/auth-tree addresses should not be turned into globally readable account lists for arbitrary callers without an explicit authorization decision.

## Mechanism

For successful Soroban operations, `participantsForAuthEntries` unions the auth signer address with every contract/account address found in `RootInvocation` and all `SubInvocations`, and `Indexer.processTransaction` persists those derived addresses into `operations_accounts`. The GraphQL `Operation.accounts` and `Transaction.accounts` resolvers then batch-load and return those rows with no caller check, so any valid JWT can inspect Soroban auth participant graphs for operations it does not own.

## Trigger

1. Identify or submit a successful Soroban transaction that uses address-based auth and/or nested subinvocations.
2. Authenticate with any valid JWT.
3. Query either:
   - `transactionByHash(hash: "...") { accounts { address } operations(first: 10) { edges { node { id accounts { address } } } } }`
   - `operationById(id: ...) { accounts { address } }`
4. Observe addresses/contracts that come from Soroban auth entries and subinvocations rather than caller-owned scope checks.

## Target Code

- `internal/indexer/processors/contract_operations.go:51-127` — recursively extracts signer and subinvocation addresses from auth entries.
- `internal/indexer/processors/contract_operations.go:332-356` — `InvokeContractOpProcessor` unions auth participants into the operation participant set.
- `internal/indexer/indexer.go:198-208` — persists every derived participant into `operations_accounts`.
- `internal/serve/graphql/dataloaders/account_loaders.go:43-66` — batch-loads `operations_accounts` rows with no authz filter.
- `internal/serve/graphql/resolvers/operation.resolvers.go:48-67` — returns `Operation.accounts` directly.
- `internal/serve/graphql/resolvers/transaction.resolvers.go:66-84` — returns `Transaction.accounts` directly from participant tables.

## Evidence

The participant extractor does not distinguish "address appeared in Soroban auth metadata" from "address is authorized to view this operation"; it simply unions every discovered address into the participant set. The nested GraphQL account resolvers perform a straight dataloader lookup on the persisted mapping and never compare the result to the authenticated request subject.

## Anti-Evidence

Only successful Soroban transactions reach this path, so malformed or failed transactions do not create the leak. Some leaked addresses are contract IDs or already-on-ledger public accounts, which reduces secrecy for those specific values, but the cross-operation authorization graph is still disclosed server-side.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of H001 (001-accountbyaddress-cross-account-read.md)
**Failed At**: reviewer

### Trace Summary

Traced all six target code paths. Confirmed that `participantsForAuthEntries` extracts signer and contract addresses from auth entries (lines 108-127), `InvokeContractOpProcessor.Participants()` unions these into the operation participant set (lines 332-356), and the indexer persists them into `operations_accounts` (lines 198-208). The GraphQL resolvers load and return these participants without per-caller filtering. However, this is identical to the working-as-designed behavior already investigated and rejected in H001.

### Code Paths Examined

- `internal/indexer/processors/contract_operations.go:108-127` — `participantsForAuthEntries` extracts signer addresses and recursively collects subinvocation participants; confirmed behavior matches hypothesis
- `internal/indexer/processors/contract_operations.go:332-356` — `InvokeContractOpProcessor.Participants()` unions source account, contract ID, and auth participants into a single set; confirmed
- `internal/indexer/indexer.go:198-208` — persists all participants into `operations_accounts` via `buffer.PushOperation`; confirmed
- `internal/serve/graphql/dataloaders/account_loaders.go:43-66` — batch-loads accounts by operation ID with no authz filter; confirmed
- `internal/serve/graphql/resolvers/operation.resolvers.go:48-67` — returns accounts directly from dataloader; no authz check; confirmed
- `internal/serve/graphql/resolvers/transaction.resolvers.go:66-84` — returns accounts by ToID from dataloader; no authz check; confirmed

### Why It Failed

This hypothesis is a duplicate of H001 (001-accountbyaddress-cross-account-read.md), which was already investigated and rejected as **working-as-designed behavior**. The H001 review established conclusively that:

1. **The JWT Subject identifies a client application, not an end user.** There is no per-user identity in the system to scope data against.
2. **The system has no concept of tenant scoping.** Root queries (`Transactions`, `Operations`, `StateChanges`) return ALL data across all accounts without any address binding.
3. **The GraphQL schema has zero authorization directives** — no `@auth`, `@hasRole`, or `@owner` patterns exist anywhere.
4. **Stellar blockchain data is inherently public.** Soroban authorization entries are part of on-chain transaction metadata visible to anyone querying the Stellar network directly.

H003 describes the same architectural property (no per-caller data scoping in GraphQL) applied to a specific data subset (Soroban auth participant addresses). The "leak" it describes is the system functioning exactly as designed — serving indexed public blockchain data to authenticated client applications.

Additionally, even if evaluated independently, the impact cannot reach Medium severity: the data is already public on the Stellar blockchain, and the "cross-tenant" framing presupposes a tenant model that does not exist in the system's design.

### Lesson Learned

Any hypothesis claiming data disclosure through GraphQL resolvers must first verify that per-user data scoping exists in the system. The wallet-backend is a client-app-authenticated blockchain data API that intentionally serves all indexed ledger data (including Soroban auth metadata) to any authenticated client. The absence of per-resolver authz checks is a design choice, not a vulnerability. H001's lesson applies broadly to all data fields in the GraphQL schema.
