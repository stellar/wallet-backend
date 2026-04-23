# H004: Muxed Soroban Operation Sources Poison operations_accounts Rows

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: Medium
**Impact**: availability / persistent participant-table corruption
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

When a Soroban operation uses a muxed source account, participant persistence should canonicalize that source to the underlying `G...` address (or reject it consistently) before writing to `operations_accounts`. Stored account IDs must remain decodable so later GraphQL queries over `Operation.accounts` continue to work.

## Mechanism

`participantsForSorobanOp` seeds the participant set with `op.SourceAccount().Address()`, which preserves an `M...` source account verbatim. `AddressBytea.Value` then calls `strkey.DecodeAny`, allocates a fixed 33-byte buffer, and copies only the first 32 bytes of the decoded payload before `BatchCopy` writes the row; later reads scan the malformed 33-byte value back through `AddressBytea.Scan`, which expects a valid 33-byte `G.../C...` encoding, so queries that load `operations_accounts` for the poisoned operation can fail.

## Trigger

1. Submit a successful Soroban `InvokeHostFunction` operation with an operation-level muxed source account (`M...`) and a regular transaction source.
2. Let ingestion persist the operation participants.
3. Query `operationById(id: ...) { accounts { address } }` or `transactionByHash(hash: ...) { operations(first: 10) { edges { node { accounts { address } } } } }`.
4. Observe loader/query failure caused by the malformed persisted account row.

## Target Code

- `internal/indexer/processors/transaction_operation_wrapper.go:55-63` — preserves the muxed operation source.
- `internal/indexer/processors/contract_operations.go:146-183` — seeds Soroban participants with `op.SourceAccount().Address()`.
- `internal/indexer/types/types.go:52-87` — `AddressBytea.Scan`/`Value` assume a fixed 33-byte storage shape.
- `internal/data/operations.go:380-420` — writes participant rows into `operations_accounts`.
- `internal/data/accounts.go:42-59` — reads `operations_accounts` rows back into `AddressBytea`.
- `internal/serve/graphql/resolvers/operation.resolvers.go:48-67` — exposes the failing load path.

## Evidence

The muxed source is never normalized through `ResolveToGAddress` on the Soroban participant path. `AddressBytea.Value` does not reject non-33-byte decoded payloads before truncating them into a 33-byte buffer, which creates an asymmetry with `AddressBytea.Scan`, where only a correctly encoded 33-byte value is accepted.

## Anti-Evidence

The damage appears limited to the participant side-table and the GraphQL fields that load from it; root `accountByAddress` rejects `M...` input, so not every query surface is affected. The finding depends on Stellar permitting muxed operation sources on the specific Soroban host-function shape used.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated (distinct from H002 muxed-forbidden-list-bypass which concerns `CheckForForbiddenSigners`, not participant persistence)
**Failed At**: reviewer

### Trace Summary

Traced the full data flow from `MuxedAccount.Address()` through `participantsForSorobanOp` → `AddressBytea.Value()` → PostgreSQL BYTEA → `AddressBytea.Scan()` → GraphQL response. The silent truncation in `Value()` is real: `DecodeAny("M...")` returns 40 raw bytes (32 Ed25519 + 8 mux ID), but `copy(result[1:], rawBytes)` into a 33-byte buffer silently drops the last 8 bytes. However, `Scan()` does NOT fail — `strkey.Encode(VersionByteMuxedAccount, 32_bytes)` succeeds, producing a truncated but structurally valid 56-character M-address. Furthermore, the correct G-address is always present in the union from the parallel `Participants()` → `ToAccountId()` path.

### Code Paths Examined

- `xdr/muxed_account.go:GetAddress()` — for `CryptoKeyTypeKeyTypeMuxedEd25519`, encodes 32+8=40 raw bytes with `VersionByteMuxedAccount`; confirmed `Address()` returns `M...` for muxed accounts
- `internal/indexer/processors/transaction_operation_wrapper.go:56-63` — `SourceAccount()` returns `*xdr.MuxedAccount`; `.Address()` preserves the M-address (confirmed)
- `internal/indexer/processors/contract_operations.go:151` — `set.NewSet(op.SourceAccount().Address())` — adds the M-address verbatim to participants (confirmed)
- `internal/indexer/processors/participants.go:167-173` — `op.Participants()` calls `SourceAccount().ToAccountId()` which resolves muxed → G-address; this G-address is added to the SAME set via union (confirmed)
- `internal/indexer/types/types.go:76-87` — `AddressBytea.Value()`: `DecodeAny` returns 40 bytes for M-address; `copy(result[1:], rawBytes)` copies only 32 bytes into the 33-byte buffer, silently dropping 8 bytes (confirmed)
- `internal/indexer/types/types.go:53-72` — `AddressBytea.Scan()`: reads 33 bytes, calls `strkey.Encode(VersionByteMuxedAccount, 32_bytes)` which SUCCEEDS (Encode does not validate payload size against version byte), producing a truncated 56-char M-address
- `strkey/main.go:143-173` — `Encode()` validates only version byte validity and max payload size (100); does NOT reject 32-byte payload for muxed version byte
- `internal/data/operations.go:390-404` — `AddressBytea(addr).Value()` is called per participant; no error for M-addresses (truncation is silent)
- `internal/data/accounts.go:42-58` — `BatchGetByOperationIDs` reads `account_id` via `AddressBytea.Scan()` which succeeds (no query failure)

### Why It Failed

**Severity below Medium.** The truncation bug is real but the impact does not meet the minimum filing threshold:

1. **No query failure**: The hypothesis claims "observe loader/query failure" but `AddressBytea.Scan()` succeeds — `strkey.Encode(VersionByteMuxedAccount, 32_bytes)` produces a valid (though wrong) StrKey. No crash, no panic, no error.

2. **Correct G-address always present**: The non-Soroban participant path (`Participants()` → `ToAccountId().Address()`) adds the canonical G-address to the same union set. The corrupted M-address is an *additional* wrong row, not a replacement.

3. **Impact is a spurious participant entry**: The only effect is that `operations_accounts` gets one extra row with a truncated M-address that doesn't correspond to any real account. GraphQL clients see one extra bogus participant address alongside the correct one.

4. **Does not match any Medium criteria**: Not a crash/panic from unauth'd input; not a Redis desync; not a CAS violation; not a JWT replay; not an OOM. The closest match is "persistent state corruption from malicious ledger data" (High), but (a) muxed sources are valid protocol data, not malicious, and (b) the corruption is limited to an extra wrong row in a side table with the correct data also present.

This is a genuine code quality bug (Low/Informational) but falls below the Medium minimum severity for this security scan.

### Lesson Learned

`AddressBytea` assumes all Stellar addresses decode to exactly 32 raw key bytes (+ 1 version byte = 33). Muxed accounts (`M...`) decode to 40 raw bytes, causing silent truncation. However, because the non-Soroban participant path (`ToAccountId()`) always adds the canonical G-address, the corruption only produces an extra wrong row rather than losing the correct participant. For a finding to reach Medium severity, the silent truncation would need to affect a data path where the correct address is NOT also stored — such as balance attribution or transaction source fields.
