---
description: TrustlineAssetModel and ContractModel compute UUID v5 from "CODE:ISSUER" or contract_id before insert; allows ON CONFLICT DO NOTHING without querying for existing IDs first
type: pattern
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, uuid, idempotency, timescaledb, postgresql]
---

# Deterministic UUID from content hash enables idempotent batch inserts without DB roundtrip

## Context

During streaming ingestion, the same trustline asset or contract token may be encountered multiple times. A naive approach would check if a row exists before inserting, requiring an extra DB roundtrip per item. Alternatively, a serial auto-increment PK would require inserting first and then looking up the generated ID.

## Detail

`TrustlineAssetModel` and `ContractModel` compute their primary key UUIDs before inserting:

```go
// trustline_assets: UUID v5 from "CODE:ISSUER"
var assetNamespace = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("trustline_assets"))
func DeterministicAssetID(code, issuer string) uuid.UUID {
    return uuid.NewSHA1(assetNamespace, []byte(code+":"+issuer))
}

// contract_tokens: UUID v5 from contract_id
var contractNamespace = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("contract_tokens"))
func DeterministicContractID(contractID string) uuid.UUID {
    return uuid.NewSHA1(contractNamespace, []byte(contractID))
}
```

Callers set the `ID` field via these functions before calling `BatchInsert`, which uses `ON CONFLICT (code, issuer) DO NOTHING`. This is idempotent and requires no pre-insert query.

## Implications

- Any code creating a new `TrustlineAsset` or `Contract` must call `DeterministicAssetID` / `DeterministicContractID` to populate the UUID — do not use `uuid.New()`.
- The namespace-qualified SHA-1 (UUID v5) ensures there are no collisions between different content types even if the raw strings are similar.
- This pattern works because the natural key (`code+issuer` or `contractID`) uniquely identifies the entity.
- The double-namespacing (`uuid.NameSpaceDNS ⊕ 'trustline_assets'` vs `uuid.NameSpaceDNS ⊕ 'contract_tokens'`) is a cross-entity collision prevention mechanism: two different entity types with the same raw string input will produce different UUIDs because their intermediate namespace UUIDs differ.

## Source

`internal/data/trustline_assets.go:DeterministicAssetID()`
`internal/data/contract_tokens.go:DeterministicContractID()`

## Related

The idempotent inserts produced by this pattern feed into [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] at step 1 (TrustlineAsset) and step 2 (Contract) — idempotency is what makes it safe to re-run those steps after a crash without violating FK constraints.

The UUID computation happens before [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] is called — `BatchInsert` with `ON CONFLICT DO NOTHING` is the insert strategy for both, enabled by the deterministic ID.

relevant_notes:
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — enables this: deterministic UUIDs make steps 1 and 2 of PersistLedgerData idempotent, so crash recovery can safely re-run the entire transaction"
  - "[[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — grounds this: BatchInsert (ON CONFLICT DO NOTHING) is the insert strategy for metadata models; deterministic UUIDs are what make this idempotent without a pre-insert SELECT"
