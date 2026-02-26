---
context: No error is logged, no retry is attempted — UNKNOWN contracts are simply ignored at ingestion time; this is intentional because classification failure is irrecoverable without re-fetching the contract code
type: decision
status: active
created: 2026-02-26
---

# unknown contract type is silently skipped in ProcessTokenChanges because unclassified contracts cannot be safely stored

When the contract classification pipeline cannot identify a contract as SAC or SEP-41, it assigns `Type = UNKNOWN`. Inside `processContractTokenChanges()`, UNKNOWN contracts are silently skipped — no insert, no error, no retry:

```go
// processContractTokenChanges
for _, change := range contractChanges {
    if change.ContractType != types.SEP41 {
        continue // UNKNOWN and SAC non-SEP41 are skipped
    }
    // ... append to account_contract_tokens
}
```

## Why silent skip, not error

Classification failure at ingestion time is irrecoverable within the current ledger transaction. The contract WASM bytecode has already been read from the ledger stream — there is no way to re-classify it without fetching the contract code again from the archive. Returning an error would abort the entire per-ledger transaction, dropping all other valid changes from that ledger. Silent skip trades completeness for availability.

## Operational consequence

A non-compliant SEP-41 contract deployed on-chain will be permanently excluded from token tracking. There is no retry queue or deferred re-classification. Operators discovering missing contracts must investigate the contract's WASM spec against the 10-function SEP-41 requirement.

## Source

`internal/services/token_ingestion.go` — `processContractTokenChanges()` (approximately line 211 per reference doc)

## Related

- [[sep-41 contract classification uses the contractspecv0 wasm custom section to verify all 10 required function signatures before accepting a contract as valid]] — the classification step that produces UNKNOWN contracts
- [[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — the tool that performs classification

relevant_notes:
  - "[[sep-41 contract classification uses the contractspecv0 wasm custom section to verify all 10 required function signatures before accepting a contract as valid]] — grounds this: failing to pass the 10-function check produces UNKNOWN; this entry explains what happens to UNKNOWN contracts downstream"
  - "[[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — co-occurs: wazero classification runs before processContractTokenChanges; UNKNOWN is the output when wazero finds non-conformance"
