---
context: All 10 function signatures must match exactly — a contract missing even one will be classified UNKNOWN and permanently excluded from token tracking
type: fact
status: active
created: 2026-02-26
---

# SEP-41 contract classification uses the contractspecv0 WASM custom section to verify all 10 required function signatures before accepting a contract as valid

The `ContractValidator` checks the following 10 function signatures to classify a contract as SEP-41:

| Function | Required Inputs | Output |
|---------|----------------|--------|
| `balance` | `id: Address` | `i128` |
| `allowance` | `from: Address, spender: Address` | `i128` |
| `decimals` | _(none)_ | `u32` |
| `name` | _(none)_ | `String` |
| `symbol` | _(none)_ | `String` |
| `approve` | `from, spender: Address, amount: i128, expiration_ledger: u32` | _(void)_ |
| `transfer` | `from, to: Address, amount: i128` OR `from: Address, to_muxed: MuxedAddress, amount: i128` (CAP-67) | _(void)_ |
| `transfer_from` | `spender, from, to: Address, amount: i128` | _(void)_ |
| `burn` | `from: Address, amount: i128` | _(void)_ |
| `burn_from` | `spender, from: Address, amount: i128` | _(void)_ |

The `transfer` function is special: it accepts both the classic `Address` variant and the CAP-67 `MuxedAddress` variant. A contract implementing only one variant still passes.

## Classification gate

All 10 functions must be present with matching signatures. A contract that exports 9 of 10, or has a type mismatch on any parameter, receives `Type = UNKNOWN`.

## Source

`internal/services/contract_validator.go`
`docs/knowledge/references/token-ingestion.md` — SEP-41 Validation Logic (lines 226–241)

## Related

- [[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — the runtime that reads the contractspecv0 section
- [[unknown contract type is silently skipped in processTokenChanges because unclassified contracts cannot be safely stored]] — what happens when a contract fails this check

relevant_notes:
  - "[[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — mechanism: wazero is the tool; this entry defines the validation criteria that wazero applies"
  - "[[unknown contract type is silently skipped in processTokenChanges because unclassified contracts cannot be safely stored]] — consequence: failing any of the 10 checks produces UNKNOWN, which ProcessTokenChanges silently skips"
