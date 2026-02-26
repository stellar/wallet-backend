---
context: Architectural choice: no CGo, no external WASM process — wazero compiles WASM bytecode in-process and reads the contractspecv0 custom section to extract XDR-encoded function signatures
type: decision
status: active
created: 2026-02-26
---

# wazero pure-Go WASM runtime validates SEP-41 contract specs in-process by compiling WASM and parsing the contractspecv0 custom section

`ContractValidator` uses [wazero](https://github.com/tetratelabs/wazero), a pure-Go WASM runtime, to validate SEP-41 contracts without CGo or external process dependencies:

```go
// contract_validator.go
runtime := wazero.NewRuntime(ctx)
module, _ := runtime.CompileModule(ctx, wasmBytes)

// Extract the contractspecv0 custom section
for _, section := range module.CustomSections() {
    if section.Name() == contractSpecV0SectionName { // "contractspecv0"
        // Unmarshal XDR-encoded ScSpecEntry list
        // Verify all 10 required SEP-41 function signatures
    }
}
```

## The contractspecv0 custom section

Soroban contracts embed their public interface in a WASM custom section named `contractspecv0`. This section contains XDR-encoded `ScSpecEntry` records describing every exported function — its name, input types, and return type. The validator reads this section to determine if a contract implements the full SEP-41 token interface.

## Why wazero (not wasmer, not CGo)

wazero is entirely written in Go with no CGo bindings, which means:
- No OS-level shared library dependencies
- No cross-compilation complications
- Works in Docker containers without system WASM packages
- No CGo overhead in build pipeline

The tradeoff is performance: wazero is slower than native WASM runtimes. For contract classification (run once per contract during checkpoint), this is acceptable.

## Source

`internal/services/contract_validator.go`
`docs/knowledge/references/token-ingestion.md` — SEP-41 Validation Logic (lines 222–241)

## Related

- [[sep-41 contract classification uses the contractspecv0 wasm custom section to verify all 10 required function signatures before accepting a contract as valid]] — the validation logic that runs inside this WASM pipeline
- [[unknown contract type is silently skipped in processTokenChanges because unclassified contracts cannot be safely stored]] — what happens when this validator returns non-SEP-41

relevant_notes:
  - "[[sep-41 contract classification uses the contractspecv0 wasm custom section to verify all 10 required function signatures before accepting a contract as valid]] — grounds this: the contractspecv0 custom section reading is how wazero implements the 10-function validation check"
  - "[[unknown contract type is silently skipped in processTokenChanges because unclassified contracts cannot be safely stored]] — consequence: wazero classification failure produces UNKNOWN, which is then silently skipped in processContractTokenChanges"
  - "[[options struct plus validateoptions enables compile-time-visible dependency validation]] — explains exception: wazero's in-process pure-Go design makes ContractValidator the one zero-dep service in the codebase; the no-CGo constraint is why no external runtime needs injecting"
