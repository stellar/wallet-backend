# H005: Protocol setup compiles and unmarshals `contractspecv0` without any local CPU or size limits

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: availability / WASM parser exhaustion
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

WASM classification should impose explicit limits on compile cost and `contractspecv0` decode volume before treating uploaded bytecode as safe to analyze. A single on-chain WASM blob should not be able to monopolize classifier CPU or memory simply by embedding a huge or pathological custom section.

## Mechanism

After fetching bytecode, `ProtocolSetupService.classify()` sends it straight into `wasmSpecExtractor.ExtractSpec()`, which compiles the full module with wazero and then repeatedly `xdr.Unmarshal`s spec entries until EOF. There is no limit on module size, custom-section length, or number of decoded `ScSpecEntry` values, so attacker-uploaded WASM can turn protocol classification into a CPU/memory exhaustion path even when the module is otherwise valid enough to compile.

## Trigger

1. Upload a WASM whose `contractspecv0` custom section is extremely large or contains a very large number of tiny spec entries.
2. Run protocol setup so classification fetches that bytecode.
3. Observe `ExtractSpec()` compile the module and iterate `xdr.Unmarshal` until the entire section is consumed, with no byte or entry cap.

## Target Code

- `internal/services/protocol_setup.go:175-180` — classification feeds every fetched bytecode into `ExtractSpec()`
- `internal/services/protocol_validator.go:35-42` — creates a default wazero runtime with custom sections enabled but no explicit resource limits
- `internal/services/protocol_validator.go:41-79` — compiles the full module and unmarshals spec entries until EOF

## Evidence

`ExtractSpec()` has only two structural checks: the module must compile, and it must contain a `contractspecv0` section. Once those pass, the decoder appends entries until the section is exhausted, and nothing in the call chain enforces a maximum section length or maximum entry count.

## Anti-Evidence

Wazero may reject some malformed modules early, and upstream network rules bound overall contract size. But valid large modules still reach the expensive compile-and-decode path, and there is no wallet-backend-side limit that turns those upstream bounds into a safe local resource budget.
