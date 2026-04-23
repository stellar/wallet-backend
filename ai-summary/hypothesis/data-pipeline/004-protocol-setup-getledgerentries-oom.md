# H004: Protocol setup batches 200 contract-code fetches into one unbounded `getLedgerEntries` response

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: availability / protocol-setup OOM
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Protocol classification should bound the size of each RPC response or reduce batch size based on byte volume, not just key count. An attacker should not be able to upload enough large WASM blobs that a single `getLedgerEntries` call forces the classifier to buffer a massive response body in RAM.

## Mechanism

`ProtocolSetupService.fetchWasmBytecodes()` batches up to 200 contract-code keys per RPC call and passes the response through `rpcService.GetLedgerEntries()`, which ultimately uses `sendRPCRequest()`'s unbounded `io.ReadAll`. If an attacker fills `protocol_wasms` with many large uploaded bytecodes, one classification run can fetch hundreds of base64-encoded WASM blobs in a single HTTP response, creating a memory-amplification path that can crash or stall the protocol-setup job.

## Trigger

1. Upload many large contract bytecodes so they appear in `protocol_wasms` as unclassified hashes.
2. Run protocol setup for a validator set that scans those hashes.
3. Observe `fetchWasmBytecodes()` sending `getLedgerEntries` requests with up to 200 keys and `sendRPCRequest()` reading the entire JSON payload into memory at once.

## Target Code

- `internal/services/protocol_setup.go:168-173` — classification fetches all unclassified bytecodes before validation
- `internal/services/protocol_setup.go:241-271` — `fetchWasmBytecodes()` batches up to `rpcLedgerEntryBatchSize = 200` and decodes every returned `ContractCode`
- `internal/services/rpc_service.go:349-373` — full-body buffering via `io.ReadAll` with no response-size cap

## Evidence

The classifier's batching threshold is a count-only constant (`rpcLedgerEntryBatchSize = 200`), not a byte budget. Each returned code entry includes base64-expanded bytecode inside JSON, and the transport layer has no mechanism to reject overlarge bodies before allocation.

## Anti-Evidence

Maximum contract-code size is bounded by Soroban network settings, so the exploit depends on those limits being large enough and enough uploaded contracts being available simultaneously. Even with those limits, though, the classifier's local memory use scales with `200 * max_code_size` rather than a bounded response target.
