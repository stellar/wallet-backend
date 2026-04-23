# H003: Checkpoint metadata fetch reads unbounded `simulateTransaction` responses into memory

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: availability / RPC-induced OOM
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Metadata simulation for checkpoint bootstrap should cap RPC response size before reading it into memory. A contract's `name()` implementation should not be able to force the wallet backend to buffer an arbitrarily large JSON/XDR simulation response during initialization.

## Mechanism

Once checkpoint finalization decides a contract needs metadata, `FetchSingleField()` calls `simulateTransaction` and `rpcService.sendRPCRequest()` immediately `io.ReadAll`s the entire HTTP body before any size check. A fake SAC-like contract can therefore turn on-chain-controlled `name()` output, events, or state changes into a large simulation payload that is fully materialized in memory and then decoded again through `json.Unmarshal`, producing an RPC-induced memory spike or OOM during bootstrap.

## Trigger

1. Make checkpoint enqueue a contract for metadata fetch (for example via the balance-only SAC placeholder path).
2. Implement `name()` so simulation returns a very large value and/or large side-channel fields (`events`, `stateChanges`, auth).
3. Start bootstrap and observe `simulateTransaction` response buffering in `sendRPCRequest()` before any limit or streaming decoder is applied.

## Target Code

- `internal/services/checkpoint.go:416-419` — checkpoint bootstrap invokes metadata simulation inside finalization
- `internal/services/contract_metadata.go:96-123` — batches up to 20 metadata simulations in parallel
- `internal/services/contract_metadata.go:178-237` — `FetchSingleField()` drives `simulateTransaction`
- `internal/services/rpc_service.go:349-373` — reads the full RPC body with `io.ReadAll` and then unmarshals it
- `internal/entities/rpc.go:247-284` — unmarshals full simulation results, including `events` and `stateChanges`

## Evidence

There is no response-size guard anywhere between `httpClient.Post(...)` and `io.ReadAll(resp.Body)`. The same raw bytes are then unmarshaled into structured Go values, so a single large response incurs at least two full in-memory representations before checkpoint can reject it.

## Anti-Evidence

Soroban and RPC may impose upstream limits on result size, so the practical ceiling depends on network configuration. But the wallet backend adds no local bound of its own, so any upstream-allowed payload is accepted wholesale by the bootstrap path.
