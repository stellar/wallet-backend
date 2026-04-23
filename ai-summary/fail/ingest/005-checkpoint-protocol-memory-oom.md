# H005: Checkpoint bootstrap retains all contract/WASM protocol bookkeeping in memory until EOF

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Checkpoint population should stream or batch protocol bookkeeping so a fresh node does not need to hold every unique contract token, WASM hash, and contract-to-WASM mapping in RAM until the entire checkpoint snapshot finishes. Large public chain state should degrade bootstrap linearly, not create a single in-memory accumulation point.

## Mechanism

`PopulateFromCheckpoint` processes the entire snapshot in one transaction and stores protocol-related state in process-local maps (`uniqueContractTokens`, `wasmClassifications`, `contractAddressesByWasmHash`) until `finalize` runs at EOF. An attacker who fills chain state with many uploaded WASMs and deployed contracts can therefore turn public ledger state into a deterministic bootstrap OOM / slow-start vector for any new or rebuilt ingest node.

## Trigger

1. Upload and deploy a large number of distinct contract bytecodes/contracts on chain.
2. Start a fresh ingest instance with an empty DB so `PopulateFromCheckpoint` runs.
3. Observe checkpoint processing retain all unique protocol bookkeeping entries in memory until the reader reaches EOF and `finalize` executes.

## Target Code

- `internal/services/checkpoint.go:101-115` — long-lived `uniqueContractTokens` checkpoint maps
- `internal/services/checkpoint.go:195-203,232-240` — in-memory `wasmClassifications` and `contractAddressesByWasmHash`
- `internal/services/checkpoint.go:334-338,375-377` — every matching contract/WASM is accumulated into those maps
- `internal/services/checkpoint.go:408-441` — persistence happens only once, during finalization

## Evidence

Balances are streamed in batches, but protocol bookkeeping is not: new hashes and contract mappings are appended to maps throughout the full reader loop and are only written after EOF. There is no intermediate flush, spill-to-disk, or configurable cap on those maps.

## Anti-Evidence

The attack is most effective against empty-DB bootstrap and rebuild flows rather than steady-state live ingestion. It also requires enough on-chain contract state to make the in-memory accumulation materially large.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full `PopulateFromCheckpoint` flow in `internal/services/checkpoint.go`. Confirmed that `checkpointProcessor` accumulates three unbounded maps (`wasmClassifications`, `contractAddressesByWasmHash`, `uniqueContractTokens`/`uniqueAssets`) throughout the entire checkpoint read loop (lines 243-268), flushing them only at `finalize` (lines 408-441). Meanwhile, balance data IS streamed via the `batch` struct with `flushBatchSize = 250_000` (lines 381-391). The asymmetry described in the hypothesis is real.

### Code Paths Examined

- `internal/services/checkpoint.go:101-115` — `checkpointData` struct holds `uniqueAssets` and `uniqueContractTokens` maps, confirmed unbounded
- `internal/services/checkpoint.go:194-205` — `checkpointProcessor` struct holds `wasmClassifications` (map[xdr.Hash]types.ContractType) and `contractAddressesByWasmHash` (map[xdr.Hash][]xdr.Hash), confirmed unbounded
- `internal/services/checkpoint.go:232-240` — maps initialized empty at start of `PopulateFromCheckpoint`
- `internal/services/checkpoint.go:258-260` — `processContractCode` appends to `wasmClassifications` for every ContractCode entry
- `internal/services/checkpoint.go:326-338` — `processEntry` appends to `uniqueContractTokens` and `contractAddressesByWasmHash` for contract instances
- `internal/services/checkpoint.go:375-377` — `processContractCode` unconditionally inserts every WASM hash into `wasmClassifications`
- `internal/services/checkpoint.go:408-441` — `finalize` is the sole consumer of these maps, called only after EOF
- `internal/services/checkpoint.go:381-391` — `flushBatchIfNeeded` streams balances at 250K threshold — protocol maps have no equivalent flush

### Why It Failed

The code pattern is real but the worst-case impact cannot reach Medium severity. Per-entry memory overhead is approximately 340 bytes (100 for wasmClassifications + 90 for contractAddressesByWasmHash + 150 for uniqueContractTokens). To consume 1GB of RAM requires ~3M unique deployed contracts; to reach a typical 4-8GB OOM threshold requires 12-24M contracts. Current Stellar mainnet has on the order of tens of thousands of contracts. Deploying millions of contracts on-chain to create the OOM payload would require enormous transaction fees, making the attack economically infeasible. Furthermore, the vulnerability only manifests during fresh bootstrap (empty-DB) flows, not steady-state operation, and operators can provision sufficient RAM for bootstrap. The finding is a legitimate design improvement suggestion (Informational severity) but falls below the Medium minimum threshold required by the objective's severity scale.

### Lesson Learned

Unbounded in-memory accumulation during checkpoint processing is a real pattern worth noting for future design reviews, but the economic cost of inflating Stellar chain state (contract deployment fees) creates a natural bound that makes OOM from this path impractical at current and foreseeable chain sizes. Future hypotheses about checkpoint OOM should quantify the on-chain cost to reach meaningful memory pressure.
