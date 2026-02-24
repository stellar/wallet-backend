---
context: OperationProcessorInterface calls are made for every operation; processors that only handle certain operation types return this sentinel to signal a non-error skip
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# processors use ErrInvalidOpType sentinel error to gracefully skip non-applicable operations

All processors implementing `OperationProcessorInterface` receive every operation in a transaction. Processors that only handle specific operation types (e.g., `ContractDeployProcessor` only processes Soroban `InvokeContractOp`) use a sentinel error `ErrInvalidOpType` to signal that an operation is not applicable — this is not an error condition, just a skip.

The caller in the Indexer fan-out checks for `ErrInvalidOpType` explicitly:

```go
err := processor.ProcessOperation(op, tx, change)
if errors.Is(err, ErrInvalidOpType) {
    continue // skip, not an error
}
if err != nil {
    return err // real error
}
```

This pattern avoids requiring each processor to maintain an explicit allowlist check in its own error handling — the sentinel communicates the skip intent more cleanly than returning `nil` (which is ambiguous between "processed successfully" and "nothing to do") or checking operation type before calling.

---

Relevant Notes:
- [[nine specialized processors run per transaction in the indexer fan-out]] — all nine processors that may return this sentinel
- [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]] — the loop where this sentinel is checked

Areas:
- [[entries/ingestion]]
