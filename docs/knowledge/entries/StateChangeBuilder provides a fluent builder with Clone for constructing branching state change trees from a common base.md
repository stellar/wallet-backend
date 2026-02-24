---
context: Multiple StateChanges often share the same transaction/operation context; Clone avoids re-setting common fields; fluent API mirrors how processors construct variations
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# StateChangeBuilder provides a fluent builder with Clone for constructing branching state change trees from a common base

`StateChangeBuilder` is a fluent builder for constructing `StateChange` structs. It supports a `Clone()` method that creates a deep copy of the in-progress builder state, enabling processors to set shared context fields once and then branch into multiple specialized records.

The typical usage pattern in processors:

```go
base := NewStateChangeBuilder().
    WithTransaction(tx).
    WithOperation(op).
    WithLedgerNumber(ledger)

// Branch 1: payment to destination
paymentChange := base.Clone().
    WithCategory(PAYMENT).
    WithAddress(destination).
    WithAmount(amount).
    Build()

// Branch 2: payment from source
feeChange := base.Clone().
    WithCategory(FEE).
    WithAddress(source).
    WithAmount(fee).
    Build()
```

Without `Clone()`, each `StateChange` would require re-setting the transaction context, operation index, and ledger number — fields that are identical for all changes within a single operation. The branching pattern is especially useful in `EffectsProcessor` and `TokenTransferProcessor` where a single operation can produce multiple state changes sharing the same provenance.

---

Relevant Notes:
- [[state change ordering uses per-operation 1-indexed counters set after sort-key-based deterministic sorting]] — the ordering applied after builder produces records
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — the output type the builder produces

Areas:
- [[entries/ingestion]]
