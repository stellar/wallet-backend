---
description: SEP-35 encodes (ledger_seq, tx_order, op_index) in 64 bits; lower 12 bits = op_index; masking with &^ 0xFFF yields tx TOID; used in DataLoader grouping and SQL joins
type: insight
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, stellar, sep-35, toid, graphql, dataloaders]
---

# TOID bit-masking recovers parent transaction ID from operation ID without a join

## Context

Operations have a `to_id` (TOID) that encodes their position in the blockchain. The DataLoader that groups operations by parent transaction needs to derive the transaction TOID from each operation's TOID efficiently.

## Detail

Per [SEP-35](https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md), a TOID encodes three values in 64 bits:
- Upper bits: `ledger_sequence`
- Middle bits: `transaction_order` within the ledger
- Lower 12 bits: `operation_index` within the transaction

For a transaction TOID, `operation_index = 0`. Masking the lower 12 bits from an operation TOID recovers its parent transaction TOID:

```go
tx_to_id = operation_id &^ 0xFFF  // in Go (DataLoader grouping)
```

```sql
-- in SQL (BatchGetByOperationIDs join)
ON (o.id & (~x'FFF'::bigint)) = transactions.to_id
```

This avoids a separate lookup table or FK join to find a transaction from an operation ID.

## Implications

- The TOID encoding is part of the Stellar protocol (SEP-35) — it will not change, making this mask stable.
- When debugging TOID values, remember: `op_id >> 12 = tx_toid >> 12` (same ledger+tx prefix, only op_index differs).
- The SQL and Go forms use equivalent bit operations; verify both when making changes.

## Source

`internal/serve/graphql/dataloaders/dataloaders.go` — Go masking in `operationsByToIDLoader`
`internal/data/transactions.go:BatchGetByOperationIDs()` — SQL masking

## Related

The TOID bitmask is used inside the DataLoaders created by [[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — `operationsByToIDLoader` uses the Go mask to group operations by parent transaction without a DB join.

The bitmask is also used in the SQL batch queries described in [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — `BatchGetByOperationIDs` applies the SQL mask `(o.id & (~x'FFF'::bigint))` as the join condition to `transactions`.

relevant_notes:
  - "[[dataloader per-request creation prevents cross-request data leakage in horizontally-scaled deployments]] — exemplifies this: operationsByToIDLoader uses the Go-side TOID mask to group operations by parent transaction; the bitmask enables this grouping without a DB roundtrip"
  - "[[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — extends this: BatchGetByOperationIDs uses the SQL-side TOID mask as a join condition alongside ROW_NUMBER partitioning; both techniques appear in the same batch query"
