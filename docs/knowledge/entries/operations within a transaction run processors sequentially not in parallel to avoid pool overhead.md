---
context: Only 3 OperationProcessorInterface processors per operation; creating a pond.Pool per operation adds more scheduling overhead than parallelism saves; transactions are parallelized, operations are not
type: decision
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# operations within a transaction run processors sequentially not in parallel to avoid pool overhead

The Indexer parallelizes at the transaction level but not at the operation level. Within a single transaction's processing, `OperationProcessorInterface` processors (`EffectsProcessor`, `ContractDeployProcessor`, `SACEventsProcessor`) run sequentially per operation. `LedgerChangeProcessor` processors also run sequentially.

The explicit justification from the code: with only 3 `OperationProcessorInterface` processors, creating a `pond.Pool` per operation adds overhead (goroutine creation, scheduling, channel management) that exceeds the savings from parallelism. Each processor call is fast — microseconds for most operations. Pool overhead is measured in tens of microseconds just for the scheduling primitives.

The contrast with transaction-level parallelism: transactions are parallelized because:
1. There can be hundreds of transactions per ledger
2. Each transaction's processing involves all nine processors across all operations — significant CPU work
3. Transactions are completely independent (no shared state)

Operations within a transaction are different:
1. There are typically 1-5 operations per transaction
2. Each operation's processor calls are individually fast
3. Pool overhead per operation would dominate

This is a recurring design pattern in performance-sensitive Go code: parallelize at the coarse grain (transactions), process sequentially at the fine grain (operations within a transaction). The crossover point where parallelism overhead exceeds savings is lower than intuition suggests.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — the level at which parallelism IS used
- [[nine specialized processors run per transaction in the indexer fan-out]] — the processors that run sequentially per-operation

Areas:
- [[entries/ingestion]]
