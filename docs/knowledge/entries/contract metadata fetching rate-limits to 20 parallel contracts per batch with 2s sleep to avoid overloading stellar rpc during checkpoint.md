---
context: The 20-contract batch size and 2s sleep are hardcoded constants in contract_metadata.go — without this throttle, a checkpoint with thousands of new SEP-41 contracts would overwhelm the RPC node
type: insight
status: active
created: 2026-02-26
---

# contract metadata fetching rate-limits to 20 parallel contracts per batch with 2s sleep to avoid overloading Stellar RPC during checkpoint

`ContractMetadataService.fetchSep41Metadata()` uses `pond.Pool` to rate-limit RPC simulation calls:

```go
const simulateTransactionBatchSize = 20  // parallel contracts per batch
const batchSleepDuration = 2 * time.Second

// For each batch of 20 contracts:
pool := pond.New(simulateTransactionBatchSize, 0)
for i, contractID := range contractIDs {
    pool.Submit(func() {
        // SimulateTransaction for name(), symbol(), decimals()
    })
    if (i+1) % simulateTransactionBatchSize == 0 {
        pool.StopAndWait()
        time.Sleep(batchSleepDuration)
        pool = pond.New(simulateTransactionBatchSize, 0)
    }
}
```

Each contract requires 3 separate RPC simulate calls (one per function). At 20 contracts per batch, that is 60 concurrent simulate calls per batch window, followed by a 2-second pause.

## Why rate-limiting matters at checkpoint

During the initial `PopulateAccountTokens()` run, the subsystem may encounter thousands of SEP-41 contracts that need metadata. Without throttling, this would fire all simulate calls simultaneously, potentially overwhelming the Stellar RPC node and causing timeout errors that abort the entire checkpoint.

## Source

`internal/services/contract_metadata.go` — constants `simulateTransactionBatchSize`, `batchSleepDuration`
`docs/knowledge/references/token-ingestion.md` — Constants table (lines 373–377)

## Related

- [[rpc metadata simulation uses a dummy random keypair because simulate_transaction does not validate signatures or balances]] — the simulation mechanism that these batches call
- [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — another protection pattern against overwhelming shared resources during ingestion

relevant_notes:
  - "[[rpc metadata simulation uses a dummy random keypair because simulate_transaction does not validate signatures or balances]] — grounds this: the dummy-keypair simulate calls are the unit being rate-limited; this entry explains the rate-limiting wrapper"
