---
context: The dummy keypair is not a security shortcut — simulation on Stellar RPC is structurally execution-only; auth and balance checks are not part of the simulation protocol
type: gotcha
status: active
created: 2026-02-26
---

# RPC metadata simulation uses a dummy random keypair because simulate_transaction does not validate signatures or balances

When `ContractMetadataService` fetches contract metadata (`name()`, `symbol()`, `decimals()`), it constructs a transaction using `keypair.MustRandom()` as the source account:

```go
// contract_metadata.go
sourceAccount := keypair.MustRandom()
tx := txnbuild.Transaction{
    SourceAccount: &txnbuild.SimpleAccount{
        AccountID: sourceAccount.Address(),
    },
    Operations: []txnbuild.Operation{invokeContractOp},
}
result, _ := rpcClient.SimulateTransaction(ctx, tx)
```

The random keypair is a throwaway — its address doesn't need to exist on-chain and its private key is never used for signing. This works because `simulate_transaction` on Stellar RPC executes the contract in a sandbox and returns the return value; it does not validate:
- Whether the source account exists
- Whether the source account has sufficient balance for fees
- Whether the transaction is properly signed

## Implication

This is not a security concern — simulation is explicitly designed as a dry-run tool. The "account" in a simulation transaction is just a fee-payer placeholder; no funds are moved and no state is written.

## Source

`internal/services/contract_metadata.go`
`docs/knowledge/references/token-ingestion.md` — RPC Metadata Fetching section (approximately line 269)

## Related

- [[contract metadata fetching rate-limits to 20 parallel contracts per batch with 2s sleep to avoid overloading stellar rpc during checkpoint]] — the rate-limiting strategy that wraps these simulation calls

relevant_notes:
  - "[[contract metadata fetching rate-limits to 20 parallel contracts per batch with 2s sleep to avoid overloading stellar rpc during checkpoint]] — co-occurs: the dummy keypair simulation calls are grouped in batches of 20 with 2s sleep between batches"
