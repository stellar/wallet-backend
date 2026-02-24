---
context: Three interface types — ParticipantsProcessorInterface, OperationProcessorInterface (3 processors, sequential per-op), LedgerChangeProcessor (4 processors, sequential per-op)
type: reference
status: current
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# nine specialized processors run per transaction in the indexer fan-out

The Indexer registers nine processors that run against each transaction during the fan-out phase:

| Processor | Interface | Level | Output |
|-----------|-----------|-------|--------|
| `ParticipantsProcessor` | `ParticipantsProcessorInterface` | Transaction | Participant sets (Stellar addresses) |
| `TokenTransferProcessor` | `TokenTransferProcessorInterface` | Transaction | `[]StateChange` (balance changes) |
| `EffectsProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` (payments, trades) |
| `ContractDeployProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` (Soroban deployments) |
| `SACEventsProcessor` | `OperationProcessorInterface` | Operation | `[]StateChange` (SAC events) |
| `TrustlinesProcessor` | `LedgerChangeProcessor[TrustlineChange]` | Operation | `[]TrustlineChange` |
| `AccountsProcessor` | `LedgerChangeProcessor[AccountChange]` | Operation | `[]AccountChange` |
| `SACBalancesProcessor` | `LedgerChangeProcessor[SACBalanceChange]` | Operation | `[]SACBalanceChange` |
| `SACInstanceProcessor` | `LedgerChangeProcessor[*data.Contract]` | Operation | `[]*data.Contract` |

**Parallelism model:** The three `OperationProcessorInterface` processors run sequentially per operation — not in a pool. Since [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]], creating a pool for only 3 processors adds more scheduling overhead than the parallelism saves. The four `LedgerChangeProcessor` processors also run sequentially.

The interface distinction matters: `ParticipantsProcessorInterface` and `TokenTransferProcessorInterface` operate at the transaction level (one call per transaction). `OperationProcessorInterface` and `LedgerChangeProcessor` operate at the operation level (one call per operation within the transaction).

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]] — explains why only transactions are parallelized, not operations
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — the parent parallel architecture

Areas:
- [[entries/ingestion]]
