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

**Per-processor behavior summary:**
- `ParticipantsProcessor` — walks classic operation source/destination + Soroban auth tree to build the address participant set
- `TokenTransferProcessor` — wraps SDK `EventsProcessor`; aggregates fee events to produce net fee StateChange; operates at tx level because the SDK aggregates across all operations
- `EffectsProcessor` — classifies 50+ SDK effect types into seven StateChange categories (PAYMENT, TRADE, OFFER, CLAIMABLE_BALANCE, LIQUIDITY_POOL, BALANCE_CHANGE, BALANCE_AUTHORIZATION)
- `ContractDeployProcessor` — recursively walks Soroban auth invocation tree to find `create_contract` calls at any depth
- `SACEventsProcessor` — parses SAC diagnostic events for token transfers and authorization changes; branches on tx meta version for `set_authorized` layout differences
- `TrustlinesProcessor` — reads `Post` ledger entry for absolute trustline balance+flags snapshot; skips G-address SAC balances (handled by SACBalancesProcessor)
- `AccountsProcessor` — reads `Post` account entry; computes derived `minBalance` from subentry counts; skips signer-only changes via `AccountChangedExceptSigners`
- `SACBalancesProcessor` — tracks only C-address SAC holders; G-address SAC balances are already captured by TrustlinesProcessor
- `SACInstanceProcessor` — records new SAC contract instances; classifies as SAC vs SEP-41 via deterministic contract ID derivation

**WHY transaction vs operation level:** `TokenTransferProcessor` must operate at the transaction level because the SDK's `EventsProcessor` aggregates fee refund events across all operations before emitting — there is no per-operation fee event to intercept. All other processors operate at the operation level because their inputs (operation body, ledger changes) are scoped per-operation.

**Parallelism model:** The three `OperationProcessorInterface` processors run sequentially per operation — not in a pool. Since [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]], creating a pool for only 3 processors adds more scheduling overhead than the parallelism saves. The four `LedgerChangeProcessor` processors also run sequentially.

The interface distinction matters: `ParticipantsProcessorInterface` and `TokenTransferProcessorInterface` operate at the transaction level (one call per transaction). `OperationProcessorInterface` and `LedgerChangeProcessor` operate at the operation level (one call per operation within the transaction).

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]] — explains why only transactions are parallelized, not operations
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — the parent parallel architecture

Areas:
- [[entries/ingestion]]
