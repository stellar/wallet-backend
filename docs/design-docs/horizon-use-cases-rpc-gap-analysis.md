# Horizon Use Cases: Stellar RPC Gap Analysis

## Purpose

SDF will shut down its public Horizon instances and deprecate the Horizon codebase. This document lists the use cases that Horizon serves today. For each use case, it describes how to serve it with Stellar RPC and rates the migration effort.

For use cases with a medium or high rating, the document also lists other alternatives and the gaps that remain.

## Scope

- Source of use cases: the Horizon public HTTP API (`internal/httpx/router.go`) and Horizon configuration flags (`internal/flags.go`).
- Target: Stellar RPC, including the archive node (beta) that serves full history.
- Out of scope: features that only RPC has, such as `simulateTransaction` and contract state reads.

## Rating scale

| Rating | Meaning |
|---|---|
| Low | An RPC method gives the same data. The client changes the request format and decodes XDR. The client stores no data. |
| Medium | RPC gives the raw data, but the client must filter, derive, or poll for it. The client may keep a small amount of local state. The client does not index history. |
| High | The client must run its own ingestion and database, or rebuild complex server logic (for example, path finding). Or RPC does not have the data. |

## Use cases: public API

### 1. Network and node information

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 1.1 | Get network passphrase, protocol version, and node version | `GET /` | `getNetwork`, `getVersionInfo` | Low | |
| 1.2 | Check node health and latest ledger | `GET /health`, `GET /` | `getHealth`, `getLatestLedger` | Low | |
| 1.3 | Fund a testnet account | `GET/POST /friendbot` (redirect) | `getNetwork` returns `friendbotUrl` | Low | Client calls friendbot directly. |
| 1.4 | Get fee statistics | `GET /fee_stats` | `getFeeStats` | Low | RPC splits Soroban and classic inclusion fees. |

### 2. Transaction submission

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 2.1 | Submit a transaction and wait for the result | `POST /transactions` | `sendTransaction`, then poll `getTransaction` | Low | Client adds a poll loop. SDKs provide helpers. |
| 2.2 | Submit a transaction without waiting | `POST /transactions_async` | `sendTransaction` | Low | |
| 2.3 | Get the next sequence number for a source account | `GET /accounts/{id}` (`sequence`) | `getLedgerEntries` with the account key | Low | |

### 3. Account state

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 3.1 | Get account details (native balance, thresholds, flags, signers, home domain) | `GET /accounts/{id}` | `getLedgerEntries` with the account key | Low | Client decodes `AccountEntry` XDR. |
| 3.2 | Get one trustline balance for a known asset | `GET /accounts/{id}` (`balances`) | `getLedgerEntries` with the trustline key | Low | |
| 3.3 | List all balances (trustlines) of an account | `GET /accounts/{id}` (`balances`) | No listing method. Client must know the asset list, then call `getLedgerEntries`. | High | Most applications need all assets an account holds, not a fixed list. Trustlines are not discoverable without indexing history. |
| 3.4 | Get one account data entry | `GET /accounts/{id}/data/{key}` | `getLedgerEntries` with the data key | Low | |
| 3.5 | List all data entries of an account | `GET /accounts/{id}` (`data`) | No listing method | High | Keys are not discoverable without indexing. |
| 3.6 | Get contract token (SEP-41) balances | Partial (`/assets` contract stats only) | `getLedgerEntries` on contract data, or `simulateTransaction` of `balance` | Low | RPC is the better source today. |
| 3.7 | Watch an account for changes | SSE on `GET /accounts/{id}` | Poll `getLedgerEntries` | High | Polling covers only entries with known keys. Horizon returns all balances and data entries of the account. RPC cannot discover them (see 3.3 and 3.5). |

### 4. Account discovery (reverse lookups)

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 4.1 | Find accounts by signer (for example, multisig and key recovery) | `GET /accounts?signer=` | None | High | |
| 4.2 | Find accounts that hold an asset | `GET /accounts?asset=` | None | High | |
| 4.3 | Find accounts by sponsor | `GET /accounts?sponsor=` | None | High | |
| 4.4 | Find accounts that hold liquidity pool shares | `GET /accounts?liquidity_pool=` | None | High | |

### 5. Transaction and operation history

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 5.1 | Get a transaction by hash | `GET /transactions/{hash}` | `getTransaction` | Low | Outside the retention window, needs an archive node. |
| 5.2 | Get the operations of one transaction | `GET /transactions/{hash}/operations` | `getTransaction`, then decode the envelope | Low | Client decodes operations from XDR. |
| 5.3 | Get an operation by ID | `GET /operations/{id}` | Derive the transaction from the ID (TOID), then `getLedgers` / `getTransactions` | Medium | No direct lookup by operation ID. |
| 5.4 | List transactions in a ledger or ledger range | `GET /ledgers/{seq}/transactions`, `GET /transactions` | `getTransactions` | Low | Includes failed transactions. |
| 5.5 | Transaction history for an account | `GET /accounts/{id}/transactions` | None. `getTransactions` has no account filter. | High | Client must scan all transactions and index by participant. |
| 5.6 | Operation history for an account | `GET /accounts/{id}/operations` | None | High | Same as 5.5. |
| 5.7 | History for a claimable balance or liquidity pool | `GET /claimable_balances/{id}/{transactions,operations}`, `GET /liquidity_pools/{id}/{transactions,operations,effects,trades}` | None | High | |
| 5.8 | Stream new transactions or operations | SSE on list endpoints | Poll `getTransactions` from the last cursor | Medium | Client manages cursors and filters. |

### 6. Payments and balance changes

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 6.1 | Payment history for an account | `GET /accounts/{id}/payments` | `getEvents` / `queryEvents` with topic filters on the account address (`transfer`, `mint`, `burn`, `clawback`) | Medium | Classic events (CAP-67) are on by default in RPC. Events for ledgers before protocol 23 need `BACKFILL_STELLAR_ASSET_EVENTS`. The archive node reads these ledgers from a Galexie data lake, and the default Galexie configuration enables this flag. Event shape differs from Horizon operations. Full history needs an archive node. |
| 6.2 | Detect incoming payments (deposit monitoring) | SSE on `GET /accounts/{id}/payments` | Poll `getEvents` with a topic filter on the receiving address | Medium | Memo is in the transaction, or in `to_muxed_id` in the event. Client stores the last cursor. |
| 6.3 | Global payment feed | `GET /payments` | Poll `getEvents` without an address filter | Medium | |
| 6.4 | Effects (fine-grained state changes per operation) | `GET /effects` and sub-resources | None. Partial coverage from events and transaction meta. | High | Effects are a Horizon-specific model. Client must derive them from `LedgerEntryChanges` in transaction meta. |

### 7. Ledgers

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 7.1 | Get a ledger by sequence | `GET /ledgers/{seq}` | `getLedgers` with `startLedger` and `limit: 1` | Low | Client decodes the ledger header XDR. Horizon fields such as operation counts must be derived. |
| 7.2 | List or stream ledgers | `GET /ledgers` (SSE) | Poll `getLedgers` | Low | |
| 7.3 | Payments, operations, or effects in a ledger | `GET /ledgers/{seq}/{payments,operations,effects}` | `getTransactions` or `getEvents` for one ledger | Medium | Client derives operations and payments. Effects: see 6.4. |

### 8. Claimable balances

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 8.1 | Get a claimable balance by ID | `GET /claimable_balances/{id}` | `getLedgerEntries` with the claimable balance key | Low | |
| 8.2 | Find claimable balances by claimant, sponsor, or asset | `GET /claimable_balances?claimant=&sponsor=&asset=` | None | High | Common for wallets that show pending claims. |

### 9. Liquidity pools

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 9.1 | Get a liquidity pool by ID | `GET /liquidity_pools/{id}` | `getLedgerEntries` with the pool key | Low | |
| 9.2 | Find pools by reserve assets or by participant account | `GET /liquidity_pools?reserves=&account=` | None | High | Pool ID for a known asset pair can be computed. Then use 9.1. |

### 10. DEX: offers, order books, trades

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 10.1 | Get an offer by ID | `GET /offers/{id}` | `getLedgerEntries` with the offer key (needs seller ID and offer ID) | Low | |
| 10.2 | List offers of an account | `GET /accounts/{id}/offers` | None | High | |
| 10.3 | Find offers by asset or sponsor | `GET /offers?selling=&buying=&seller=&sponsor=` | None | High | |
| 10.4 | Get the order book for an asset pair | `GET /order_book` (SSE) | None | High | Client must index all offers. |
| 10.5 | Trade history (global, by account, offer, pool, or asset pair) | `GET /trades` and sub-resources | None. Trades are in transaction meta (`ClaimAtom`). | High | |
| 10.6 | Trade aggregations (OHLC candles) | `GET /trade_aggregations` | None | High | |

### 11. Path finding

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 11.1 | Find payment paths (strict send and strict receive) | `GET /paths/strict-send`, `GET /paths/strict-receive` | None | High | Needs a full in-memory order book and pool graph. |

### 12. Assets

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 12.1 | Asset statistics (supply, number of holders, issuer flags) | `GET /assets` | None. Issuer flags only, through `getLedgerEntries` on the issuer account. | High | |
| 12.2 | Search assets by code or issuer | `GET /assets?asset_code=&asset_issuer=` | None | High | |

## Use cases: operation and configuration

These use cases apply to teams that run their own Horizon instance.

| ID | Use case | Horizon | RPC | Effort | Notes |
|---|---|---|---|---|---|
| 13.1 | Run a node with limited history | `--history-retention-count` | `--history-retention-window` | Low | |
| 13.2 | Run a node with full history | Default (no reaping), `horizon db reingest range` | Archive node (beta), backed by a ledger datastore (`serve-ledgers-from-datastore`) | Medium | Archive node is in beta. Operator runbook: `stellar-rpc/docs/ARCHIVE-NODE-BETA-RUNBOOK.md`. |
| 13.3 | Ingest only selected accounts or assets | `--exp-enable-ingestion-filtering`, admin `/ingestion/filters` | None | Medium | RPC stores raw data and has a smaller footprint, so filtering is less needed. |
| 13.4 | Run read-only API nodes against a shared database | `--ro-database-url`, `--ingest=false` | None. Each RPC node ingests into a local database. | Medium | Scale by running more RPC nodes. |
| 13.5 | Rate limit by client IP | `--per-hour-rate-limit` | Per-method queue limits and execution timeouts only | Low | Use a reverse proxy for per-IP limits. |
| 13.6 | Disable transaction submission or path finding | `--disable-tx-sub`, `--disable-path-finding` | No per-method disable flag | Low | Block methods at a reverse proxy. |
| 13.7 | TLS and load balancer client IP handling | `--tls-cert`, `--tls-key`, `--behind-cloudflare`, `--behind-aws-load-balancer` | None | Low | Terminate TLS at a reverse proxy. |
| 13.8 | Metrics and admin endpoints | `--admin-port` (`/metrics`, pprof) | `--admin-endpoint` (`/metrics`, pprof) | Low | |
| 13.9 | Captive Core configuration | `--captive-core-config-path`, `--stellar-core-binary-path` | Same options | Low | RPC enables `EMIT_CLASSIC_EVENTS` by default (see 6.1). |
| 13.10 | Reduce storage by dropping transaction meta | `--skip-txmeta` | None | Low | RPC keeps meta only within the retention window. |

## Summary

| Effort | Use cases |
|---|---|
| Low | 1.1–1.4, 2.1–2.3, 3.1, 3.2, 3.4, 3.6, 5.1, 5.2, 5.4, 7.1, 7.2, 8.1, 9.1, 10.1, 13.1, 13.5–13.10 |
| Medium | 5.3, 5.8, 6.1–6.3, 7.3, 13.2–13.4 |
| High | 3.3, 3.5, 3.7, 4.1–4.4, 5.5–5.7, 6.4, 8.2, 9.2, 10.2–10.6, 11.1, 12.1, 12.2 |

Pattern: RPC replaces lookups by primary key and raw ledger or transaction reads. RPC does not replace listing all entries of an account (balances, data), reverse lookups (by account, asset, sponsor, signer), account-scoped history, or derived DEX data. These need an indexer.

## Alternatives and remaining gaps

This section covers each use case with a medium or high RPC rating. It lists alternatives to RPC that are ready to use, and the gap that remains when RPC and all alternatives are considered. It does not rate migration effort or propose solutions.

Operation and configuration use cases (section 13) are out of scope for this section.

### Ready-to-use alternatives

| Name | Type | Interface | Scope |
|---|---|---|---|
| [wallet-backend](https://github.com/stellar/wallet-backend) | Indexer. Self-hosted, or hosted by infrastructure providers. | GraphQL | Per-account history (transactions, operations, state changes) and current balances (native, trustlines, SAC, SEP-41, liquidity pool shares). Lookups by account, transaction hash, and operation ID only. |
| [Alchemy Stellar Data API](https://www.alchemy.com/docs/reference/stellar-data-api-overview) | Hosted data API | JSON over HTTP | Per-address transfer history, balances, and NFT holdings. Covers classic assets and contract tokens. |
| [Stellar Indexer](https://stellarindexer.com/) | Hosted data API | REST, with a TypeScript SDK | Soroban contract data, with extensions for specific protocols (for example, Blend) |
| [StellarExpert API](https://stellar.expert/openapi.html) | Hosted explorer API | REST | Asset statistics, asset holders, account history, and market data |
| [OBSRVR](https://docs.withobsrvr.com/) | Hosted data provider | REST (Gateway API), data streams (Flow) | Account and asset queries, structured ledger data, and contract events |

### Indexing platforms

Indexing platforms run indexers that a developer writes. A platform is not a ready-to-use alternative for a use case unless an indexer (for example, a subgraph, Substreams package, or pipeline) for that data already exists. The tables below list a platform only where such an indexer or dataset exists.

| Name | Type | Notes |
|---|---|---|
| [Goldsky](https://docs.goldsky.com/chains/stellar) | Hosted streaming pipelines (Turbo) to the client's database | Provides prebuilt Stellar datasets, for example `stellar_mainnet.transactions`. Subgraphs are not available for Stellar. |
| [SubQuery](https://subquery.network/), hosted by [OnFinality](https://onfinality.io/) | Indexer SDK (TypeScript) and hosting | |
| [The Graph](https://thegraph.com/docs/en/about/) | Substreams | Subgraphs and Token API are not available for Stellar. |
| [Mercury](https://docs.mercurydata.app/) | Hosted indexer with GraphQL | Retroshades indexes Soroban contract events. Mercury Classic indexes transactions and contract events. |

### Coverage

| Coverage | Meaning |
|---|---|
| Covered | An alternative serves the use case. No gap remains. |
| Partial | Alternatives serve part of the use case. The remaining gap is listed. |
| None | No alternative serves the use case. The gap is the full use case. |

### 3. Account state

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 3.3 | List all balances of an account | High | Covered | wallet-backend: `Account.balances`. Alchemy: balances. | |
| 3.5 | List all data entries of an account | High | Partial | wallet-backend: `Account.stateChanges` with category `DATA_ENTRY` returns data entry history. | Current data entries of an account. |
| 3.7 | Watch an account for changes | High | Partial | wallet-backend: poll `Account.stateChanges` from the last cursor. Covers balances, trustlines, signers, thresholds, flags, home domain, and data entries. | Push delivery (streaming). Changes to offers, sponsorships, and sequence numbers. |

### 4. Account discovery (reverse lookups)

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 4.1 | Find accounts by signer | High | None | | Full use case. Key recovery and multisig tools depend on it. |
| 4.2 | Find accounts that hold an asset | High | Covered | StellarExpert API: asset holders | |
| 4.3 | Find accounts by sponsor | High | None | | Full use case. |
| 4.4 | Find accounts that hold liquidity pool shares | High | None | | Full use case. |

### 5. Transaction and operation history

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 5.3 | Get an operation by ID | Medium | Covered | wallet-backend: `operationById` | |
| 5.5 | Transaction history for an account | High | Partial | wallet-backend: `Account.transactions` | Memo and envelope XDR in the transaction. The client must read them from RPC `getTransaction`. |
| 5.6 | Operation history for an account | High | Covered | wallet-backend: `Account.operations` | |
| 5.7 | History for a claimable balance or liquidity pool | High | None | | Full use case. |
| 5.8 | Stream new transactions or operations | Medium | Covered | Goldsky: `stellar_mainnet.transactions` dataset. OBSRVR Flow. | |

### 6. Payments and balance changes

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 6.1 | Payment history for an account | Medium | Covered | wallet-backend: `Account.stateChanges` with category `BALANCE`. Covers classic assets, SAC, and SEP-41 tokens. Alchemy: transfer history. | |
| 6.2 | Detect incoming payments (deposit monitoring) | Medium | Partial | wallet-backend: poll `Account.stateChanges` with category `BALANCE` and reason `CREDIT`. `toMuxedId` holds the muxed ID or `MEMO_ID`. Alchemy: poll transfer history. | Push delivery (streaming). Text and hash memos. |
| 6.3 | Global payment feed | Medium | Covered | OBSRVR Flow | |
| 6.4 | Effects | High | Partial | wallet-backend: state changes (see 3.7). | Effects for offers, trades, claimable balances, sponsorships, liquidity pool deposits and withdrawals, and sequence number bumps. |

### 7. Ledgers

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 7.3 | Payments, operations, or effects in a ledger | Medium | None | | Full use case. The client derives the data from RPC `getTransactions` and `getEvents`. |

### 8. Claimable balances

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 8.2 | Find claimable balances by claimant, sponsor, or asset | High | Partial | StellarExpert API: claimable balances by account | Lookup by sponsor or by asset. |

### 9. Liquidity pools

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 9.2 | Find pools by reserve assets or by participant account | High | Partial | By account: wallet-backend `Account.balances` (pool shares). By asset pair: compute the pool ID, then RPC `getLedgerEntries`. | List all pools that contain one asset. |

### 10. DEX: offers, order books, trades

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 10.2 | List offers of an account | High | None | | Full use case. |
| 10.3 | Find offers by asset or sponsor | High | None | | Full use case. |
| 10.4 | Get the order book for an asset pair | High | None | | Full use case. |
| 10.5 | Trade history | High | Covered | StellarExpert API | |
| 10.6 | Trade aggregations | High | Covered | StellarExpert API | |

### 11. Path finding

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 11.1 | Find payment paths | High | None | | Full use case. Wallets and anchors use path payments. |

### 12. Assets

| ID | Use case | RPC effort | Coverage | Alternatives | Remaining gap |
|---|---|---|---|---|---|
| 12.1 | Asset statistics | High | Covered | StellarExpert API | |
| 12.2 | Search assets by code or issuer | High | Covered | StellarExpert API | |

### Gap summary

| Coverage | Use cases |
|---|---|
| None | 4.1, 4.3, 4.4 (reverse lookups by signer, sponsor, pool share). 5.7 (claimable balance and pool history). 7.3 (ledger-scoped operations and payments). 10.2–10.4 (offers and order books). 11.1 (path finding). |
| Partial | 3.5 (current data entries). 3.7, 6.2 (push delivery). 5.5, 6.2 (memos and envelope XDR). 6.4 (effects outside state changes). 8.2 (claimable balances by sponsor or asset). 9.2 (pools by single asset). |
| Covered | 3.3, 4.2, 5.3, 5.6, 5.8, 6.1, 6.3, 10.5, 10.6, 12.1, 12.2 |
