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
| 4.1 | Find accounts by signer | `GET /accounts?signer=` | None | High | |
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

## Alternatives

This section describes ready-to-use alternatives for use cases with a medium or high RPC rating. It does not rate migration effort or propose solutions. Operation and configuration use cases (section 13) are out of scope.

Each alternative lists:

- **Use cases:** the IDs it supports, fully or partially.
- **Capabilities:** what it does well.
- **Limited or no coverage:** areas where developers must use another solution.

Indexing platforms and pipeline frameworks (for example, SubQuery, The Graph Substreams, Mercury, Galexie, the Ingest SDK) are tools to build an indexer. They are not listed as alternatives.

### wallet-backend

[Repository](https://github.com/stellar/wallet-backend). Indexer with a GraphQL API. Self-hosted, or hosted by infrastructure providers.

**Use cases**

| Support | IDs |
|---|---|
| Full | 3.3, 5.3, 5.6, 6.1 |
| Partial | 3.5 (history only), 3.7 (polling only), 5.5 (no memo or envelope), 6.2 (polling, `MEMO_ID` only), 6.4 (subset of effects), 9.2 (by account) |

**Capabilities**

- Per-account history: transactions, operations, and state changes (balances, trustlines, signers, thresholds, flags, home domain, data entries, SEP-41 allowances).
- Current balances for accounts and contracts: native, trustlines, SAC, SEP-41, and liquidity pool shares.
- Lookups by account, transaction hash, and operation ID.

**Limited or no coverage**

- Network-wide queries and reverse lookups (by asset, signer, sponsor, ledger).
- DEX and market data.
- Claimable balances.
- Asset-level data.
- Push delivery.
- Transaction memos and XDR.

### Alchemy

[Data API](https://www.alchemy.com/docs/reference/stellar-data-api-overview), [Address Activity webhook](https://www.alchemy.com/docs/reference/stellar-address-activity-webhook), [WebSocket subscriptions](https://www.alchemy.com/docs/reference/stellar-websocket-subscriptions). Hosted. Also provides Stellar RPC.

**Use cases**

| Support | IDs |
|---|---|
| Full | 5.8, 6.1, 6.2, 6.3 |
| Partial | 3.3 (no liquidity pool shares), 3.7 (value movements only), 6.4 (value movements only), 10.5 (live fills for tracked accounts only) |

**Capabilities**

- Per-address transfer history and balances across native, classic, and Soroban tokens.
- Push delivery: webhooks for value movements of tracked accounts (payments, DEX fills, claimable balances, liquidity pools, Soroban transfers), and WebSocket streams of ledgers, events, transactions (with envelopes), and payments.

**Limited or no coverage**

- Account state other than balances.
- Operation and transaction history queries.
- Reverse lookups.
- DEX offers, order books, and market data.
- Claimable balances and liquidity pools as objects.
- Asset-level data.
- Memos in transfer and webhook records. Memos are available from the WebSocket transaction envelope.

### Stellar Indexer

[Website](https://stellarindexer.com/), [API reference](https://api.stellarindexer.com/openapi), [TypeScript SDK](https://github.com/Creit-Tech/Stellar-Indexer-SDK). Hosted, by Creit Tech. Closed beta.

**Use cases**

| Support | IDs |
|---|---|
| Full | 3.3, 3.5, 9.2, 10.2 |
| Partial | 10.3 (by asset pair only), 10.4 (no push delivery) |

**Capabilities**

- Current classic state: accounts, trustlines, data entries, offers of an account, offers and market depth for an asset pair, and liquidity pools by asset.
- Soroban contract data, and endpoints for specific protocols (for example, Blend).

**Limited or no coverage**

- History: transactions, operations, payments, effects, and trades.
- Push delivery.
- Reverse lookups.
- Asset-level data.

### StellarExpert

[API reference](https://stellar.expert/openapi.html) (OpenAPI 3.1, index at `https://api.stellar.expert/api-docs/index`). Hosted explorer API with paid plans.

**Use cases**

| Support | IDs |
|---|---|
| Full | 3.3, 4.2, 4.4, 5.5, 9.2, 10.5, 10.6, 12.1 |
| Partial | 5.6 (transaction records, not operation records), 5.7 (liquidity pools only), 6.1 (payments as transactions), 6.2 (polling), 7.3 (raw XDR), 8.2 (by claimant only), 12.2 (ranked list, no search by code) |

**Capabilities**

- Network-wide transaction history with filters: address, source, destination, asset, operation type, offer, liquidity pool, memo, and time range. Records include envelope, result, and meta XDR.
- Asset data: statistics, supply, holders, distribution, ratings, prices, and candles.
- Market data: trades by account, asset, liquidity pool, and offer, and candles by market pair.
- Liquidity pools by asset, with share holders.
- Claimable balances by claimant.

**Limited or no coverage**

- Push delivery. Only a long-poll stream of new ledger sequences exists.
- Open offers and order books.
- Account changes outside value movements (effects).
- Reverse lookups by signer or sponsor.

### OBSRVR

[Documentation](https://docs.withobsrvr.com/): Lake (query API), Flow (managed pipelines), Gateway (hosted Horizon and RPC). Hosted, with paid plans.

**Use cases**

| Support | IDs |
|---|---|
| Full | 3.3, 4.2, 5.8, 6.1, 6.2, 6.3 |
| Partial | 5.5, 5.6 (activity feeds; filters not documented), 12.1 (statistics per asset), 12.2 (general search) |

**Capabilities**

- Lake: account balances and activity, transfers by account or asset across classic and SAC, payments, asset holders and statistics, contract events, decoded transactions, and search. Recent data is served from hot storage, full history from cold storage.
- Flow: prebuilt processors (for example, payments with memo, raw transactions, contract events) that deliver to a webhook, database, or queue.
- Gateway and dedicated nodes host Horizon. This depends on the Horizon codebase, so it is not counted as coverage.

**Limited or no coverage**

- DEX offers, order books, and trades.
- Claimable balances and liquidity pools.
- Reverse lookups by signer or sponsor.
- Effects.

The Lake API reference lists endpoints but not all parameters. Coverage for history queries is partial until filters are documented.

### Goldsky

[Stellar overview](https://docs.goldsky.com/chains/stellar), [Stellar datasets](https://docs.goldsky.com/turbo-pipelines/sources/stellar). Hosted streaming pipelines (Turbo) with prebuilt datasets.

**Use cases**

| Support | IDs |
|---|---|
| Full | 5.8, 6.2, 6.3 |
| Partial | 3.7 (balance changes only) |

**Capabilities**

- Prebuilt datasets streamed with less than 5 seconds of latency, with full history: ledgers, transactions (with memos), operations, events, transfers, ledger entry changes, and balances.
- SQL filters in the pipeline, delivered to the client's database or another sink.

**Limited or no coverage**

- No query API. Data goes to the client's own storage. History and state use cases need the client to store and query the data.

### Use cases without full coverage

| ID | Use case | Partial coverage | Remaining gap |
|---|---|---|---|
| 3.7 | Watch an account for changes | wallet-backend, Alchemy, Goldsky | Push delivery of changes other than value movements (signers, thresholds, flags, trustlines, data entries, sponsorships). |
| 4.1 | Find accounts by signer | | Full use case. |
| 4.3 | Find accounts by sponsor | | Full use case. |
| 5.7 | History for a claimable balance or liquidity pool | StellarExpert (liquidity pools) | Claimable balance history. |
| 6.4 | Effects | wallet-backend, Alchemy | Effects for sponsorships, offer creation, update, and removal, and sequence number bumps. |
| 7.3 | Payments, operations, or effects in a ledger | StellarExpert (raw XDR) | Decoded operations, payments, and effects per ledger. |
| 8.2 | Find claimable balances by claimant, sponsor, or asset | StellarExpert (by claimant) | Lookup by sponsor or asset. |
| 10.3 | Find offers by asset or sponsor | Stellar Indexer (by asset pair) | Lookup by sponsor, or by one asset. |
| 10.4 | Get the order book for an asset pair | Stellar Indexer | Push delivery of order book updates. |
| 11.1 | Find payment paths | | Full use case. |
