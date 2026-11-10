# GraphQL schema reference

Every query, type, and field served at `POST /graphql/query`. For auth, pagination, and examples, see [GraphQL API](graphql.md). Generated from the schema files by `make gql-docs`; do not edit by hand.

## Contents

- [Queries](#queries)
- [Objects](#objects)
- [Interfaces](#interfaces)
- [Enums](#enums)
- [Input objects](#input-objects)
- [Scalars](#scalars)

## Queries

Root queries. All lookups are read-only.

### transactionByHash

Look up a transaction by hash. Returns null, with an error in the response, when no indexed transaction has this hash.

Returns: [Transaction](#transaction)

| Name | Type | Default | Description |
|---|---|---|---|
| `hash` | `String`! |  | Transaction hash: 64 hex characters. For a fee-bump transaction, the outer (fee-bump) hash. Any other format returns an INVALID_TRANSACTION_HASH error. |

### accountByAddress

Look up an account (G...) or contract (C...) by address. A contract's history holds the operations it authorised and the state changes applied to it. Any valid address returns an Account, even one with no indexed activity; its connections are then empty.

Returns: [Account](#account)

| Name | Type | Default | Description |
|---|---|---|---|
| `address` | `String`! |  | Account strkey (G...) or contract strkey (C...). Any other value, including a muxed address (M...), returns an INVALID_ADDRESS error. |

### operationById

Look up an operation by its ID (TOID). Returns null, with an error in the response, when no indexed operation has this ID.

Returns: [Operation](#operation)

| Name | Type | Default | Description |
|---|---|---|---|
| `id` | [Int64](#int64)! |  | Operation ID (TOID). |

### blendPools

Blend v2 pool-wide catalog, independent of any account, keyset-paginated by pool contract address. Pool deployment is permissionless, so the catalog is unbounded; pages are capped at 50 pools (also the default when first/last is omitted).

Returns: [BlendPoolConnection](#blendpoolconnection)!

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  |  |
| `after` | `String` |  |  |
| `last` | `Int` |  |  |
| `before` | `String` |  |  |

### blendPool

Look up one Blend v2 pool by its contract address (C...).

Returns: [BlendPool](#blendpool)

| Name | Type | Default | Description |
|---|---|---|---|
| `address` | `String`! |  |  |

## Objects

### Account

A Stellar account or contract address whose activity is indexed.

| Field | Type | Description |
|---|---|---|
| `address` | `String`! | The account's public key (G...) or contract address (C...). |
| `balances` | [BalanceConnection](#balanceconnection)! | All token balances held by this account: native XLM, classic trustlines, SAC, SEP-41, and liquidity-pool shares. Edges come in a fixed order by token type: native, trustlines, SEP-41, then liquidity pools for a G-address; SAC, then SEP-41 for a C-address. |
| `transactions` | [AccountTransactionConnection](#accounttransactionconnection)! | Transactions this account participated in, oldest first. Set since/until to bound the ledger close time; bounded queries on long histories run faster. |
| `operations` | [OperationConnection](#operationconnection)! | Operations this account participated in, oldest first. Set since/until to bound the ledger close time; bounded queries on long histories run faster. |
| `stateChanges` | [StateChangeConnection](#statechangeconnection)! | State changes affecting this account, oldest first, optionally filtered. Set since/until to bound the ledger close time; bounded queries on long histories run faster. |
| `sep41Allowances` | [SEP41AllowanceConnection](#sep41allowanceconnection)! | Active SEP-41 allowances granted by this account (as token holder), ordered by spender. Allowances whose expiration ledger is below the latest ingested ledger are filtered out server-side. |
| `blendPositions` | [BlendAccountPositions](#blendaccountpositions)! | An account's Blend v2 lending, collateral, borrowing, and backstop positions. |

Arguments of `balances`:

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

Arguments of `transactions`:

| Name | Type | Default | Description |
|---|---|---|---|
| `since` | [Time](#time) |  | Only include items whose ledger close time is at or after this time. |
| `until` | [Time](#time) |  | Only include items whose ledger close time is at or before this time. Must not be earlier than `since`. |
| `memo` | [MemoFilter](#memofilter) |  | Only include transactions whose memo matches exactly. |
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

Arguments of `operations`:

| Name | Type | Default | Description |
|---|---|---|---|
| `since` | [Time](#time) |  | Only include items whose ledger close time is at or after this time. |
| `until` | [Time](#time) |  | Only include items whose ledger close time is at or before this time. Must not be earlier than `since`. |
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

Arguments of `stateChanges`:

| Name | Type | Default | Description |
|---|---|---|---|
| `filter` | [AccountStateChangeFilterInput](#accountstatechangefilterinput) |  | Conditions the state changes must match; all are ANDed. Omit for no filtering. |
| `since` | [Time](#time) |  | Only include items whose ledger close time is at or after this time. |
| `until` | [Time](#time) |  | Only include items whose ledger close time is at or before this time. Must not be earlier than `since`. |
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

Arguments of `sep41Allowances`:

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

### AccountCreatedChange

An account came into existence. `account` is the new account: a G-address for a classic account creation, a C-address for a smart-contract deployment. Pair: (ACCOUNT, CREATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `creatorAddress` | `String`! | Account that created this one: the funder of a classic account's starting balance, or the deployer of a contract. |

### AccountFlagsChange

Account authorization flags set or cleared in one operation. Pairs: (FLAGS, SET) lists flags that were turned on, (FLAGS, CLEAR) lists flags that were turned off.

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `flags` | \[[AccountFlag](#accountflag)!\]! | Flags that were set (reason SET) or cleared (reason CLEAR). |

### AccountMergedChange

An account merge. `account` is the merged (removed) account. Pair: (ACCOUNT, MERGE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `destinationAddress` | `String`! | Account (G...) that received the merged account's XLM balance. |

### AccountTransactionConnection

Relay-style page of an account's transactions.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[AccountTransactionEdge](#accounttransactionedge)!\]! | Transactions in this page, in ledger order (oldest first). |
| `pageInfo` | [PageInfo](#pageinfo)! | Cursors and page flags for fetching adjacent pages. |

### AccountTransactionEdge

One transaction in an account's history, with the transaction's operations and state changes inlined so a full account-history page resolves in one query.

| Field | Type | Description |
|---|---|---|
| `node` | [Transaction](#transaction)! | The transaction. |
| `cursor` | `String`! | Opaque cursor for this edge. Pass it as `after` or `before`. |
| `operations` | \[[Operation](#operation)!\]! | This transaction's operations that the account participated in. Not paginated. |
| `stateChanges` | \[[BaseStateChange](#basestatechange)!\]! | This transaction's state changes that affected the account, newest first. Not paginated: at most 2,000 rows, and a transaction with more drops its oldest. |

### AllowanceChange

A SEP-41 allowance approval: `account` (the token holder) authorized `spender` to transfer up to `amount` of the token on its behalf. Pair: (ALLOWANCE, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String`! | Contract ID (C...) of the token the allowance applies to. |
| `spender` | `String`! | Address (G... or C...) authorized to spend from the holder's balance. |
| `amount` | `String`! | Approved allowance, as an integer string in the token's smallest unit. |
| `expirationLedger` | [UInt32](#uint32)! | Last ledger sequence at which the allowance is live. |

### BalanceAuthorizationChange

Authorization to hold or transact an asset granted or revoked for the account. Exactly one of tokenId / liquidityPoolId is set. For classic trustlines, `flags` lists the trustline flags that were set (reason SET) or cleared (reason CLEAR). For SAC authorization of contract holders, authorization is a plain boolean in the contract balance entry, so `flags` is null. Pairs: (BALANCE_AUTHORIZATION, SET), (BALANCE_AUTHORIZATION, CLEAR).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String` | Stellar Asset Contract ID (C...) of the asset; null for liquidity-pool-share trustlines. |
| `liquidityPoolId` | `String` | Hex-encoded liquidity pool ID for pool-share trustlines; null for asset trustlines. |
| `flags` | \[[TrustlineFlag](#trustlineflag)!\] | Trustline flags that changed; null for SAC contract-holder authorization, which has no flags. |

### BalanceChange

A movement of value on the account's token balance. Covers operation-sourced movements and the per-transaction net fee charged to the fee-paying account. Pairs: (BALANCE, DEBIT), (BALANCE, CREDIT), (BALANCE, MINT), (BALANCE, BURN). Clawbacks are recorded as BURN. Transaction-fee rows are (BALANCE, DEBIT) with `operation` null (fees are charged per transaction, not per operation) and `toMuxedId` null; refunds are netted into the fee charge, never a separate row.

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation) | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String`! | Contract ID (C...) of the token whose balance moved. For XLM and classic assets, the Stellar Asset Contract ID. |
| `amount` | `String`! | Amount moved, as an integer string in the token's smallest unit (stroops for XLM and classic assets). |
| `toMuxedId` | `String` | CAP-67 destination id on the recipient's row: the id of a muxed destination (M... account, or W... contract via the CAP-84 to_muxed_id event field) or, for a classic operation that credits an account (payment, path payment, create account, account merge, claimable balance claim, liquidity pool withdraw), the transaction's MEMO_ID. Set on the CREDIT row of SAC and classic transfers and mints, and on the CREDIT/MINT row of SEP-41 transfer/mint events. Null on debits, issuer MINT rows, fee rows and for text/hash memos. Recorded from the release that added each source onward; rows ingested earlier stay null. Rendered as a decimal string so u64 values above 2^53-1 survive JSON number quantization. |

### BalanceConnection

Relay-style page of an account's token balances.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[BalanceEdge](#balanceedge)!\]! | Balances in this page, in the token-type order described on `Account.balances`. |
| `pageInfo` | [PageInfo](#pageinfo)! | Cursors and page flags for fetching adjacent pages. |

### BalanceEdge

One balance in a page, with its pagination cursor.

| Field | Type | Description |
|---|---|---|
| `node` | [Balance](#balance)! | The balance. Select type-specific fields with inline fragments. |
| `cursor` | `String`! | Opaque cursor for this edge. Pass it as `after` or `before`. |

### BlendAccountPositions

BlendAccountPositions aggregates one account's Blend v2 exposure across every pool it has touched. backstopClaimed lists lifetime backstop-emission claims per backstop. Unlike claimedBlnd (per pool), the on-chain backstop claim event carries no pool address, so these totals are reported per backstop, not per pool.

| Field | Type | Description |
|---|---|---|
| `pools` | \[[BlendPoolPosition](#blendpoolposition)!\]! |  |
| `backstop` | \[[BlendBackstopPosition](#blendbackstopposition)!\]! |  |
| `backstopClaimed` | \[[BlendBackstopClaimed](#blendbackstopclaimed)!\]! |  |
| `activeAuctions` | \[[BlendAuction](#blendauction)!\]! | Active Dutch auctions where this account is the auction owner: being liquidated (USER_LIQUIDATION), or — only when this account IS the backstop address — carrying bad debt (BAD_DEBT) or settling interest (INTEREST). Sorted by (poolAddress, auctionType). |

### BlendAuction

BlendAuction is one active Dutch auction on a Blend v2 pool. Amounts in bid and lot are raw protocol-token i128 decimal strings (not USD), at the scale noted per field below.

| Field | Type | Description |
|---|---|---|
| `poolAddress` | `String`! |  |
| `poolName` | `String` |  |
| `auctionType` | [BlendAuctionType](#blendauctiontype)! |  |
| `bid` | \[[BlendAuctionAmount](#blendauctionamount)!\]! | Assets the filler pays. Units by type: USER_LIQUIDATION/BAD_DEBT dTokens; INTEREST backstop LP tokens. |
| `lot` | \[[BlendAuctionAmount](#blendauctionamount)!\]! | Assets the filler receives. Units by type: USER_LIQUIDATION bTokens; BAD_DEBT backstop LP tokens; INTEREST underlying. |
| `startBlock` | `Int`! | Ledger the auction started at — anchors the Dutch-auction lot/bid scaling (0-200 lot ramps up, 200-400 bid ramps down). |

### BlendAuctionAmount

BlendAuctionAmount is one asset's raw protocol-token amount within an auction's bid or lot.

| Field | Type | Description |
|---|---|---|
| `assetContractId` | `String`! |  |
| `amount` | `String`! | Raw on-chain integer amount at the asset's native decimals, NOT a USD value. |

### BlendAuctionChange

One side of a filled Blend v2 Dutch auction. Every fill produces two rows — one for the account being liquidated (or the pool, for BAD_DEBT/INTEREST auctions) and one for the filler — each naming the other as `counterparty`. Pair: (BLEND_AUCTION, FILL).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `poolId` | `String`! | Contract ID of the Blend pool the auction ran on. |
| `auctionType` | [BlendAuctionType](#blendauctiontype)! | Which auction was filled. |
| `fillPercent` | `Int`! | Percentage of the auction filled by this fill, 1-100. |
| `counterparty` | `String`! | The other account in the fill: the filler on the liquidated side, and vice versa. |
| `lot` | \[[BlendAuctionAmount](#blendauctionamount)!\]! | Assets the filler received. Units by type: USER_LIQUIDATION bTokens; BAD_DEBT backstop LP tokens; INTEREST underlying. |
| `bid` | \[[BlendAuctionAmount](#blendauctionamount)!\]! | Assets the filler assumed. Units by type: USER_LIQUIDATION/BAD_DEBT dTokens; INTEREST backstop LP tokens. |

### BlendBackstopChange

A change to the account's Blend v2 backstop deposit for one pool, denominated in Comet BLND:USDC LP tokens. Pairs: (BLEND_BACKSTOP, CREDIT), (BLEND_BACKSTOP, DEBIT).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `amount` | `String`! | Comet LP tokens deposited or withdrawn, as a decimal string. |
| `poolId` | `String`! | Contract ID of the Blend pool whose backstop the deposit backs. |

### BlendBackstopClaimed

BlendBackstopClaimed is an account's lifetime backstop-emission claims from one backstop, in that backstop's Comet LP tokens (7 decimals): the backstop converts claimed BLND into its LP token and re-deposits it, so LP tokens are what the claimer receives. Each backstop has its own LP token, so totals from different versions are never summed.

| Field | Type | Description |
|---|---|---|
| `version` | [BlendVersion](#blendversion)! |  |
| `lpTokens` | `String`! |  |

### BlendBackstopEmissionsClaimChange

Backstop emissions claimed from the Blend v2 backstop. The claimed BLND is swapped into Comet LP tokens and auto-restaked (the restake itself emits BLEND_BACKSTOP CREDIT rows per pool), so the amount is denominated in LP tokens and no single pool or token is attributable to the claim itself. Pair: (BLEND_BACKSTOP_EMISSIONS, CLAIM).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `amount` | `String`! | Comet BLND:USDC LP tokens minted by the claim, as a decimal string. |

### BlendBackstopPosition

BlendBackstopPosition is an account's backstop deposit in one pool. The backstop is a single protocol-wide contract holding each pool's first-loss capital separately, so exposure is inherently per-pool (poolAddress) even though the backstop is not the pool contract itself — one BlendBackstopPosition per backed pool. shares is the ACTIVE (non-queued) backstop share balance; lpTokens/usdValue value the whole deposit — active plus queued-for-withdrawal shares — since queued shares keep earning pool interest and remain slashable first-loss capital until actually withdrawn. emissionsEarnedBlnd is CLAIMABLE (uncollected) BLND accrued on the backstop emission stream for this pool; it accrues on active shares only (queued shares earn no emissions).

| Field | Type | Description |
|---|---|---|
| `poolAddress` | `String`! |  |
| `poolName` | `String` |  |
| `version` | [BlendVersion](#blendversion) | The Blend deployment of the pool's backstop. Null when that backstop is not one the backend indexes. |
| `shares` | `String`! |  |
| `lpTokens` | `String` | The deposit converted to Comet LP tokens at the pool's shares:tokens rate. Null when that rate is unknown: the pool's blend_backstop_pools balance can be absent or zeroed (an emissions-only write creates the row with zero balance columns) while this account provably holds shares — a conversion reported as "0" there would be indistinguishable from a closed position. |
| `usdValue` | `Float` |  |
| `q4w` | \[[BlendQ4W](#blendq4w)!\]! |  |
| `emissionsEarnedBlnd` | `String`! |  |
| `emissionsEarnedUsd` | `Float` |  |

### BlendBackstopQueueChange

A queue-for-withdrawal entry added to or removed from the account's Blend v2 backstop position for one pool. ADD queues shares for the 17-day withdrawal lock; REMOVE cancels a queued entry (an executed withdrawal is a BLEND_BACKSTOP DEBIT instead). Pairs: (BLEND_BACKSTOP_QUEUE, ADD), (BLEND_BACKSTOP_QUEUE, REMOVE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `amount` | `String`! | Backstop shares queued or dequeued, as a decimal string. |
| `poolId` | `String`! | Contract ID of the Blend pool whose backstop the queue entry belongs to. |

### BlendCollateralChange

A change to the account's collateralized supply position in a Blend v2 pool reserve: underlying tokens posted as (or released from) borrow collateral. Pairs: (BLEND_COLLATERAL, CREDIT), (BLEND_COLLATERAL, DEBIT).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `tokenId` | `String`! | Contract ID of the reserve asset posted or released. |
| `amount` | `String`! | Underlying amount moved, as a decimal string in the asset's smallest unit. |
| `poolId` | `String`! | Contract ID of the Blend pool the reserve belongs to. |

### BlendDebtChange

A change to the account's debt position in a Blend v2 pool reserve. Pairs: (BLEND_DEBT, BORROW), (BLEND_DEBT, REPAY), (BLEND_DEBT, FLASH_LOAN), (BLEND_DEBT, BAD_DEBT), (BLEND_DEBT, BURN). Amount denomination varies by reason: BORROW, REPAY, and FLASH_LOAN carry the underlying amount; BAD_DEBT (debt socialized to the backstop) and BURN (defaulted debt written off; `account` is the pool) carry dTokens.

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `tokenId` | `String`! | Contract ID of the reserve asset borrowed against. |
| `amount` | `String`! | Amount moved, as a decimal string (see the type docs for denomination by reason). |
| `poolId` | `String`! | Contract ID of the Blend pool the debt is owed to. |

### BlendEmissionsClaimChange

BLND emissions claimed from a Blend v2 pool's reserves. Pair: (BLEND_EMISSIONS, CLAIM).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `tokenId` | `String` | Contract ID of the BLND Stellar Asset Contract the emissions pay out in. Null only on networks with no known BLND SAC (e.g. a standalone test network). |
| `amount` | `String`! | BLND claimed, as a decimal string in stroops. |
| `poolId` | `String`! | Contract ID of the Blend pool whose reserve emissions were claimed. |

### BlendPool

BlendPool is a pool-wide catalog view of one Blend v2 pool, independent of any account. suppliedUsd/borrowedUsd are the sum of each reserve's own suppliedUsd/borrowedUsd (nil if any reserve's is nil — a missing price makes the pool-wide total genuinely uncomputable, not just smaller). backstopUsd is the pool's backstop-LP token balance priced at the Comet LP rate. interestApy and netApy are both supplied-USD-weighted means of each reserve's supplyApy across reserves; netApy additionally folds in each reserve's emissionsSupplyApr — i.e. it is the supply-side yield including BLND emissions, not netted against the pool's borrow side. Both are nil if any priced reserve's supplyApy is nil, since weighting that reserve in at 0% yield would report a number that looks real and isn't.

| Field | Type | Description |
|---|---|---|
| `address` | `String`! |  |
| `name` | `String` |  |
| `version` | [BlendVersion](#blendversion) | The Blend deployment of the pool's backstop. Null when that backstop is not one the backend indexes. |
| `status` | [BlendPoolStatus](#blendpoolstatus) | Pool status. Statuses ADMIN_ACTIVE/ACTIVE/ADMIN_ON_ICE/ON_ICE accept supply (deposits); ADMIN_ACTIVE/ACTIVE also allow borrowing; ADMIN_FROZEN/FROZEN/ SETUP reject both. Null until the pool's config entry has been ingested, and also null for an unrecognized on-chain status value. |
| `oracleContractId` | `String` |  |
| `backstopRate` | `Int` | Share of borrower interest routed to the pool's backstop, as 7-decimal fixed point (4750000 = 47.5%). |
| `maxPositions` | `Int` |  |
| `suppliedUsd` | `Float` |  |
| `borrowedUsd` | `Float` |  |
| `backstopUsd` | `Float` |  |
| `interestApy` | `Float` |  |
| `netApy` | `Float` |  |
| `reserves` | \[[BlendReserve](#blendreserve)!\]! |  |
| `admin` | `String` | Pool admin address (G... or C...). Distinguishes owned pools (admin can retune parameters) from standard pools whose admin is disabled. Null when not yet observed. |
| `inRewardZone` | `Boolean`! | Whether this pool is in the backstop's reward zone and therefore receives BLND emissions. |

### BlendPoolConnection

Relay-style page of the Blend v2 pool catalog.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[BlendPoolEdge](#blendpooledge)!\]! |  |
| `pageInfo` | [PageInfo](#pageinfo)! |  |

### BlendPoolEdge

One Blend v2 pool in a page, with its pagination cursor.

| Field | Type | Description |
|---|---|---|
| `node` | [BlendPool](#blendpool)! |  |
| `cursor` | `String`! |  |

### BlendPoolPosition

BlendPoolPosition rolls up an account's reserve positions within one pool.

| Field | Type | Description |
|---|---|---|
| `poolAddress` | `String`! |  |
| `poolName` | `String` |  |
| `usdValue` | `Float` |  |
| `suppliedUsd` | `Float` |  |
| `borrowedUsd` | `Float` |  |
| `netApy` | `Float` | Supply-vs-borrow interest netting over TOTAL SUPPLIED USD — the blend-sdk-js PositionsEstimate convention shown by the Blend UI: (Σ suppliedUsd·supplyApy − Σ borrowedUsd·borrowApy) / Σ suppliedUsd. 0 for a position with debt but no supply (bad debt is forgiven). Null when any contributing reserve is missing a fresh oracle price, or has a null supplyApy/borrowApy of its own. |
| `claimedBlnd` | `String`! | Lifetime BLND this account has claimed from this pool's reserve emissions. |
| `reserves` | \[[BlendReservePosition](#blendreserveposition)!\]! |  |

### BlendQ4W

BlendQ4W is one queued backstop withdrawal, unlocking at expiration (unix seconds). amount is in backstop shares; lpTokens/usdValue value those shares through the same shares→LP→USD conversion as the position's totals, and go null under the same unknown-rate condition as the position's lpTokens.

| Field | Type | Description |
|---|---|---|
| `amount` | `String`! |  |
| `expiration` | [Int64](#int64)! |  |
| `lpTokens` | `String` |  |
| `usdValue` | `Float` |  |

### BlendReserve

BlendReserve is a pool-wide reserve catalog view: current utilization, supply/borrow APY, emissions APR, and pool-wide (not per-account) underlying token amounts, all as of "now" (rates are projected forward from the reserve's last on-chain update).

| Field | Type | Description |
|---|---|---|
| `assetContractId` | `String`! |  |
| `tokenName` | `String` |  |
| `tokenSymbol` | `String` |  |
| `tokenDecimals` | `Int` |  |
| `enabled` | `Boolean`! |  |
| `utilization` | `Float` |  |
| `supplyApy` | `Float` | Compounded interest rate for each side. Null when the reserve's rate curve compounds past what a Float can hold — reserve config is permissionless, so an arbitrarily steep curve is representable on-chain but not here. |
| `borrowApy` | `Float` |  |
| `emissionsSupplyApr` | `Float` |  |
| `emissionsBorrowApr` | `Float` |  |
| `suppliedTokens` | `String`! |  |
| `borrowedTokens` | `String`! |  |
| `suppliedUsd` | `Float` |  |
| `borrowedUsd` | `Float` |  |
| `cFactor` | `Int` | Collateral factor as 7-decimal fixed point (9000000 = 0.9): the fraction of this reserve's value usable as collateral. |
| `lFactor` | `Int` | Liability factor as 7-decimal fixed point: scales how much borrowing this reserve's value supports. |
| `priceUsd` | `Float` |  |

### BlendReservePosition

BlendReservePosition is an account's position in one reserve of a pool. suppliedTokens/collateralTokens/borrowedTokens are underlying-asset amounts at projected (as-of-now) rates. interestEarned/interestPaid are underlying-asset amounts too. emissionsEarnedBlnd is CLAIMABLE (uncollected) BLND accrued across the reserve's b/d emission streams; claimed history is tracked per pool only (see BlendPoolPosition.claimedBlnd), not per reserve.

| Field | Type | Description |
|---|---|---|
| `assetContractId` | `String`! |  |
| `tokenName` | `String` |  |
| `tokenSymbol` | `String` |  |
| `tokenDecimals` | `Int` |  |
| `suppliedTokens` | `String`! |  |
| `collateralTokens` | `String`! |  |
| `borrowedTokens` | `String`! |  |
| `suppliedUsd` | `Float` |  |
| `borrowedUsd` | `Float` |  |
| `supplyApy` | `Float` | Compounded interest rate for each side. Null when the reserve's rate curve compounds past what a Float can hold — reserve config is permissionless, so an arbitrarily steep curve is representable on-chain but not here. |
| `borrowApy` | `Float` |  |
| `emissionsSupplyApr` | `Float` | The reserve's POOL-WIDE bToken (supply) emission-stream APR: annualized BLND value over the side's pool-wide supplied USD, NOT scaled to this account's holding. 0 when no active stream (unconfigured or expired), null when the stream is active but the reserve or BLND price is unavailable. |
| `emissionsBorrowApr` | `Float` | The reserve's POOL-WIDE dToken (borrow) emission-stream APR: annualized BLND value over the side's pool-wide borrowed USD, NOT scaled to this account's holding. 0 when no active stream (unconfigured or expired), null when the stream is active but the reserve or BLND price is unavailable. |
| `interestEarned` | `String`! | Lifetime interest earned on the supply side of this reserve: the current underlying value of the account's supply+collateral bTokens (at projected, as-of-now rates) minus the net principal it contributed — a cost basis tracked across the account's whole deposit/withdraw history. Token-denominated: a raw integer at the reserve asset's decimals (tokenDecimals), so multiply by priceUsd for a USD value. A liquidation reduces the cost basis by the seized collateral's underlying value at the reserve's rate when the fill is processed, so collateral lost to a fill cancels out instead of being reported as earned interest. Survives a full exit: a position zeroed to no tokens still reports the earnings realized up to the exit (current value 0 minus the negative leftover cost basis). May be slightly negative from cost-basis dust truncation or bRate movement after an exit. |
| `interestPaid` | `String`! | Lifetime interest paid on the debt side of this reserve, mirroring interestEarned: the current underlying value of the account's liability dTokens (at projected, as-of-now rates) minus its net borrowed principal (cost basis tracked from borrow/repay history). Token-denominated (raw integer at tokenDecimals; use priceUsd for USD), lifetime, and survives a full exit the same way. May be slightly negative for the same dust/rate reasons. |
| `emissionsEarnedBlnd` | `String`! |  |
| `emissionsEarnedUsd` | `Float` |  |
| `priceUsd` | `Float` | The reserve asset's per-unit USD price from the pool's oracle — lets a client value token-denominated fields like interestEarned in USD. |

### BlendSupplyChange

A change to the account's uncollateralized supply position in a Blend v2 pool reserve: underlying tokens lent to the pool without backing a borrow. Pairs: (BLEND_SUPPLY, CREDIT), (BLEND_SUPPLY, DEBIT).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! |  |
| `reason` | [StateChangeReason](#statechangereason)! |  |
| `ingestedAt` | [Time](#time)! |  |
| `ledgerCreatedAt` | [Time](#time)! |  |
| `ledgerNumber` | [UInt32](#uint32)! |  |
| `account` | [Account](#account)! |  |
| `operation` | [Operation](#operation)! |  |
| `transaction` | [Transaction](#transaction)! |  |
| `tokenId` | `String`! | Contract ID of the reserve asset supplied or withdrawn. |
| `amount` | `String`! | Underlying amount moved, as a decimal string in the asset's smallest unit. |
| `poolId` | `String`! | Contract ID of the Blend pool the reserve belongs to. |

### DataEntryAddedChange

A data entry created on the account. Pair: (DATA_ENTRY, ADD).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `name` | `String`! | Name of the data entry. |
| `value` | `String`! | Value of the new entry, base64-encoded. |

### DataEntryRemovedChange

A data entry removed from the account. Pair: (DATA_ENTRY, REMOVE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `name` | `String`! | Name of the data entry. |
| `oldValue` | `String`! | Value the entry had when removed, base64-encoded. |

### DataEntryUpdatedChange

An existing data entry's value changed. Pair: (DATA_ENTRY, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `name` | `String`! | Name of the data entry. |
| `oldValue` | `String`! | Previous value, base64-encoded. |
| `newValue` | `String`! | New value, base64-encoded. |

### HomeDomainClearedChange

A home domain removed from the account. Pair: (HOME_DOMAIN, CLEAR).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `oldHomeDomain` | `String`! | Home domain the account had when it was removed. |

### HomeDomainSetChange

A home domain set on an account that had none. Pair: (HOME_DOMAIN, SET).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `homeDomain` | `String`! | The newly set home domain. |

### HomeDomainUpdatedChange

An existing home domain replaced by a different one. Pair: (HOME_DOMAIN, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `oldHomeDomain` | `String`! | Previous home domain. |
| `newHomeDomain` | `String`! | New home domain. |

### LiquidityPoolBalance

An account's liquidity-pool share holding. `balance` is the account's pool shares and `tokenId` is the pool ID; `reserves` carries the pool's constituent assets and amounts.

Implements: [Balance](#balance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |
| `reserves` | \[[LiquidityPoolReserve](#liquiditypoolreserve)!\]! | The pool's constituent assets and reserve amounts. |
| `lastModifiedLedger` | [UInt32](#uint32)! | Ledger in which this pool-share trustline was last modified. |

### LiquidityPoolReserve

One constituent asset of a liquidity pool and its reserve amount.

| Field | Type | Description |
|---|---|---|
| `asset` | `String`! | Canonical asset name: CODE:ISSUER, or native for XLM. |
| `amount` | `String`! | Amount of this asset in the pool, with 7 decimal places. |

### NativeBalance

The account's native XLM balance.

Implements: [Balance](#balance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |
| `minimumBalance` | `String`! | Minimum XLM balance the account must hold, with 7 decimal places. It is the base reserve requirement and excludes liabilities: (2 + numSubentries + numSponsoring - numSponsored) * baseReserve. Spendable balance = balance - minimumBalance - sellingLiabilities. |
| `buyingLiabilities` | `String`! | XLM locked in open buy offers, with 7 decimal places. |
| `sellingLiabilities` | `String`! | XLM locked in open sell offers, with 7 decimal places. |
| `numSubentries` | [UInt32](#uint32)! | Number of subentries on the account (trustlines, offers, data entries, signers). |
| `lastModifiedLedger` | [UInt32](#uint32)! | Ledger in which this balance entry was last modified. |

### Operation

One operation within a Stellar transaction.

| Field | Type | Description |
|---|---|---|
| `id` | [Int64](#int64)! | Operation ID (TOID): a globally unique, chronologically sortable identifier. |
| `type` | [OperationType](#operationtype)! | The operation's type. |
| `operationXdr` | `String`! | The operation body, base64-encoded XDR. |
| `resultCode` | `String`! | Operation result code in snake case (for example op_success, op_underfunded, op_bad_auth). |
| `successful` | `Boolean`! | Whether the operation succeeded. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that included this operation. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that included this operation. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this operation. |
| `transaction` | [Transaction](#transaction)! | Transaction that contains this operation. |
| `accounts` | \[[Account](#account)!\]! | Accounts and contracts that participated in this operation. Each one has this operation in its `operations` history. |
| `stateChanges` | [StateChangeConnection](#statechangeconnection)! | State changes produced by this operation, in ledger order. |

Arguments of `stateChanges`:

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

### OperationConnection

Relay-style page of operations.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[OperationEdge](#operationedge)!\]! | Operations in this page, in ledger order (oldest first). |
| `pageInfo` | [PageInfo](#pageinfo)! | Cursors and page flags for fetching adjacent pages. |

### OperationEdge

One operation in a page, with its pagination cursor.

| Field | Type | Description |
|---|---|---|
| `node` | [Operation](#operation)! | The operation. |
| `cursor` | `String`! | Opaque cursor for this edge. Pass it as `after` or `before`. |

### PageInfo

Relay-style pagination metadata; cursors are opaque strings.

| Field | Type | Description |
|---|---|---|
| `startCursor` | `String` | Cursor of the first edge in this page; null when the page is empty. |
| `endCursor` | `String` | Cursor of the last edge in this page; null when the page is empty. |
| `hasNextPage` | `Boolean`! | Forward paging (`first`, or no paging arguments): true when more edges follow this page. Backward paging (`last`): true whenever `before` was given. |
| `hasPreviousPage` | `Boolean`! | Backward paging (`last`): true when more edges precede this page. Forward paging: true whenever `after` was given. |

### SACBalance

A Stellar Asset Contract balance held by a contract address.

Implements: [Balance](#balance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |
| `code` | `String`! | Asset code of the wrapped classic asset. |
| `issuer` | `String`! | Issuer account address (G...) of the wrapped classic asset. |
| `decimals` | `Int`! | Number of decimal places in the balance amount. |
| `isAuthorized` | `Boolean`! | Whether the holder is authorized to transact the asset. |
| `isClawbackEnabled` | `Boolean`! | Whether the issuer can claw the asset back from this holder. |

### SEP41Allowance

An approve() grant issued by a SEP-41 token holder.

| Field | Type | Description |
|---|---|---|
| `owner` | `String`! | Token holder (G... or C...) that granted the allowance. |
| `spender` | `String`! | Address (G... or C...) authorized to spend from the holder's balance. |
| `tokenId` | `String`! | Contract ID (C...) of the token. |
| `amount` | `String`! | Approved allowance, as an integer string in the token's smallest unit. |
| `expirationLedger` | [UInt32](#uint32)! | Last ledger sequence at which the allowance is live. |
| `lastModifiedLedger` | [UInt32](#uint32)! | Ledger in which this allowance was last modified. |

### SEP41AllowanceConnection

Relay-style page of SEP-41 allowances.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[SEP41AllowanceEdge](#sep41allowanceedge)!\]! | Allowances in this page, ordered by spender. |
| `pageInfo` | [PageInfo](#pageinfo)! | Cursors and page flags for fetching adjacent pages. |

### SEP41AllowanceEdge

One SEP-41 allowance in a page, with its pagination cursor.

| Field | Type | Description |
|---|---|---|
| `node` | [SEP41Allowance](#sep41allowance)! | The allowance. |
| `cursor` | `String`! | Opaque cursor for this edge. Pass it as `after` or `before`. |

### SEP41Balance

A pure SEP-41 (non-SAC) contract token balance.

Implements: [Balance](#balance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |
| `name` | `String` | Token name reported by the contract; null when the contract does not expose one. |
| `symbol` | `String` | Token symbol reported by the contract; null when the contract does not expose one. |
| `decimals` | `Int`! | Number of decimal places in the balance amount. |
| `lastModifiedLedger` | [UInt32](#uint32)! | Ledger in which this balance entry was last modified. |

### SignerAddedChange

A signer added to the account. Pair: (SIGNER, ADD).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `signerAddress` | `String`! | Signer key as a strkey: account (G...), pre-authorized transaction (T...), SHA-256 hash (X...), or signed payload (P...). |
| `newWeight` | `Int`! | Weight assigned to the new signer (0-255). |

### SignerRemovedChange

A signer removed from the account. Pair: (SIGNER, REMOVE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `signerAddress` | `String`! | Signer key as a strkey: account (G...), pre-authorized transaction (T...), SHA-256 hash (X...), or signed payload (P...). |
| `oldWeight` | `Int`! | Weight the signer had before removal (0-255). |

### SignerUpdatedChange

An existing signer's weight changed. Pair: (SIGNER, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `signerAddress` | `String`! | Signer key as a strkey: account (G...), pre-authorized transaction (T...), SHA-256 hash (X...), or signed payload (P...). |
| `oldWeight` | `Int`! | Weight before this change (0-255). Can be 0 when the signer is the master key and its weight was 0. |
| `newWeight` | `Int`! | New weight (0-255). |

### StateChangeConnection

Relay-style page of state changes.

| Field | Type | Description |
|---|---|---|
| `edges` | \[[StateChangeEdge](#statechangeedge)!\]! | State changes in this page, in ledger order (oldest first). |
| `pageInfo` | [PageInfo](#pageinfo)! | Cursors and page flags for fetching adjacent pages. |

### StateChangeEdge

One state change in a page, with its pagination cursor.

| Field | Type | Description |
|---|---|---|
| `node` | [BaseStateChange](#basestatechange)! | The state change. Select concrete-type fields with inline fragments. |
| `cursor` | `String`! | Opaque cursor for this edge. Pass it as `after` or `before`. |

### ThresholdChange

A signature-threshold change. `threshold` identifies which of the account's three thresholds changed; one state change is emitted per changed threshold. Pair: (SIGNATURE_THRESHOLD, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `threshold` | [ThresholdLevel](#thresholdlevel)! | Which signature threshold changed. |
| `oldThreshold` | `Int`! | Previous threshold value (0-255). |
| `newThreshold` | `Int`! | New threshold value (0-255). |

### Transaction

A Stellar transaction.

| Field | Type | Description |
|---|---|---|
| `hash` | `String`! | Transaction hash, 64 hex characters. For a fee-bump transaction, the outer (fee-bump) hash. |
| `feeCharged` | [Int64](#int64)! | Fee charged for the transaction, in stroops. |
| `resultCode` | `String`! | Transaction result code, as the XDR TransactionResultCode name (for example TransactionResultCodeTxSuccess). |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that included this transaction. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that included this transaction. |
| `isFeeBump` | `Boolean`! | Whether this transaction is a fee-bump transaction. |
| `memoType` | [MemoType](#memotype) | Memo type. Null while the memo backfill has not reached this transaction yet; NONE when the transaction has no memo. For a fee-bump transaction, the inner transaction's memo. |
| `memo` | `String` | Memo value: TEXT as UTF-8 (invalid sequences replaced with U+FFFD), ID as a decimal string, HASH and RETURN as 64 hex characters. Null for NONE and while not yet backfilled. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this transaction. |
| `operations` | [OperationConnection](#operationconnection)! | Operations contained in this transaction, in application order. |
| `accounts` | \[[Account](#account)!\]! | Accounts and contracts that participated in this transaction. Each one has this transaction in its `transactions` history. |
| `stateChanges` | [StateChangeConnection](#statechangeconnection)! | State changes produced by this transaction, in ledger order. |

Arguments of `operations`:

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

Arguments of `stateChanges`:

| Name | Type | Default | Description |
|---|---|---|---|
| `first` | `Int` |  | Return up to this many edges from the start of the list, or after `after`. Default 50 when neither `first` nor `last` is set. Maximum 100. Cannot be combined with `last` or `before`. |
| `after` | `String` |  | Cursor from a previous page. Returns the edges after it. Requires `first`; ignored without it. |
| `last` | `Int` |  | Return up to this many edges from the end of the list, or before `before`. Maximum 100. Cannot be combined with `first` or `after`. |
| `before` | `String` |  | Cursor from a previous page. Returns the edges before it. Requires `last`; ignored without it. |

### TrustlineAddedChange

A trustline created. Exactly one of tokenId / liquidityPoolId is set. Pair: (TRUSTLINE, ADD).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String` | Stellar Asset Contract ID (C...) of the trusted asset; null for liquidity-pool-share trustlines. |
| `liquidityPoolId` | `String` | Hex-encoded liquidity pool ID for pool-share trustlines; null for asset trustlines. |
| `limit` | `String`! | Initial trustline limit, as a decimal string with 7 decimal places (for example "100.0000000"). |

### TrustlineBalance

A classic Stellar asset held via a trustline.

Implements: [Balance](#balance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |
| `code` | `String`! | Asset code (1-12 characters). |
| `issuer` | `String`! | Issuer account address (G...). |
| `assetType` | [AssetType](#assettype)! | Classic asset type, determined by the asset code length. |
| `limit` | `String`! | Trustline limit, with 7 decimal places. |
| `buyingLiabilities` | `String`! | Amount locked in open buy offers, with 7 decimal places. |
| `sellingLiabilities` | `String`! | Amount locked in open sell offers, with 7 decimal places. |
| `lastModifiedLedger` | [UInt32](#uint32)! | Ledger in which this trustline was last modified. |
| `isAuthorized` | `Boolean`! | Whether the holder is fully authorized to transact the asset. |
| `isAuthorizedToMaintainLiabilities` | `Boolean`! | Whether the holder may maintain existing liabilities on the asset. |

### TrustlineRemovedChange

A trustline removed. Exactly one of tokenId / liquidityPoolId is set. Pair: (TRUSTLINE, REMOVE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String` | Stellar Asset Contract ID (C...) of the trusted asset; null for liquidity-pool-share trustlines. |
| `liquidityPoolId` | `String` | Hex-encoded liquidity pool ID for pool-share trustlines; null for asset trustlines. |

### TrustlineUpdatedChange

A trustline limit updated. Exactly one of tokenId / liquidityPoolId is set. Pair: (TRUSTLINE, UPDATE).

Implements: [BaseStateChange](#basestatechange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation)! | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |
| `tokenId` | `String` | Stellar Asset Contract ID (C...) of the trusted asset; null for liquidity-pool-share trustlines. |
| `liquidityPoolId` | `String` | Hex-encoded liquidity pool ID for pool-share trustlines; null for asset trustlines. |
| `oldLimit` | `String`! | Previous trustline limit, as an integer string in stroops (for example "1000000000"). |
| `newLimit` | `String`! | New trustline limit, as a decimal string with 7 decimal places (for example "100.0000000"). |

## Interfaces

### Balance

Common contract for every token balance held by an account.

Implemented by: [LiquidityPoolBalance](#liquiditypoolbalance), [NativeBalance](#nativebalance), [SACBalance](#sacbalance), [SEP41Balance](#sep41balance), [TrustlineBalance](#trustlinebalance)

| Field | Type | Description |
|---|---|---|
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, SAC, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |

### BaseStateChange

Common contract implemented by every state change. A state change records one modification to one account's ledger state, attributed to the transaction (and, except for transaction fees, the operation) that caused it.

Each concrete type documents the exact (category, reason) pairs it represents and the nullability of every field, so the variant structure is fully encoded in the schema. Select concrete-type fields via inline fragments; `category` and `reason` carry the same discrimination for generic consumers.

Implemented by: [AccountCreatedChange](#accountcreatedchange), [AccountFlagsChange](#accountflagschange), [AccountMergedChange](#accountmergedchange), [AllowanceChange](#allowancechange), [BalanceAuthorizationChange](#balanceauthorizationchange), [BalanceChange](#balancechange), [BlendAuctionChange](#blendauctionchange), [BlendBackstopChange](#blendbackstopchange), [BlendBackstopEmissionsClaimChange](#blendbackstopemissionsclaimchange), [BlendBackstopQueueChange](#blendbackstopqueuechange), [BlendCollateralChange](#blendcollateralchange), [BlendDebtChange](#blenddebtchange), [BlendEmissionsClaimChange](#blendemissionsclaimchange), [BlendSupplyChange](#blendsupplychange), [DataEntryAddedChange](#dataentryaddedchange), [DataEntryRemovedChange](#dataentryremovedchange), [DataEntryUpdatedChange](#dataentryupdatedchange), [HomeDomainClearedChange](#homedomainclearedchange), [HomeDomainSetChange](#homedomainsetchange), [HomeDomainUpdatedChange](#homedomainupdatedchange), [SignerAddedChange](#signeraddedchange), [SignerRemovedChange](#signerremovedchange), [SignerUpdatedChange](#signerupdatedchange), [ThresholdChange](#thresholdchange), [TrustlineAddedChange](#trustlineaddedchange), [TrustlineRemovedChange](#trustlineremovedchange), [TrustlineUpdatedChange](#trustlineupdatedchange)

| Field | Type | Description |
|---|---|---|
| `category` | [StateChangeCategory](#statechangecategory)! | Category of account state this change affects. |
| `reason` | [StateChangeReason](#statechangereason)! | Why the change occurred. Each concrete type documents its valid reasons. |
| `ingestedAt` | [Time](#time)! | When the indexer persisted this state change. |
| `ledgerCreatedAt` | [Time](#time)! | Close time of the ledger that produced this change. |
| `ledgerNumber` | [UInt32](#uint32)! | Sequence number of the ledger that produced this change. |
| `account` | [Account](#account)! | Account or contract whose state changed. |
| `operation` | [Operation](#operation) | Operation that caused this change. Non-null on every concrete type except BalanceChange, where it is null on transaction-fee rows (fees are charged per transaction, not per operation). |
| `transaction` | [Transaction](#transaction)! | Transaction that caused this change. |

## Enums

### AccountFlag

Stellar account authorization flag.

| Value | Description |
|---|---|
| `AUTH_REQUIRED` | Holders of the account's assets must be authorized by the issuer. |
| `AUTH_REVOCABLE` | The issuer can revoke a holder's authorization. |
| `AUTH_IMMUTABLE` | The account's flags can never be changed again. |
| `AUTH_CLAWBACK_ENABLED` | The issuer can claw back its assets from holders. |

### AssetType

Classic Stellar asset type, determined by the asset code length.

| Value | Description |
|---|---|
| `CREDIT_ALPHANUM4` | Asset code of 1-4 characters. |
| `CREDIT_ALPHANUM12` | Asset code of 5-12 characters. |

### BlendAuctionType

Which object a Blend v2 Dutch auction sells off.

| Value | Description |
|---|---|
| `USER_LIQUIDATION` | A user's collateral, sold to cover their debt. |
| `BAD_DEBT` | Backstop LP tokens, sold to cover debt socialized to the backstop. |
| `INTEREST` | Accrued pool interest, sold for backstop LP tokens. |

### BlendPoolStatus

BlendPoolStatus is a pool's operational status. On-chain encoding: 0 ADMIN_ACTIVE, 1 ACTIVE, 2 ADMIN_ON_ICE, 3 ON_ICE, 4 ADMIN_FROZEN, 5 FROZEN, 6 SETUP. Statuses 0-3 accept supply (deposits); 0-1 also allow borrowing; 4-6 reject both.

| Value | Description |
|---|---|
| `ADMIN_ACTIVE` |  |
| `ACTIVE` |  |
| `ADMIN_ON_ICE` |  |
| `ON_ICE` |  |
| `ADMIN_FROZEN` |  |
| `FROZEN` |  |
| `SETUP` |  |

### BlendVersion

BlendVersion is the Blend deployment a backstop belongs to. V2 is the original deployment; V2_1 is the 2026-09-30 redeploy with the patched Comet LP token. Both are indexed. Values for V2 backstop positions reflect the old Comet pool's on-chain state after the 2026-08-25 incident.

| Value | Description |
|---|---|
| `V2` |  |
| `V2_1` |  |

### MemoType

Transaction memo type, matching the XDR MemoType names.

| Value | Description |
|---|---|
| `NONE` | The transaction has no memo. |
| `TEXT` | A UTF-8 text memo of at most 28 bytes. |
| `ID` | An unsigned 64-bit integer memo. |
| `HASH` | A 32-byte hash memo. |
| `RETURN` | A 32-byte hash of a transaction this transaction refunds or returns. |

### OperationType

Stellar operation type, one value per operation defined by the Stellar protocol (matching the XDR OperationType names).

| Value | Description |
|---|---|
| `CREATE_ACCOUNT` | Creates and funds a new account. |
| `PAYMENT` | Sends an asset to a destination account. |
| `PATH_PAYMENT_STRICT_RECEIVE` | Path payment where the destination receives an exact amount. |
| `PATH_PAYMENT_STRICT_SEND` | Path payment where the source sends an exact amount. |
| `MANAGE_SELL_OFFER` | Creates, updates, or deletes an offer to sell an asset. |
| `CREATE_PASSIVE_SELL_OFFER` | Creates a passive sell offer, which does not take offers at the same price. |
| `MANAGE_BUY_OFFER` | Creates, updates, or deletes an offer to buy an asset. |
| `SET_OPTIONS` | Sets account options: flags, thresholds, signers, home domain, inflation destination. |
| `CHANGE_TRUST` | Creates, updates, or removes a trustline. |
| `ALLOW_TRUST` | Issuer authorizes or deauthorizes a trustline to its asset. |
| `ACCOUNT_MERGE` | Merges the source account into a destination account and removes it. |
| `INFLATION` | Inflation operation. Fails on protocol 12 and later. |
| `MANAGE_DATA` | Sets, updates, or deletes an account data entry. |
| `BUMP_SEQUENCE` | Raises the source account's sequence number. |
| `CREATE_CLAIMABLE_BALANCE` | Creates a claimable balance. |
| `CLAIM_CLAIMABLE_BALANCE` | Claims a claimable balance. |
| `BEGIN_SPONSORING_FUTURE_RESERVES` | Starts sponsoring reserves for another account. |
| `END_SPONSORING_FUTURE_RESERVES` | Ends a sponsorship started by BEGIN_SPONSORING_FUTURE_RESERVES. |
| `REVOKE_SPONSORSHIP` | Removes or transfers sponsorship of a ledger entry or signer. |
| `CLAWBACK` | Issuer claws back an asset from a holder's trustline. |
| `CLAWBACK_CLAIMABLE_BALANCE` | Issuer claws back a claimable balance. |
| `SET_TRUST_LINE_FLAGS` | Issuer sets or clears authorization flags on a trustline. |
| `LIQUIDITY_POOL_DEPOSIT` | Deposits assets into a liquidity pool in exchange for pool shares. |
| `LIQUIDITY_POOL_WITHDRAW` | Withdraws assets from a liquidity pool by redeeming pool shares. |
| `INVOKE_HOST_FUNCTION` | Soroban: invokes a contract function, uploads Wasm, or creates a contract. |
| `EXTEND_FOOTPRINT_TTL` | Soroban: extends the time to live of ledger entries in the footprint. |
| `RESTORE_FOOTPRINT` | Soroban: restores archived ledger entries in the footprint. |

### StateChangeCategory

Category of account state affected by a state change. Each category maps to one or more concrete BaseStateChange types; every concrete type documents its exact (category, reason) pairs.

| Value | Description |
|---|---|
| `BALANCE` | Token balance movements: BalanceChange (operation-sourced, or a transaction-fee row with null operation). |
| `ACCOUNT` | Account lifecycle: AccountCreatedChange, AccountMergedChange. |
| `SIGNER` | Account signer set: SignerAddedChange, SignerUpdatedChange, SignerRemovedChange. |
| `SIGNATURE_THRESHOLD` | Signature thresholds: ThresholdChange. |
| `DATA_ENTRY` | Account data entries: DataEntryAddedChange, DataEntryUpdatedChange, DataEntryRemovedChange. |
| `HOME_DOMAIN` | The account's home domain: HomeDomainSetChange, HomeDomainUpdatedChange, HomeDomainClearedChange. |
| `ALLOWANCE` | SEP-41 token allowances: AllowanceChange. |
| `FLAGS` | Account authorization flags: AccountFlagsChange. |
| `TRUSTLINE` | Trustlines: TrustlineAddedChange, TrustlineUpdatedChange, TrustlineRemovedChange. |
| `BALANCE_AUTHORIZATION` | Asset authorization for a holder: BalanceAuthorizationChange. |
| `BLEND_SUPPLY` | Blend v2 uncollateralized supply positions: BlendSupplyChange. |
| `BLEND_COLLATERAL` | Blend v2 collateralized supply positions: BlendCollateralChange. |
| `BLEND_DEBT` | Blend v2 debt positions: BlendDebtChange. |
| `BLEND_AUCTION` | Blend v2 Dutch-auction fills: BlendAuctionChange. |
| `BLEND_EMISSIONS` | Blend v2 pool reserve emissions: BlendEmissionsClaimChange. |
| `BLEND_BACKSTOP_EMISSIONS` | Blend v2 backstop emissions: BlendBackstopEmissionsClaimChange. |
| `BLEND_BACKSTOP` | Blend v2 backstop deposits: BlendBackstopChange. |
| `BLEND_BACKSTOP_QUEUE` | Blend v2 backstop withdrawal queue entries: BlendBackstopQueueChange. |

### StateChangeReason

Why a state change occurred. Each value applies only to the categories listed in its description.

| Value | Description |
|---|---|
| `CREATE` | ACCOUNT: account created (classic) or contract deployed. |
| `MERGE` | ACCOUNT: account merged into another account. |
| `DEBIT` | BALANCE: value left the account (payment sent, or a transaction-fee charge with null operation); BLEND_SUPPLY, BLEND_COLLATERAL, or BLEND_BACKSTOP: position decreased. |
| `CREDIT` | BALANCE: value entered the account (payment received); BLEND_SUPPLY, BLEND_COLLATERAL, or BLEND_BACKSTOP: position increased. |
| `MINT` | BALANCE: tokens minted to the account. |
| `BURN` | BALANCE: tokens burned from the account (including clawbacks); BLEND_DEBT: defaulted debt written off (dTokens burnt). |
| `ADD` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry added; BLEND_BACKSTOP_QUEUE: withdrawal queued. |
| `REMOVE` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry removed; BLEND_BACKSTOP_QUEUE: queued withdrawal cancelled. |
| `UPDATE` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry updated; SIGNATURE_THRESHOLD: threshold changed; HOME_DOMAIN: domain changed from one value to another; ALLOWANCE: SEP-41 allowance approved. |
| `SET` | FLAGS or BALANCE_AUTHORIZATION: flags turned on; HOME_DOMAIN: domain set on an account that had none. |
| `CLEAR` | FLAGS or BALANCE_AUTHORIZATION: flags turned off; HOME_DOMAIN: domain removed. |
| `BORROW` | BLEND_DEBT: underlying borrowed from the pool. |
| `REPAY` | BLEND_DEBT: underlying repaid to the pool. |
| `FLASH_LOAN` | BLEND_DEBT: borrow repaid within the same transaction (fee retained by the pool). |
| `BAD_DEBT` | BLEND_DEBT: debt socialized to the backstop (dTokens transferred). |
| `FILL` | BLEND_AUCTION: a Dutch auction was (partially) filled. |
| `CLAIM` | BLEND_EMISSIONS or BLEND_BACKSTOP_EMISSIONS: accrued emissions claimed. |

### ThresholdLevel

Which of an account's three signature thresholds a ThresholdChange refers to.

| Value | Description |
|---|---|
| `LOW` | The low threshold. |
| `MEDIUM` | The medium threshold. |
| `HIGH` | The high threshold. |

### TokenType

Classification of a token as held in an account's balance.

| Value | Description |
|---|---|
| `NATIVE` | The native XLM asset. |
| `CLASSIC` | A classic Stellar asset held via a trustline. |
| `SAC` | A Stellar Asset Contract balance held by a contract address. |
| `SEP41` | A pure SEP-41 (non-SAC) contract token balance. |
| `LIQUIDITY_POOL` | A liquidity-pool share position. |

### TrustlineFlag

Stellar trustline authorization flag.

| Value | Description |
|---|---|
| `AUTHORIZED` | The holder is fully authorized to transact the asset. |
| `AUTHORIZED_TO_MAINTAIN_LIABILITIES` | The holder may only maintain existing liabilities on the asset. |
| `CLAWBACK_ENABLED` | The issuer can claw the asset back from this trustline. |

## Input objects

### AccountStateChangeFilterInput

Filters for an account's state changes; all conditions are ANDed.

| Field | Type | Description |
|---|---|---|
| `transactionHash` | `String` | Only state changes from the transaction with this hash (64 hex characters). Any other format returns an INVALID_TRANSACTION_HASH error. |
| `operationId` | [Int64](#int64) | Only state changes from the operation with this ID (TOID). |
| `category` | [StateChangeCategory](#statechangecategory) | Only state changes with this category. |
| `reason` | [StateChangeReason](#statechangereason) | Only state changes with this reason. |

### MemoFilter

Exact-match filter on a transaction's memo. NONE is not a valid filter type.

`value` encodings: TEXT is the memo text (at most 28 bytes), ID is a decimal unsigned 64-bit integer, HASH and RETURN are 64 hex characters.

An account with a long history and few matching memos walks its whole history within the time range, so set `since`/`until` to bound it.

| Field | Type | Description |
|---|---|---|
| `type` | [MemoType](#memotype)! | Memo type to match. Must not be NONE. |
| `value` | `String`! | Memo value to match, encoded as described on MemoFilter. |

## Scalars

### Int64

Signed 64-bit integer, serialized as a JSON number. GraphQL's Int is 32-bit; this scalar carries larger values such as operation IDs (TOIDs) and stroop amounts.

### Time

RFC 3339 timestamp.

### UInt32

Unsigned 32-bit integer, serialized as a JSON number. Used for ledger sequence numbers and other non-negative counters.
