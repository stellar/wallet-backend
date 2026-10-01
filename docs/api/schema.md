# GraphQL schema reference

Every query, type, and field served at `POST /graphql/query`. For auth, pagination, and examples, see [GraphQL API](graphql.md). Generated from the schema files by `make gql-docs`; do not edit by hand.

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
| `stateChanges` | \[[BaseStateChange](#basestatechange)!\]! | This transaction's state changes that affected the account. Not paginated. |

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
| `toMuxedId` | `String` | CAP-67 destination memo carried by SEP-41 transfer/mint events (CREDIT and MINT only). Rendered as a decimal string so u64 values above 2^53-1 survive JSON number quantization. |

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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
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
| `balance` | `String`! | Balance amount as a decimal string. Native XLM, trustline, and liquidity-pool balances have 7 decimal places (for example "100.0000000"). SAC and SEP-41 balances are integers in the token's smallest unit; divide by 10^decimals. |
| `tokenId` | `String`! | Token identifier: the token's contract ID (C...), which for native XLM and classic assets is the Stellar Asset Contract ID, or the hex-encoded pool ID for liquidity-pool shares. |
| `tokenType` | [TokenType](#tokentype)! | Classification of the token. |

### BaseStateChange

Common contract implemented by every state change. A state change records one modification to one account's ledger state, attributed to the transaction (and, except for transaction fees, the operation) that caused it.

Each concrete type documents the exact (category, reason) pairs it represents and the nullability of every field, so the variant structure is fully encoded in the schema. Select concrete-type fields via inline fragments; `category` and `reason` carry the same discrimination for generic consumers.

Implemented by: [AccountCreatedChange](#accountcreatedchange), [AccountFlagsChange](#accountflagschange), [AccountMergedChange](#accountmergedchange), [AllowanceChange](#allowancechange), [BalanceAuthorizationChange](#balanceauthorizationchange), [BalanceChange](#balancechange), [DataEntryAddedChange](#dataentryaddedchange), [DataEntryRemovedChange](#dataentryremovedchange), [DataEntryUpdatedChange](#dataentryupdatedchange), [HomeDomainClearedChange](#homedomainclearedchange), [HomeDomainSetChange](#homedomainsetchange), [HomeDomainUpdatedChange](#homedomainupdatedchange), [SignerAddedChange](#signeraddedchange), [SignerRemovedChange](#signerremovedchange), [SignerUpdatedChange](#signerupdatedchange), [ThresholdChange](#thresholdchange), [TrustlineAddedChange](#trustlineaddedchange), [TrustlineRemovedChange](#trustlineremovedchange), [TrustlineUpdatedChange](#trustlineupdatedchange)

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

### StateChangeReason

Why a state change occurred. Each value applies only to the categories listed in its description.

| Value | Description |
|---|---|
| `CREATE` | ACCOUNT: account created (classic) or contract deployed. |
| `MERGE` | ACCOUNT: account merged into another account. |
| `DEBIT` | BALANCE: value left the account (payment sent, or a transaction-fee charge with null operation). |
| `CREDIT` | BALANCE: value entered the account (payment received). |
| `MINT` | BALANCE: tokens minted to the account. |
| `BURN` | BALANCE: tokens burned from the account (including clawbacks). |
| `ADD` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry added. |
| `REMOVE` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry removed. |
| `UPDATE` | SIGNER, TRUSTLINE, or DATA_ENTRY: entry updated; SIGNATURE_THRESHOLD: threshold changed; HOME_DOMAIN: domain changed from one value to another; ALLOWANCE: SEP-41 allowance approved. |
| `SET` | FLAGS or BALANCE_AUTHORIZATION: flags turned on; HOME_DOMAIN: domain set on an account that had none. |
| `CLEAR` | FLAGS or BALANCE_AUTHORIZATION: flags turned off; HOME_DOMAIN: domain removed. |

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

## Scalars

### Int64

Signed 64-bit integer, serialized as a JSON number. GraphQL's Int is 32-bit; this scalar carries larger values such as operation IDs (TOIDs) and stroop amounts.

### Time

RFC 3339 timestamp.

### UInt32

Unsigned 32-bit integer, serialized as a JSON number. Used for ledger sequence numbers and other non-negative counters.
