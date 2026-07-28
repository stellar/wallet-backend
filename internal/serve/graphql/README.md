# API Reference

The wallet-backend provides a GraphQL API that enables flexible querying of blockchain data including transactions, operations, accounts, and state changes. The API is designed for applications that need efficient, customizable data retrieval with strong typing and introspection capabilities.

**Key Benefits:**
- **Flexible Queries**: Request exactly the data you need, nothing more
- **Strong Typing**: Full type safety with schema introspection
- **Efficient Data Loading**: Built-in DataLoaders prevent N+1 queries
- **Cursor-based Pagination**: Relay-style pagination for all list queries
- **Rich Relationships**: Easily traverse relationships between accounts, transactions, operations, and state changes

**In this section:**
- [Getting Started](#getting-started)
- [Queries](#queries)
- [Pagination](#pagination)
- [State Changes](#state-changes)
- [Error Handling](#error-handling)
- [Performance Features](#performance-features)

### Getting Started

**Endpoint**: `POST /graphql`

**Authentication**: All GraphQL requests require JWT authentication. See the [Authentication](../../../README.md#authentication) section in the main README for details.

**Quick Example:**

```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "query": "{ transactionByHash(hash: \"abc123...\") { hash ledgerNumber feeCharged } }"
  }'
```

**Schema Introspection:**

You can explore the full schema using GraphQL introspection, when enabled:

```graphql
query {
  __schema {
    types {
      name
      description
    }
  }
}
```

Introspection (`__schema`, `__type`) is **disabled by default** — it exposes the full schema, including any unreleased or internal-only fields, to anyone who can reach the endpoint. Enable it with the `--graphql-introspection-enabled` flag or `GRAPHQL_INTROSPECTION_ENABLED` environment variable. Production deployments should leave it disabled; dev environments typically enable it.

## Queries

The GraphQL API provides three root queries for accessing blockchain data — `transactionByHash`, `accountByAddress`, and `operationById`. Account balances are fetched through `accountByAddress`:

| # | Query | Description |
|---|-------|-------------|
| 1 | [`transactionByHash`](#1-get-transaction-by-hash) | Get a specific transaction by its hash |
| 2 | [`accountByAddress`](#2-get-account-by-address) | Get account info and related data |
| 3 | [`operationById`](#3-get-operation-by-id) | Get a specific operation by ID |

### 1. Get Transaction by Hash

Retrieve a specific transaction by its hash.

```graphql
query GetTransaction {
  transactionByHash(hash: "abc123...") {
    hash
    feeCharged
    resultCode
    ledgerNumber
    ledgerCreatedAt
    isFeeBump
    ingestedAt

    # Related data
    accounts {
      address
    }

    operations(first: 10) {
      edges {
        node {
          id
          type
        }
      }
    }
  }
}
```

### 2. Get Account by Address

Retrieve account information and related data.

```graphql
query GetAccount {
  accountByAddress(address: "GABC...") {
    address

    # Related transactions
    transactions(first: 10) {
      edges {
        node {
          hash
          ledgerNumber
        }
      }
      pageInfo {
        hasNextPage
      }
    }

    # Related operations
    operations(first: 20) {
      edges {
        node {
          id
          type
          operationXdr
        }
      }
    }

    # Related state changes with optional filtering
    stateChanges(
      filter: {
        transactionHash: "abc123..."  # Filter by transaction hash
        operationId: 12345            # Filter by operation ID
        category: BALANCE             # Filter by state change category (enum)
        reason: CREDIT                # Filter by state change reason (enum)
      }
      first: 50
    ) {
      edges {
        node {
          category
          reason
          ... on BalanceChange {
            tokenId
            amount
            ledgerNumber
          }
          ... on SignerAddedChange {
            signerAddress
            newWeight
          }
        }
      }
    }
  }
}
```

**State Changes Filter Parameters:**

The `stateChanges` field on Account supports an optional `filter` parameter with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `transactionHash` | `String` | Filter by transaction hash - returns only state changes from this transaction |
| `operationId` | `Int64` | Filter by operation ID - returns only state changes from this operation |
| `category` | `StateChangeCategory` | Filter by state change category enum (e.g., `BALANCE`, `ACCOUNT`, `SIGNER`, `TRUSTLINE`, `FLAGS`) |
| `reason` | `StateChangeReason` | Filter by state change reason enum (e.g., `CREDIT`, `DEBIT`, `CREATE`, `MERGE`, `ADD`, `REMOVE`) |

Enum-typed filters take unquoted enum values (`category: BALANCE`), not strings. All conditions are ANDed.

### 3. Get Operation by ID

Retrieve a specific operation by its ID.

```graphql
query GetOperation {
  operationById(id: 12345) {
    id
    type
    operationXdr
    resultCode
    successful
    ledgerNumber
    ledgerCreatedAt

    transaction {
      hash
      ledgerNumber
    }

    accounts {
      address
    }

    stateChanges(first: 10) {
      edges {
        node {
          ... on BalanceChange {
            tokenId
            amount
          }
        }
      }
    }
  }
}
```

**Operation Types:**

The `type` field (an `OperationType` enum) supports all Stellar operation types:
- `CREATE_ACCOUNT`, `PAYMENT`, `PATH_PAYMENT_STRICT_RECEIVE`, `PATH_PAYMENT_STRICT_SEND`
- `MANAGE_SELL_OFFER`, `CREATE_PASSIVE_SELL_OFFER`, `MANAGE_BUY_OFFER`
- `SET_OPTIONS`, `CHANGE_TRUST`, `ALLOW_TRUST`, `ACCOUNT_MERGE`, `INFLATION`
- `MANAGE_DATA`, `BUMP_SEQUENCE`
- `CREATE_CLAIMABLE_BALANCE`, `CLAIM_CLAIMABLE_BALANCE`
- `BEGIN_SPONSORING_FUTURE_RESERVES`, `END_SPONSORING_FUTURE_RESERVES`, `REVOKE_SPONSORSHIP`
- `CLAWBACK`, `CLAWBACK_CLAIMABLE_BALANCE`, `SET_TRUST_LINE_FLAGS`
- `LIQUIDITY_POOL_DEPOSIT`, `LIQUIDITY_POOL_WITHDRAW`
- `INVOKE_HOST_FUNCTION`, `EXTEND_FOOTPRINT_TTL`, `RESTORE_FOOTPRINT` (Soroban)

### 4. Get Account Balances

Retrieve account balances through a Relay-style connection, including native XLM, classic trustlines, and contract tokens.

```graphql
query GetAccountBalances {
  accountByAddress(address: "GABC...") {
    balances(first: 50) {
      edges {
        node {
          __typename
          tokenId
          tokenType
          balance

          ... on NativeBalance {
            minimumBalance
            buyingLiabilities
            sellingLiabilities
            numSubentries
            lastModifiedLedger
          }

          ... on TrustlineBalance {
            code
            issuer
            assetType
            limit
            buyingLiabilities
            sellingLiabilities
            lastModifiedLedger
            isAuthorized
            isAuthorizedToMaintainLiabilities
          }

          ... on SACBalance {
            code
            issuer
            decimals
            isAuthorized
            isClawbackEnabled
          }

          ... on SEP41Balance {
            name
            symbol
            decimals
            lastModifiedLedger
          }

          ... on LiquidityPoolBalance {
            reserves {
              asset
              amount
            }
            lastModifiedLedger
          }
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
```

**Balance Types:**

The query returns different balance types based on the token:

All balance types implement the `Balance` interface. Select concrete-type fields via inline fragments.

| Type | Description | Key Fields |
|------|-------------|------------|
| `NativeBalance` | XLM (native asset) | `minimumBalance`, `buyingLiabilities`, `sellingLiabilities`, `numSubentries`, `lastModifiedLedger` |
| `TrustlineBalance` | Classic Stellar trustlines | `code`, `issuer`, `assetType`, `limit`, `buyingLiabilities`, `sellingLiabilities`, `isAuthorized`, `isAuthorizedToMaintainLiabilities`, `lastModifiedLedger` |
| `SACBalance` | Stellar Asset Contract (wrapped classic assets) | `code`, `issuer`, `decimals`, `isAuthorized`, `isClawbackEnabled` |
| `SEP41Balance` | Pure SEP-41 (non-SAC) contract token | `name`, `symbol`, `decimals`, `lastModifiedLedger` |
| `LiquidityPoolBalance` | Liquidity-pool share position | `reserves { asset amount }`, `lastModifiedLedger` |

**Common Fields (all balance types, from the `Balance` interface):**
- `balance: String!` - Current balance amount
- `tokenId: String!` - Contract ID (C...) for the token, or the pool ID for pool shares
- `tokenType: TokenType!` - One of: `NATIVE`, `CLASSIC`, `SAC`, `SEP41`, `LIQUIDITY_POOL`

**NativeBalance-specific Fields:**
- `minimumBalance: String!` - Base reserve requirement (excludes liabilities)
- `buyingLiabilities: String!` - XLM locked in open buy offers
- `sellingLiabilities: String!` - XLM locked in open sell offers
- `numSubentries: UInt32!` - Number of subentries (trustlines, offers, data entries, signers)
- `lastModifiedLedger: UInt32!` - Ledger in which this balance entry was last modified

**TrustlineBalance-specific Fields:**
- `assetType: AssetType!` - Classic asset type by code length: `CREDIT_ALPHANUM4` or `CREDIT_ALPHANUM12`

**Token Types:**
- `NATIVE` - XLM (Stellar's native asset)
- `CLASSIC` - Classic Stellar trustline assets
- `SAC` - Stellar Asset Contract (classic assets wrapped for Soroban)
- `SEP41` - Pure SEP-41 (non-SAC) contract tokens
- `LIQUIDITY_POOL` - Liquidity-pool share positions

**Example: Query with Type Fragments:**

```graphql
query GetDetailedBalances {
  accountByAddress(address: "GABC...") {
    balances(first: 25) {
      edges {
        node {
          tokenId
          balance
          tokenType

          ... on NativeBalance {
            minimumBalance
            buyingLiabilities
            sellingLiabilities
            lastModifiedLedger
          }

          ... on TrustlineBalance {
            code
            issuer
            limit
            isAuthorized
          }

          ... on SACBalance {
            code
            issuer
            decimals
          }
        }
      }
    }
  }
}
```

**Response Example:**

```json
{
  "data": {
    "accountByAddress": {
      "balances": {
        "edges": [
          {
            "node": {
              "tokenId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
              "balance": "100.0000000",
              "tokenType": "NATIVE",
              "minimumBalance": "1.0000000",
              "buyingLiabilities": "0.0000000",
              "sellingLiabilities": "0.0000000",
              "lastModifiedLedger": 12345678
            }
          },
          {
            "node": {
              "tokenId": "CAQCMV4JFG4EZXQEAV7TUV2E52DMSO2LQKBOSA7UM3B4NIP4DQJ3JHQJ",
              "balance": "500.0000000",
              "tokenType": "CLASSIC",
              "code": "USDC",
              "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
              "limit": "922337203685.4775807",
              "isAuthorized": true
            }
          }
        ],
        "pageInfo": {
          "endCursor": "djE6Y2xhc3NpYzoxMjNlNDU2Ny1lODliLTEyZDMtYTQ1Ni00MjY2MTQxNzQwMDA=",
          "hasNextPage": true
        }
      }
    }
  }
}
```

**How It Works:**

This query uses keyset pagination over the balance backing tables:

1. Reads native, trustline, and SAC balances from PostgreSQL in a stable source order
2. Builds Relay `edges` and `pageInfo` so clients can continue paging with opaque cursors

**Supported Address Types:**
- **G-addresses**: Returns native XLM, trustlines, and SAC balances
- **C-addresses** (contract addresses): Returns SAC balances only

**Error Handling:**

This query returns structured GraphQL errors with error codes in the `extensions` field:

| Error Code | Description |
|------------|-------------|
| `INVALID_ADDRESS` | The provided address is not a valid Stellar account (G...) or contract (C...) address |
| `BAD_USER_INPUT` | `first`/`last` exceeds the page size cap, or an invalid pagination argument combination was given |
| `INTERNAL_ERROR` | An unexpected error occurred while fetching or processing balance data (storage or RPC failure) |

**Error Response Example:**

```json
{
  "errors": [
    {
      "message": "invalid address format: must be a valid Stellar account (G...) or contract (C...) address",
      "extensions": {
        "code": "INVALID_ADDRESS",
        "address": "invalid-address"
      },
      "path": ["accountByAddress"]
    }
  ],
  "data": {
    "accountByAddress": null
  }
}
```

## Pagination

The API uses **Relay-style cursor-based pagination** for all list queries. This provides stable pagination even when data changes.

**Forward Pagination:**

```graphql
# Get first page
query {
  accountByAddress(address: "GABC...") {
    transactions(first: 10) {
      edges {
        node { hash }
        cursor
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}

# Get next page
query {
  accountByAddress(address: "GABC...") {
    transactions(first: 10, after: "endCursorFromPreviousPage") {
      edges {
        node { hash }
        cursor
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
```

**Backward Pagination:**

```graphql
# Get last page
query {
  accountByAddress(address: "GABC...") {
    transactions(last: 10) {
      edges {
        node { hash }
        cursor
      }
      pageInfo {
        hasPreviousPage
        startCursor
      }
    }
  }
}

# Get previous page
query {
  accountByAddress(address: "GABC...") {
    transactions(last: 10, before: "startCursorFromCurrentPage") {
      edges {
        node { hash }
        cursor
      }
      pageInfo {
        hasPreviousPage
        startCursor
      }
    }
  }
}
```

**PageInfo Fields:**
- `hasNextPage: Boolean!` - True if more items exist after the current page
- `hasPreviousPage: Boolean!` - True if more items exist before the current page
- `startCursor: String` - Cursor of the first item in the page
- `endCursor: String` - Cursor of the last item in the page

**Page Size Limits:**

Every connection in the schema — account-scoped (`Account.transactions`/`operations`/`stateChanges`/`balances`/`sep41Allowances`) and nested (`Transaction.operations`/`stateChanges`, `Operation.stateChanges`) — caps `first`/`last` at **100**. A page size above the cap is rejected with a `BAD_USER_INPUT` error rather than silently clamped, so callers get an explicit signal instead of a smaller-than-requested page.

## State Changes

State changes represent modifications to an account's state. The API uses an **interface-based design**: every state change implements the `BaseStateChange` interface, and each concrete type encodes one variant of the state — its exact `(category, reason)` pairs and its own typed fields. Select concrete-type fields via inline fragments; `category` and `reason` are on the interface for generic consumers.

**Interface fields (`BaseStateChange`, present on every type):**

| Field | Type | Notes |
|-------|------|-------|
| `category` | `StateChangeCategory!` | Category of account state affected |
| `reason` | `StateChangeReason!` | Why the change occurred |
| `ingestedAt` | `Time!` | When the indexer persisted the change |
| `ledgerCreatedAt` | `Time!` | Close time of the producing ledger |
| `ledgerNumber` | `UInt32!` | Sequence number of the producing ledger |
| `account` | `Account!` | Account whose state changed |
| `operation` | `Operation` | Producing operation; **non-null on every type except `BalanceChange`**, where it is null on transaction-fee rows (fees are per-transaction, not per-operation) |
| `transaction` | `Transaction!` | Producing transaction |

**Concrete Types:**

Each type below also exposes all interface fields. "Own fields" lists only what the type adds; `!` marks non-null.

| Type | `(category, reason)` pairs | Own fields |
|------|----------------------------|------------|
| `BalanceChange` | `(BALANCE, DEBIT)`, `(BALANCE, CREDIT)`, `(BALANCE, MINT)`, `(BALANCE, BURN)` | `tokenId: String!`, `amount: String!`, `toMuxedId: String` |
| `AccountCreatedChange` | `(ACCOUNT, CREATE)` | `funderAddress: String!` |
| `ContractDeployedChange` | `(ACCOUNT, CREATE)` | `deployerAddress: String!` |
| `AccountMergedChange` | `(ACCOUNT, MERGE)` | `destinationAddress: String!` |
| `SignerAddedChange` | `(SIGNER, ADD)` | `signerAddress: String!`, `newWeight: Int!` |
| `SignerUpdatedChange` | `(SIGNER, UPDATE)` | `signerAddress: String!`, `oldWeight: Int!`, `newWeight: Int!` |
| `SignerRemovedChange` | `(SIGNER, REMOVE)` | `signerAddress: String!`, `oldWeight: Int!` |
| `ThresholdChange` | `(SIGNATURE_THRESHOLD, LOW)`, `(SIGNATURE_THRESHOLD, MEDIUM)`, `(SIGNATURE_THRESHOLD, HIGH)` | `oldThreshold: Int!`, `newThreshold: Int!` |
| `AccountFlagsChange` | `(FLAGS, SET)`, `(FLAGS, CLEAR)` | `flags: [AccountFlag!]!` |
| `HomeDomainChange` | `(METADATA, HOME_DOMAIN)` | `oldHomeDomain: String!`, `newHomeDomain: String!` |
| `DataEntryChange` | `(METADATA, DATA_ENTRY)` | `name: String!`, `oldValue: String`, `newValue: String` |
| `AllowanceChange` | `(ALLOWANCE, UPDATE)` | `tokenId: String!`, `spender: String!`, `amount: String!`, `expirationLedger: UInt32!` |
| `TrustlineAddedChange` | `(TRUSTLINE, ADD)` | `tokenId: String`, `liquidityPoolId: String`, `limit: String!` |
| `TrustlineUpdatedChange` | `(TRUSTLINE, UPDATE)` | `tokenId: String`, `liquidityPoolId: String`, `oldLimit: String!`, `newLimit: String!` |
| `TrustlineRemovedChange` | `(TRUSTLINE, REMOVE)` | `tokenId: String`, `liquidityPoolId: String` |
| `BalanceAuthorizationChange` | `(BALANCE_AUTHORIZATION, SET)`, `(BALANCE_AUTHORIZATION, CLEAR)` | `tokenId: String`, `liquidityPoolId: String`, `flags: [TrustlineFlag!]` |

Notes on the polymorphic fields:
- On `TrustlineAddedChange`/`TrustlineUpdatedChange`/`TrustlineRemovedChange`/`BalanceAuthorizationChange`, exactly one of `tokenId` / `liquidityPoolId` is set (asset trustline vs. pool-share trustline).
- On `BalanceAuthorizationChange`, `flags` is null for SAC contract-holder authorization (a plain boolean in the contract balance entry, so there are no trustline flags).

**State Change Categories** (`StateChangeCategory` enum):

| Category | Types |
|----------|-------|
| `BALANCE` | `BalanceChange` (operation-sourced movements and transaction fees) |
| `ACCOUNT` | `AccountCreatedChange`, `ContractDeployedChange`, `AccountMergedChange` |
| `SIGNER` | `SignerAddedChange`, `SignerUpdatedChange`, `SignerRemovedChange` |
| `SIGNATURE_THRESHOLD` | `ThresholdChange` |
| `METADATA` | `HomeDomainChange`, `DataEntryChange` |
| `ALLOWANCE` | `AllowanceChange` |
| `FLAGS` | `AccountFlagsChange` |
| `TRUSTLINE` | `TrustlineAddedChange`, `TrustlineUpdatedChange`, `TrustlineRemovedChange` |
| `BALANCE_AUTHORIZATION` | `BalanceAuthorizationChange` |

**State Change Reasons** (`StateChangeReason` enum) — each reason applies only to the categories noted:

| Reason | Applies to |
|--------|-----------|
| `CREATE` | ACCOUNT: account created or contract deployed |
| `MERGE` | ACCOUNT: account merged into another |
| `DEBIT` | BALANCE: value left the account (or a transaction-fee charge, which has a null `operation`) |
| `CREDIT` | BALANCE: value entered the account |
| `MINT` | BALANCE: tokens minted to the account |
| `BURN` | BALANCE: tokens burned from the account (including clawbacks) |
| `ADD` | SIGNER or TRUSTLINE: entry added |
| `REMOVE` | SIGNER or TRUSTLINE: entry removed |
| `UPDATE` | SIGNER or TRUSTLINE: entry updated; ALLOWANCE: SEP-41 allowance approved |
| `LOW` / `MEDIUM` / `HIGH` | SIGNATURE_THRESHOLD: which threshold changed |
| `HOME_DOMAIN` | METADATA: home domain changed |
| `DATA_ENTRY` | METADATA: data entry created, updated, or removed |
| `SET` | FLAGS or BALANCE_AUTHORIZATION: flags turned on |
| `CLEAR` | FLAGS or BALANCE_AUTHORIZATION: flags turned off |

**Flag Enum Values:**

`AccountFlag` (on `AccountFlagsChange.flags`):
- `AUTH_REQUIRED` - Holders of the account's assets must be authorized by the issuer
- `AUTH_REVOCABLE` - The issuer can revoke a holder's authorization
- `AUTH_IMMUTABLE` - The account's flags can never be changed again
- `AUTH_CLAWBACK_ENABLED` - The issuer can claw back its assets from holders

`TrustlineFlag` (on `BalanceAuthorizationChange.flags`):
- `AUTHORIZED` - The holder is fully authorized to transact the asset
- `AUTHORIZED_TO_MAINTAIN_LIABILITIES` - The holder may only maintain existing liabilities
- `CLAWBACK_ENABLED` - The issuer can claw the asset back from this trustline

**Example: Querying balance changes:**

```graphql
query GetBalanceChanges {
  accountByAddress(address: "GABC...") {
    stateChanges(filter: { category: BALANCE }, first: 100) {
      edges {
        node {
          category
          reason
          operation {
            id
          }

          # BalanceChange covers both operation-sourced movements and transaction
          # fees; on a fee row the `operation` selected above is null.
          ... on BalanceChange {
            tokenId
            amount
            account {
              address
            }
            transaction {
              hash
            }
          }
        }
      }
    }
  }
}
```

**Example: Querying mixed state change types:**

```graphql
query GetAccountStateChanges {
  accountByAddress(address: "GABC...") {
    stateChanges(first: 50) {
      edges {
        node {
          category
          reason
          ledgerNumber

          ... on BalanceChange {
            tokenId
            amount
          }

          ... on SignerUpdatedChange {
            signerAddress
            oldWeight
            newWeight
          }

          ... on TrustlineAddedChange {
            tokenId
            liquidityPoolId
            limit
          }

          ... on AccountFlagsChange {
            flags
          }
        }
      }
    }
  }
}
```

## Error Handling

The GraphQL API returns structured errors with an `extensions.code` field so clients can branch on error type without parsing the message.

**Error Response Format:**

```json
{
  "errors": [
    {
      "message": "invalid transaction hash format: must be a 64-character hex string",
      "extensions": {
        "code": "INVALID_TRANSACTION_HASH",
        "hash": "not-a-hash"
      },
      "path": ["transactionByHash"]
    }
  ],
  "data": null
}
```

**Error with Additional Context (Extensions):**

Some errors include additional context in the `extensions` field. For example, when an invalid address is provided:

```json
{
  "errors": [
    {
      "message": "invalid address format: must be a valid Stellar account (G...) or contract (C...) address",
      "extensions": {
        "code": "INVALID_ADDRESS",
        "address": "invalid-address"
      },
      "path": ["accountByAddress"]
    }
  ],
  "data": null
}
```

**Error Codes:**

| Error Code | Meaning |
|------------|---------|
| `BAD_USER_INPUT` | Client-correctable validation failure — an invalid pagination combination, a page size over the cap, or similar |
| `INVALID_ADDRESS` | The provided address is not a valid Stellar account (G...) or contract (C...) address |
| `INVALID_TRANSACTION_HASH` | The provided hash is not a 64-character hex string |
| `INTERNAL_ERROR` | A sanitized, generic failure from a specific resolver (e.g. the balances query) that already masks its own internal detail |
| `GRAPHQL_VALIDATION_FAILED` | The query failed schema validation (unknown field, bad argument, ...) |
| `GRAPHQL_PARSE_FAILED` | The query failed to parse |
| `COMPLEXITY_LIMIT_EXCEEDED` | The query's computed complexity exceeds the configured limit (see [Complexity Limits](#2-complexity-limits)) |
| `QUERY_TOO_DEEP` | The query's selection set nests deeper than the depth limit (see [Depth Limit](#3-depth-limit)) |
| `INTERNAL_SERVER_ERROR` | An unmasked internal failure — see below |

**Error Masking:**

Any error surfaced without one of the codes above is treated as an internal failure: the server logs the underlying error server-side and returns a generic `"internal server error"` message under `INTERNAL_SERVER_ERROR` instead of forwarding the raw error text to the client. This prevents a bare SQL driver error, a wrapped Go error, or other internal detail (query text, table/column names, etc.) from leaking to callers.

## Performance Features

The GraphQL API is optimized for production use with several performance enhancements:

### 1. DataLoader Pattern

Prevents N+1 query problems by batching and caching database requests. When querying related data across multiple nodes, DataLoaders automatically:
- Batch multiple requests into a single database query
- Cache results within a single request
- Reduce database roundtrips

For example, without dataloader, the following query would:
1. First fetch first 5 transactions
2. For each transaction, make an individual DB call to get the operations

However, with dataloader, the individual DB calls to get operations get converted to a single DB call for all batched operations for all transactions.
```graphql
query ListTransactions {
  accountByAddress(address: "GABC...") {
    transactions(first: 5, after: "cursor123") {
      edges {
        node {
          hash
        }
        # operations is inlined on the edge (AccountTransactionEdge), so a full
        # account-history page resolves in one batched query.
        operations {
          id
          type
          operationXdr
          ledgerNumber
          ledgerCreatedAt
        }
      }
    }
  }
}
```

### 2. Complexity Limits

Queries are limited by a configurable complexity score to prevent resource exhaustion. Complexity is calculated based on:
- Number of fields requested
- Pagination parameters (`first`/`last` multiplied by field complexity)

The complexity limit is set via the `--graphql-complexity-limit` flag (see `cmd/utils/global_options.go` for the built-in default) or the `GRAPHQL_COMPLEXITY_LIMIT` environment variable; deployments commonly override the built-in default to fit their own query patterns.

If a query exceeds the limit, you'll receive an error:
```json
{
  "errors": [
    {
      "message": "operation has complexity 1100, which exceeds the limit of 1000",
      "extensions": {
        "code": "COMPLEXITY_LIMIT_EXCEEDED"
      }
    }
  ]
}
```

### 3. Depth Limit

Independent of the complexity limit, queries are also limited by selection-set nesting depth (default: **15**). A chain of `first: 1` connections costs only ~1 in complexity per level regardless of how deep it goes, so depth is capped separately to reject pathologically deep queries that would otherwise slip under the complexity budget. Fragment spreads are resolved against the query's fragments before measuring depth, so nesting can't be hidden behind a fragment indirection.

If a query exceeds the limit, you'll receive an error with code `QUERY_TOO_DEEP`:
```json
{
  "errors": [
    {
      "message": "operation has depth 18, which exceeds the limit of 15",
      "extensions": {
        "code": "QUERY_TOO_DEEP"
      }
    }
  ]
}
```

### 4. Request Timeout

Each request's context is bounded to **30 seconds**. A resolver or database query still running when the timeout elapses is canceled and the request fails; this bounds worst-case resource usage per request independent of the complexity and depth limits.

### 5. Automatic Persisted Queries (APQ)

Reduces bandwidth by allowing clients to send query hashes instead of full query strings:

```bash
# First request: Send full query with hash
POST /graphql
{
  "query": "{ accountByAddress(address: \"GABC...\") { transactions(first: 10) { ... } } }",
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "abc123..."
    }
  }
}

# Subsequent requests: Send only hash
POST /graphql
{
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "abc123..."
    }
  }
}
```

### 6. Field Selection Optimization

The API only queries database columns that are requested in the GraphQL query, reducing unnecessary data transfer:

```graphql
# Only queries 'hash' and 'ledgerNumber' columns
query {
  accountByAddress(address: "GABC...") {
    transactions(first: 10) {
      edges {
        node {
          hash
          ledgerNumber
        }
      }
    }
  }
}
```

**Best Practices:**

1. **Request only needed fields** - Don't query heavy resolver-backed fields like `operationXdr` unless required
2. **Use reasonable pagination limits** - Start with `first: 10-50` and increase if needed
3. **Leverage DataLoaders** - Query related data in a single request rather than multiple sequential queries
4. **Consider APQ for production** - Reduces bandwidth for frequently-executed queries
5. **Monitor complexity** - Break complex queries into multiple smaller queries if needed
