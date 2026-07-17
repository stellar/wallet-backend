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
    "query": "{ transactionByHash(hash: \"abc123...\") { hash ledgerNumber envelopeXdr } }"
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
    envelopeXdr
    feeCharged
    resultCode
    metaXdr
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
          operationType
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
          operationType
          operationXdr
        }
      }
    }

    # Related state changes with optional filtering
    stateChanges(
      filter: {
        transactionHash: "abc123..."  # Filter by transaction hash
        operationId: 12345            # Filter by operation ID
        category: "BALANCE"           # Filter by state change category
        reason: "CREDIT"              # Filter by state change reason
      }
      first: 50
    ) {
      edges {
        node {
          ... on StandardBalanceChange {
            type
            reason
            tokenId
            amount
            ledgerNumber
          }
          ... on SignerChange {
            type
            reason
            signerAddress
            signerWeights
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
| `category` | `String` | Filter by state change category (e.g., `BALANCE`, `ACCOUNT`, `SIGNER`, `TRUSTLINE`, `RESERVES`) |
| `reason` | `String` | Filter by state change reason (e.g., `CREDIT`, `DEBIT`, `CREATE`, `MERGE`, `ADD`, `REMOVE`) |

### 3. Get Operation by ID

Retrieve a specific operation by its ID.

```graphql
query GetOperation {
  operationById(id: 12345) {
    id
    operationType
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
          ... on StandardBalanceChange {
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

The `operationType` field supports all Stellar operation types:
- `CREATE_ACCOUNT`, `PAYMENT`, `PATH_PAYMENT_STRICT_RECEIVE`, `PATH_PAYMENT_STRICT_SEND`
- `MANAGE_SELL_OFFER`, `CREATE_PASSIVE_SELL_OFFER`, `MANAGE_BUY_OFFER`
- `SET_OPTIONS`, `CHANGE_TRUST`, `ALLOW_TRUST`, `ACCOUNT_MERGE`
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
            lastModifiedLedger
          }

          ... on TrustlineBalance {
            code
            issuer
            type
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

| Type | Description | Key Fields |
|------|-------------|------------|
| `NativeBalance` | XLM (native asset) | `balance`, `tokenId`, `tokenType`, `minimumBalance`, `buyingLiabilities`, `sellingLiabilities`, `lastModifiedLedger` |
| `TrustlineBalance` | Classic Stellar trustlines | `code`, `issuer`, `limit`, `isAuthorized`, liabilities |
| `SACBalance` | Stellar Asset Contract (wrapped classic assets) | `code`, `issuer`, `decimals`, `isAuthorized`, `isClawbackEnabled` |

**Common Fields (all balance types):**
- `balance: String!` - Current balance amount
- `tokenId: String!` - Contract ID (C...) for the token
- `tokenType: TokenType!` - One of: `NATIVE`, `CLASSIC`, `SAC`

**NativeBalance-specific Fields:**
- `minimumBalance: String!` - Minimum balance required for reserves
- `buyingLiabilities: String!` - Liabilities from buy offers
- `sellingLiabilities: String!` - Liabilities from sell offers
- `lastModifiedLedger: UInt32!` - Ledger number when account was last modified

**Token Types:**
- `NATIVE` - XLM (Stellar's native asset)
- `CLASSIC` - Classic Stellar trustline assets
- `SAC` - Stellar Asset Contract (classic assets wrapped for Soroban)

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

State changes represent modifications to an account's state. The API uses an **interface-based design** where all state changes implement the `BaseStateChange` interface.

**State Change Categories:**

| Category | Types | Description |
|----------|-------|-------------|
| `BALANCE` | `StandardBalanceChange` | Changes to an account's balance (payments, mints, burns, clawbacks) |
| `ACCOUNT` | `AccountChange` | Account creation or merge operations |
| `SIGNER` | `SignerChange` | Signer additions/removals |
| `SIGNATURE_THRESHOLD` | `SignerThresholdsChange` | Threshold changes (low/medium/high) |
| `METADATA` | `MetadataChange` | Account metadata/data entries |
| `FLAGS` | `FlagsChange` | Account flag changes |
| `TRUSTLINE` | `TrustlineChange` | Trustline limit changes |
| `RESERVES` | `ReservesChange` | Sponsorship relationships for an account's base reserves |
| `BALANCE_AUTHORIZATION` | `BalanceAuthorizationChange` | Balance authorization for trustlines and contract accounts |

**State Change Reasons:**

Reasons provide context for why a state change occurred:
- `CREATE`, `MERGE` - Account lifecycle
- `DEBIT`, `CREDIT` - Balance decreases/increases
- `MINT`, `BURN` - Token creation/destruction
- `ADD`, `REMOVE` - Adding/removing signers, trustlines
- `UPDATE` - Updates to the state. Could be account flags, reserves etc...
- `LOW`, `MEDIUM`, `HIGH` - Threshold levels
- `HOME_DOMAIN` - Home domain changes
- `SET`, `CLEAR` - Setting/clearing values
- `DATA_ENTRY` - Data entry operations
- `SPONSOR`, `UNSPONSOR` - Sponsorship relationship changes

**State Change Type-Specific Fields:**

| Type | Fields | Description |
|------|--------|-------------|
| `StandardBalanceChange` | `tokenId`, `amount` | Token identifier and change amount |
| `AccountChange` | `funderAddress` | Address that funded the account (for CREATE reason) |
| `SignerChange` | `signerAddress`, `signerWeights` | Signer address and weight changes |
| `SignerThresholdsChange` | `thresholds` | Threshold configuration changes |
| `MetadataChange` | `keyValue` | Metadata key-value changes |
| `FlagsChange` | `flags` | Array of flag names |
| `TrustlineChange` | `tokenId`, `limit`, `keyValue` | Trustline configuration and additional data |
| `ReservesChange` | `sponsoredAddress`, `sponsorAddress`, `keyValue` | Reserve sponsorship and additional data |
| `BalanceAuthorizationChange` | `tokenId`, `flags`, `keyValue` | Balance authorization flags |

**Example: Querying Specific State Change Types:**

```graphql
query GetBalanceChanges {
  accountByAddress(address: "GABC...") {
    stateChanges(first: 100) {
      edges {
        node {
          type
          reason

          ... on StandardBalanceChange {
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

**Example: Querying state changes for a specific account:**

```graphql
query GetAccountStateChanges {
  accountByAddress(address: "GABC...") {
    stateChanges(first: 50) {
      edges {
        node {
          type
          reason
          ledgerNumber

          ... on StandardBalanceChange {
            tokenId
            amount
          }

          ... on SignerChange {
            signerAddress
            signerWeights
          }
        }
      }
    }
  }
}
```

**Field Structure Details:**

Several state change fields return JSON-formatted strings containing old and new values. Here are the structures:

1. **signerWeights** (SignerChange):
   - For new signers: `{"new": 1}`
   - For updated signers: `{"old": 1, "new": 2}`
   - For removed signers: `{"old": 1}`

2. **thresholds** (SignerThresholdsChange):
   - Format: `{"old": "10", "new": "20"}`
   - Values represent threshold weights as strings

3. **limit** (TrustlineChange):
   - For new trustlines: `{"limit": {"new": "1000"}}`
   - For updated trustlines: `{"limit": {"old": "1000", "new": "2000"}}`

4. **keyValue** (MetadataChange, TrustlineChange, ReservesChange, and BalanceAuthorizationChange):
   - For MetadataChange (home domain): `{"home_domain": "example.com"}`
   - For MetadataChange (data entry): `{"entry_name": {"old": "base64OldValue", "new": "base64NewValue"}}`
   - For BalanceAuthorizationChange (liquidity pools): `{"liquidity_pool_id": "pool_id"}`
   - For TrustlineChange: Additional trustline configuration data
   - For ReservesChange: Additional reserve data

5. **flags** (FlagsChange and BalanceAuthorizationChange):
   - Array of flag names that were set or cleared
   - See Flag Values Reference below for possible values

**Flag Values Reference:**

*Account Flags (FlagsChange):*
- `auth_required_flag` - Authorization required for accounts to hold assets
- `auth_revocable_flag` - Issuer can revoke authorization
- `auth_immutable_flag` - Authorization flags cannot be changed
- `auth_clawback_enabled_flag` - Issuer can clawback assets

*Trustline/Balance Authorization Flags (BalanceAuthorizationChange):*
- `authorized` - Trustline is authorized to hold assets
- `authorized_to_maintain_liabilities` - Can maintain liabilities but not increase balance
- `clawback_enabled_flag` - Asset issuer can clawback this balance
- `auth_revocable_flag` - Authorization can be revoked
- `auth_immutable_flag` - Authorization flags are immutable

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
          operations {
            id
            operationType
            operationXdr
            ledgerNumber
            ledgerCreatedAt
          }
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

1. **Request only needed fields** - Don't query `envelopeXdr`, `metaXdr` unless required
2. **Use reasonable pagination limits** - Start with `first: 10-50` and increase if needed
3. **Leverage DataLoaders** - Query related data in a single request rather than multiple sequential queries
4. **Consider APQ for production** - Reduces bandwidth for frequently-executed queries
5. **Monitor complexity** - Break complex queries into multiple smaller queries if needed
