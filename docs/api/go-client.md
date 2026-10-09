# Go client

For Go developers who call wallet-backend from a Go service. After reading it you can add the client, create it with or without request signing, fetch history and balances page by page, and tell the error types apart.

## Contents

- [Install](#install)
- [Create a client](#create-a-client)
- [Methods](#methods)
- [Paginate](#paginate)
- [Query options](#query-options)
- [Errors](#errors)
- [Where in the code](#where-in-the-code)

The client wraps the queries described in the [GraphQL API guide](graphql.md). Field meanings are in the [schema reference](schema.md).

## Install

1. Add the module:

   ```bash
   go get github.com/stellar/wallet-backend/pkg/wbclient@<VERSION>
   ```

   Use a release tag such as `v1.0.0`. Tags are listed on [GitHub Releases](https://github.com/stellar/wallet-backend/releases).

   Verify: `go.mod` lists `github.com/stellar/wallet-backend <VERSION>`.

2. Build your module:

   ```bash
   go build ./...
   ```

   Verify: the build succeeds.

`pkg/wbclient` has no `go.mod` of its own. It is a package inside the `github.com/stellar/wallet-backend` module, so your `go.mod` requires that whole module and its dependency graph. That module declares `go 1.25.9`, so your build needs Go 1.25.9 or later.

Import paths:

| Package | Path |
|---|---|
| Client | `github.com/stellar/wallet-backend/pkg/wbclient` |
| Request signing | `github.com/stellar/wallet-backend/pkg/wbclient/auth` |
| Result types | `github.com/stellar/wallet-backend/pkg/wbclient/types` |

## Create a client

Pass the server's base URL without a path. The client appends `/graphql/query`.

Without auth, for a server that does not set `CLIENT_AUTH_PUBLIC_KEYS`:

```go
client := wbclient.NewClient("http://localhost:8001", nil)

tx, err := client.GetTransactionByHash(ctx, "<TX_HASH>")
if err != nil {
    return err
}
fmt.Println(tx.Hash, tx.LedgerNumber, tx.ResultCode)
```

With auth, sign every request with a Stellar key whose public key is in the server's `CLIENT_AUTH_PUBLIC_KEYS`:

```go
jwtManager, err := auth.NewJWTManager("<CLIENT_SECRET_SEED>", "<CLIENT_PUBLIC_KEY>", 0)
if err != nil {
    return err
}
client := wbclient.NewClient("http://localhost:8001", auth.NewHTTPRequestSigner(jwtManager))
```

`auth.NewJWTTokenGenerator("<CLIENT_SECRET_SEED>")` also works as the signer's generator. It derives the public key from the seed.

Defaults set by the client:

| Setting | Value | Change it |
|---|---|---|
| HTTP timeout | 30 s | Replace `client.HTTPClient` |
| Token lifetime | 5 s from signing | Fixed. The server accepts up to `CLIENT_AUTH_MAX_TIMEOUT_SECONDS`, default 15. |
| Method | `POST` with `Content-Type: application/json` | Fixed |

```go
client.HTTPClient = &http.Client{Timeout: 10 * time.Second}
```

Verify: call `GetAccountByAddress` with any valid `G...` address. It returns the address back with no error. Against a server with auth on and a key it does not trust, the call fails with `unexpected statusCode=401`.

## Methods

All methods take a `context.Context` first.

| Method | Returns | Paging, time, filter, fields |
|---|---|---|
| `GetTransactionByHash(ctx, hash, opts...)` | `*types.GraphQLTransaction` | `QueryOptions.TransactionFields` |
| `GetAccountByAddress(ctx, address, opts...)` | `*types.Account` | `QueryOptions.AccountFields` |
| `GetOperationByID(ctx, id, opts...)` | `*types.Operation` | `QueryOptions.OperationFields` |
| `GetAccountTransactions(ctx, address, timeRange, page, opts...)` | `*types.TransactionConnection` | `*TimeRange`, `*Page`, `TransactionFields` |
| `GetAccountTransactionsWithOpsAndStateChanges(ctx, address, timeRange, page)` | `*types.AccountTransactionConnection` | `*TimeRange`, `*Page`. Each edge carries the account's operations and state changes in that transaction. |
| `GetAccountOperations(ctx, address, timeRange, page, opts...)` | `*types.OperationConnection` | `*TimeRange`, `*Page`, `OperationFields` |
| `GetAccountStateChanges(ctx, address, filter, timeRange, page)` | `*types.StateChangeConnection` | `*StateChangeFilter`, `*TimeRange`, `*Page` |
| `GetTransactionOperations(ctx, hash, page, opts...)` | `*types.OperationConnection` | `*Page`, `OperationFields` |
| `GetTransactionStateChanges(ctx, hash, page)` | `*types.StateChangeConnection` | `*Page` |
| `GetOperationStateChanges(ctx, id, page)` | `*types.StateChangeConnection` | `*Page` |
| `GetAccountBalances(ctx, address, page)` | `*types.BalanceConnection` | `*Page` |
| `GetAllAccountBalances(ctx, address)` | `[]types.Balance` | Walks every page, 100 balances per request |

A nil `*Page`, `*TimeRange`, or `*StateChangeFilter` sends no arguments for it. The server then returns its default page of 50, with no time bounds and no filter.

The client has no method for `Account.sep41Allowances`. Query it over HTTP as shown in the [GraphQL API guide](graphql.md#balances).

Parameter types:

| Type | Fields | Rules |
|---|---|---|
| `wbclient.Page` | `First`, `After`, `Last`, `Before`, all pointers | Same as the server: `First` with `After`, or `Last` with `Before`. Values from 1 to 100. |
| `wbclient.TimeRange` | `Since`, `Until`, `*time.Time` | Ledger close time, both inclusive. A nil bound is open. |
| `wbclient.StateChangeFilter` | `TransactionHash`, `OperationID`, `Category`, `Reason` | Nil fields are skipped. Set fields must all match. |

State-change nodes come back as `types.StateChangeNode`. Switch on the concrete type, such as `*types.BalanceChange`, to read type-specific fields.

## Paginate

Forward through an account's transactions, oldest first:

```go
first := int32(100)
var after *string
for {
    conn, err := client.GetAccountTransactions(ctx, "<G_ADDRESS>", nil, &wbclient.Page{First: &first, After: after})
    if err != nil {
        return err
    }
    for _, edge := range conn.Edges {
        fmt.Println(edge.Node.Hash, edge.Node.LedgerCreatedAt)
    }
    if !conn.PageInfo.HasNextPage {
        break
    }
    after = conn.PageInfo.EndCursor
}
```

The most recent operations from the last 24 hours, newest page first:

```go
last := int32(50)
since := time.Now().Add(-24 * time.Hour)
conn, err := client.GetAccountOperations(ctx, "<G_ADDRESS>", &wbclient.TimeRange{Since: &since}, &wbclient.Page{Last: &last})
if err != nil {
    return err
}
// Edges inside the page are oldest first. For the page before it, pass
// &wbclient.Page{Last: &last, Before: conn.PageInfo.StartCursor}.
for _, edge := range conn.Edges {
    fmt.Println(edge.Node.ID, edge.Node.Type, edge.Node.Successful)
}
```

Incoming balance movements for an account:

```go
category := types.StateChangeCategoryBalance
reason := types.StateChangeReasonCredit
conn, err := client.GetAccountStateChanges(ctx, "<G_ADDRESS>", &wbclient.StateChangeFilter{Category: &category, Reason: &reason}, nil, nil)
if err != nil {
    return err
}
for _, edge := range conn.Edges {
    if bc, ok := edge.Node.(*types.BalanceChange); ok {
        fmt.Println(bc.LedgerCreatedAt, bc.TokenID, bc.Amount)
    }
}
```

Every balance an account holds, across all pages:

```go
balances, err := client.GetAllAccountBalances(ctx, "<G_ADDRESS>")
if err != nil {
    return err
}
for _, b := range balances {
    switch v := b.(type) {
    case *types.NativeBalance:
        fmt.Println("XLM", v.BalanceValue, "minimum", v.MinimumBalance)
    case *types.TrustlineBalance:
        fmt.Println(v.Code, v.Issuer, v.BalanceValue)
    case *types.SACBalance:
        fmt.Println(v.Code, v.BalanceValue, "decimals", v.Decimals)
    case *types.SEP41Balance:
        fmt.Println(v.TokenID, v.BalanceValue, "decimals", v.Decimals)
    case *types.LiquidityPoolBalance:
        fmt.Println(v.TokenID, v.BalanceValue, v.Reserves)
    }
}
```

`GetAllAccountBalances` returns an empty, non-nil slice for an account with no balances. It stops with an error if the server reports more pages without a cursor, or returns the same cursor twice.

For one page at a time, call `GetAccountBalances` with a `Page`:

```go
first := int32(20)
conn, err := client.GetAccountBalances(ctx, "<G_ADDRESS>", &wbclient.Page{First: &first})
if err != nil {
    return err
}
for _, b := range conn.Balances() {
    fmt.Println(b.GetTokenType(), b.GetTokenID(), b.GetBalance())
}
```

## Query options

`QueryOptions` replaces the default field list for transactions, operations, or accounts. Pass the GraphQL field names you want. Fields you leave out stay at their zero value in the result struct.

```go
opts := &wbclient.QueryOptions{TransactionFields: []string{"hash", "ledgerCreatedAt"}}
conn, err := client.GetAccountTransactions(ctx, "<G_ADDRESS>", nil, nil, opts)
if err != nil {
    return err
}
for _, edge := range conn.Edges {
    fmt.Println(edge.Node.Hash, edge.Node.LedgerCreatedAt)
}
```

| Option | Default fields |
|---|---|
| `TransactionFields` | `hash`, `resultCode`, `feeCharged`, `ledgerNumber`, `ledgerCreatedAt`, `isFeeBump`, `ingestedAt` |
| `OperationFields` | `id`, `type`, `operationXdr`, `resultCode`, `successful`, `ledgerNumber`, `ledgerCreatedAt`, `ingestedAt` |
| `AccountFields` | `address` |

Use scalar fields from those defaults only. The result structs have no slots for other fields, and the client puts the names into the query text as given. A name the schema does not know fails the whole request.

State-change and balance queries always select every type's fields. They take no `QueryOptions`.

## Errors

| Situation | What the method returns | How to detect it |
|---|---|---|
| Invalid `Page` combination, such as `First` with `Last` | Error before any request is sent | Message starts with `building pagination variables:` |
| Network failure or timeout | Wrapped `net/http` error | A cancelled or expired `ctx` matches `errors.Is(err, context.Canceled)` or `context.DeadlineExceeded` |
| HTTP status 400 or higher | `unexpected statusCode=<CODE>, body=<BODY>` | A plain error. Read the status from the message. |
| HTTP 200 with `errors` in the body | Wrapped `wbclient.GraphQLErrors` | `errors.As(err, &gqlErrs)`, then `gqlErrs[i].Extensions["code"]` |
| HTTP 200 with a null result and no errors | `wbclient.ErrTransactionNotFound`, `ErrOperationNotFound`, or `ErrAccountNotFound` | `errors.Is` |
| Response that breaks the schema's non-null rules | Unmarshal error | Message names the connection or edge |

Parse and validation failures arrive as HTTP 422, so they come back as the `unexpected statusCode` error, not as `GraphQLErrors`. A 401 from the auth check does the same.

The server pairs a null `transactionByHash` or `operationById` with an `INTERNAL_SERVER_ERROR` entry. A missing transaction or operation therefore returns `GraphQLErrors` with that code, not `ErrTransactionNotFound` or `ErrOperationNotFound`. `accountByAddress` returns an account for every valid address, so a valid address never produces `ErrAccountNotFound`. An invalid one returns `GraphQLErrors` with `INVALID_ADDRESS`.

```go
_, err := client.GetTransactionByHash(ctx, "<TX_HASH>")
var gqlErrs wbclient.GraphQLErrors
if errors.As(err, &gqlErrs) {
    for _, e := range gqlErrs {
        switch e.Extensions["code"] {
        case "INVALID_TRANSACTION_HASH":
            // The hash is not 64 hex characters.
        case "INTERNAL_SERVER_ERROR":
            // Not indexed, or a server-side failure. The server does not say which.
        }
    }
}
```

`GraphQLErrors.Error()` joins every entry as `CODE: message`, separated by a semicolon and a space.

## Where in the code

| Path | Role |
|---|---|
| `pkg/wbclient/client.go` | `Client`, `NewClient`, every method, error handling |
| `pkg/wbclient/options.go` | `Page`, `TimeRange`, `StateChangeFilter`, client-side pagination checks |
| `pkg/wbclient/queries.go` | Query strings and default field sets |
| `pkg/wbclient/types/types.go` | Result structs, balance types, connections |
| `pkg/wbclient/types/statechange.go` | State-change node types and their decoding |
| `pkg/wbclient/auth/jwt_manager.go` | `NewJWTManager`, `NewJWTTokenGenerator` |
| `pkg/wbclient/auth/jwt_http_signer_verifier.go` | `NewHTTPRequestSigner` and the request binding |
| `pkg/wbclient/client_test.go` | Usage against a test server |
