---
description: JWT/Ed25519 auth flow, client library usage, and Soroban contract invocation auth
type: reference
subsystem: auth
areas: [auth, jwt, ed25519, soroban]
status: current
vault: docs/knowledge
---

# Authentication Architecture

The wallet-backend uses two independent authentication systems:

1. **HTTP request authentication** — JWT/Ed25519-based middleware that gates the GraphQL API. Clients sign every HTTP request with a per-request JWT; the server verifies the Ed25519 signature against a configured set of public keys.
2. **Soroban contract invocation auth** — XDR-level authorization entries required when invoking smart contracts. Distinct from HTTP auth; operates on transaction envelopes.

## High-Level Overview

```mermaid
flowchart TD
    Client["External Client"]
    Client -->|"Bearer <JWT>"| Middleware["AuthenticationMiddleware"]
    Middleware -->|verified| GraphQL["/graphql/query handler"]
    Middleware -->|"401 Unauthorized"| Rejected["Request rejected"]

    GraphQL -->|"InvokeHostFunction op"| TxService["TransactionService.BuildAndSign()"]
    TxService -->|"adjustParamsForSoroban()"| SorobanAuth["sorobanauth.CheckForForbiddenSigners()"]
    SorobanAuth -->|"ErrForbiddenSigner"| GraphQLErr["FORBIDDEN_SIGNER error"]

    style Middleware fill:#e1f5fe
    style SorobanAuth fill:#fff3e0
```

**Key source files:**
- `internal/serve/middleware/middleware.go` — `AuthenticationMiddleware`
- `internal/serve/serve.go` — wiring, conditional auth enable/disable
- `pkg/wbclient/auth/` — JWT generation, parsing, HTTP signing/verifying
- `pkg/sorobanauth/sorobanauth.go` — `AuthSigner`, `CheckForForbiddenSigners`

---

## Auth Flow (Server-Side Middleware)

### Request Verification Pipeline

Every authenticated request passes through `AuthenticationMiddleware` → `JWTHTTPSignerVerifier.VerifyHTTPRequest()` → `MultiJWTTokenParser.ParseJWT()`.

```mermaid
flowchart TD
    Request["HTTP Request"] --> ExtractHeader["Extract Authorization header"]
    ExtractHeader --> CheckBearer{"Starts with 'Bearer '?"}
    CheckBearer -->|No| ErrUnauth1["ErrUnauthorized → 401"]
    CheckBearer -->|Yes| ReadBody["io.LimitReader body\n(max DefaultMaxBodySizeBytes = 100KB)"]
    ReadBody --> BuildMethodPath["methodAndPath = 'METHOD /path'"]
    BuildMethodPath --> ParseUnverified["claims.DecodeTokenString()\n(ParseUnverified — no sig check)"]
    ParseUnverified --> LookupParser["Look up parser by claims.Subject\n(Stellar public key from sub claim)"]
    LookupParser --> KeyKnown{"Subject in\njwtParsers map?"}
    KeyKnown -->|No| ErrUnauth2["ErrUnauthorized → 401"]
    KeyKnown -->|Yes| ValidateClaims["claims.Validate(methodAndPath, body, MaxTimeout)"]
    ValidateClaims --> ClaimsOK{"All checks\npass?"}
    ClaimsOK -->|No| ErrUnauth3["ErrUnauthorized → 401"]
    ClaimsOK -->|Yes| VerifySig["jwtgo.ParseWithClaims()\ned25519.Verify signature"]
    VerifySig --> SigOK{"Signature\nvalid?"}
    SigOK -->|Expired| ExpiredErr["ExpiredTokenError → metrics.IncSignatureVerificationExpired + 401"]
    SigOK -->|Invalid| ErrUnauth4["ErrUnauthorized → 401"]
    SigOK -->|Valid| PassThrough["next.ServeHTTP()\n(no context injection)"]

    style PassThrough fill:#e8f5e9
    style ErrUnauth1 fill:#ffebee
    style ErrUnauth2 fill:#ffebee
    style ErrUnauth3 fill:#ffebee
    style ErrUnauth4 fill:#ffebee
    style ExpiredErr fill:#fff3e0
```

### Multi-Key O(1) Routing

`MultiJWTTokenParser` supports multiple authorized clients without scanning every key:

```mermaid
flowchart LR
    Token["JWT token"] --> Decode["ParseUnverified\nextract sub claim"]
    Decode --> SubjectKey["claims.Subject\n(Stellar public key)"]
    SubjectKey --> MapLookup["jwtParsers map\nlookup O(1)"]
    MapLookup --> Found{"Found?"}
    Found -->|Yes| Delegate["Delegate to\nJWTManager.ParseJWT()"]
    Found -->|No| Reject["Error: subject not in\nexpected public keys"]

    style MapLookup fill:#e1f5fe
```

The Subject-as-key pattern lets the server verify the token with the exact right key without trying all registered keys. The public key itself acts as the lookup index.

### Error Classification

```mermaid
flowchart TD
    VerifyErr["VerifyHTTPRequest() error"] --> IsUnauth{"errors.Is\nErrUnauthorized?"}
    IsUnauth -->|No| Internal["InternalServerError → 500\n(appTracker.CaptureException)"]
    IsUnauth -->|Yes| IsExpired{"errors.As\nExpiredTokenError?"}
    IsExpired -->|Yes| Expired["metricsService.IncSignatureVerificationExpired\n+ httperror.Unauthorized → 401"]
    IsExpired -->|No| Unauth["httperror.Unauthorized → 401"]

    style Internal fill:#ffebee
    style Expired fill:#fff3e0
    style Unauth fill:#fff3e0
```

### Claims Validation Rules

`customClaims.Validate()` enforces all of these before the signature is checked:

| Check | Rule | Error |
|-------|------|-------|
| `exp` present | `ExpiresAt != nil` | "JWT expiration is not set" |
| `iat` present | `IssuedAt != nil` | "JWT issuance time is not set" |
| Expiry window (absolute) | `exp <= now + MaxTimeout` | "JWT expiration is too long" |
| Expiry window (relative) | `exp - iat <= MaxTimeout` | "difference between exp and iat is too long" |
| Subject format | `IsValidEd25519PublicKey(sub)` | "JWT subject is not a valid Stellar public key" |
| Method+path binding | `claims.MethodAndPath == "METHOD /path"` | "JWT method-and-path does not match" |
| Body hash | `SHA-256(body) == claims.bodyHash` | "JWT hashed body does not match" |

The body hash prevents replay attacks: a token signed for `POST /graphql/query` with body `X` cannot be reused for the same endpoint with body `Y`.

### JWT Claims Schema

| Field | JSON key | Type | Description |
|-------|----------|------|-------------|
| `BodyHash` | `bodyHash` | `string` | `hex(SHA-256(request body))` |
| `MethodAndPath` | `methodAndPath` | `string` | e.g. `"POST /graphql/query"` |
| `Subject` | `sub` | `string` | Stellar Ed25519 public key (G...) |
| `IssuedAt` | `iat` | `NumericDate` | Token creation time |
| `ExpiresAt` | `exp` | `NumericDate` | Token expiry time |

### Server Configuration

| Config field | CLI flag | Default | Description |
|---|---|---|---|
| `ClientAuthPublicKeys` | `--client-auth-public-keys` | (empty = disabled) | Comma-separated Stellar public keys |
| `ClientAuthMaxTimeoutSeconds` | `--client-auth-max-timeout-seconds` | `15` | Maximum `exp - iat` window |
| `ClientAuthMaxBodySizeBytes` | `--client-auth-max-body-size-bytes` | `102400` (100KB) | Body size cap for verification |

**Auth is optional:** if `ClientAuthPublicKeys` is empty, `RequestAuthVerifier` is `nil` and the middleware is not mounted. The `/health` and `/api-metrics` routes are always unauthenticated.

### Constants

| Constant | Value | Location |
|----------|-------|----------|
| `DefaultMaxTimeout` | `15s` | `pkg/wbclient/auth/jwt_manager.go` |
| `DefaultMaxBodySizeBytes` | `102400` (100KB) | `pkg/wbclient/auth/jwt_http_signer_verifier.go` |
| `ErrUnauthorized` | `errors.New("not authorized")` | `pkg/wbclient/auth/jwt_http_signer_verifier.go` |

---

## Client Library

The `pkg/wbclient` package provides a typed Go client that handles JWT signing transparently. `Client.request()` calls `SignHTTPRequest()` before every HTTP send.

### Request Signing Flow

```mermaid
sequenceDiagram
    participant App as Application code
    participant C as Client.request()
    participant S as JWTHTTPSignerVerifier.SignHTTPRequest()
    participant G as JWTManager.GenerateJWT()
    participant H as HTTP

    App->>C: executeGraphQL(query, variables)
    C->>C: json.Marshal(GraphQLRequest)
    C->>C: http.NewRequestWithContext(POST, baseURL+/graphql/query)
    C->>S: SignHTTPRequest(req, 5s)
    S->>S: io.ReadAll(req.Body)
    S->>S: methodAndPath = "POST /graphql/query"
    S->>G: GenerateJWT(methodAndPath, body, now+5s)
    G->>G: claims{bodyHash, methodAndPath, sub, iat, exp}
    G->>G: jwtgo.NewWithClaims(SigningMethodEdDSA, claims)
    G->>G: token.SignedString(ed25519.PrivKey)
    G-->>S: JWT string
    S->>S: req.Header.Set("Authorization", "Bearer <JWT>")
    S->>S: req.Body = io.NopCloser(bytes.NewBuffer(body)) ← restore body
    S-->>C: nil
    C->>H: HTTPClient.Do(req)
    H-->>C: *http.Response
```

The body is read and then restored (`req.Body = io.NopCloser(...)`) so the HTTP transport can read it again. The JWT expiry is hardcoded to `now + 5s` in `Client.request()`.

### Type Architecture

```mermaid
flowchart TD
    subgraph Interfaces
        Parser["JWTTokenParser\nParseJWT(token, methodAndPath, body)"]
        Generator["JWTTokenGenerator\nGenerateJWT(methodAndPath, body, expiresAt)"]
        Signer["HTTPRequestSigner\nSignHTTPRequest(req, timeout)"]
        Verifier["HTTPRequestVerifier\nVerifyHTTPRequest(req)"]
    end

    subgraph Implementations
        JWTManager["JWTManager\n(single key)"]
        Multi["MultiJWTTokenParser\n(N keys)"]
        SVJWT["JWTHTTPSignerVerifier"]
    end

    Parser -->|implemented by| JWTManager
    Parser -->|implemented by| Multi
    Generator -->|implemented by| JWTManager
    Signer -->|implemented by| SVJWT
    Verifier -->|implemented by| SVJWT

    SVJWT --> Parser
    SVJWT --> Generator

    style Interfaces fill:#e1f5fe
    style Implementations fill:#e8f5e9
```

### Constructors

| Constructor | Returns | Use case |
|---|---|---|
| `NewJWTManager(privKey, pubKey, timeout)` | `*JWTManager` | Both sign and verify with a single key |
| `NewJWTTokenParser(timeout, pubKey)` | `JWTTokenParser` | Server: verify tokens from one key |
| `NewMultiJWTTokenParser(timeout, pubKeys...)` | `JWTTokenParser` | Server: verify tokens from N keys |
| `NewJWTTokenGenerator(privKey)` | `JWTTokenGenerator` | Client: generate tokens |
| `NewHTTPRequestSigner(generator)` | `HTTPRequestSigner` | Client: sign HTTP requests |
| `NewHTTPRequestVerifier(parser, maxBodyBytes)` | `HTTPRequestVerifier` | Server: verify HTTP requests |
| `NewClient(baseURL, requestSigner)` | `*Client` | Full typed API client |

### Client Design Notes

- **Per-request JWTs, no session tokens.** Every call to `request()` generates a fresh JWT with a 5-second expiry window. There is no token caching, refresh, or session management.
- **Signer is optional.** `Client.request()` checks `if c.RequestSigner != nil` before signing; passing `nil` produces an unauthenticated client (suitable when server auth is disabled).
- **HTTP client timeout is 30s.** Distinct from the JWT 5s expiry window — the former is the network timeout, the latter is the auth freshness window.

---

## Soroban Auth

Soroban (Stellar's smart contract platform) requires *authorization entries* to be attached to contract invocation transactions. This is separate from HTTP authentication — it operates on XDR transaction envelopes and is needed when users invoke contracts that require account signatures.

### AuthorizeEntry Flow

`AuthSigner.AuthorizeEntry()` is not currently called in the production path — the server returns unsigned auth entries to the client via the `BuildTransaction` mutation, and the client is expected to sign them. `CheckForForbiddenSigners` is the active production use.

```mermaid
flowchart TD
    Input["SorobanAuthorizationEntry\n(from simulation response)"] --> CheckType{"credentials.Type ==\nSorobanCredentialsAddress?"}
    CheckType -->|No| UnsupportedErr["UnsupportedCredentialsTypeError"]
    CheckType -->|Yes| BuildSkeleton["Build skeleton entry:\npreserve RootInvocation\nset nonce + validUntilLedgerSeq\nSignature = empty ScVal"]
    BuildSkeleton --> BuildPreimage["HashIdPreimage{\nType: EnvelopeTypeSorobanAuthorization\n networkId, nonce, invocation, sigExpLedger\n}"]
    BuildPreimage --> MarshalBinary["preimage.MarshalBinary()"]
    MarshalBinary --> HashPayload["payload = hash.Hash(preimageBytes)\n[32]byte SHA-256"]
    HashPayload --> Sign["keypair.Full.Sign(payload[:])"]
    Sign --> BuildScVal["Build ScVal as ScvVec[\n  ScvMap{public_key: ScvBytes, signature: ScvBytes}\n]"]
    BuildScVal --> Attach["addrAuth.Signature = scSignature"]
    Attach --> Return["Return authorized\nSorobanAuthorizationEntry"]

    style Return fill:#e8f5e9
    style UnsupportedErr fill:#ffebee
```

### CheckForForbiddenSigners Decision Tree

This is the active production guard in `adjustParamsForSoroban()`. It prevents channel accounts from being used as signers in Soroban auth entries, which would be a security violation (channel accounts are internal infrastructure, not user accounts).

```mermaid
flowchart TD
    Start["CheckForForbiddenSigners(\n  simulationResults,\n  opSourceAccount,\n  forbiddenSigners...\n)"] --> IterResults["Iterate simulationResponseResults"]
    IterResults --> IterAuth["Iterate auth entries in each result"]
    IterAuth --> CredType{"auth.Credentials.Type?"}

    CredType -->|SourceAccount| SACheck{"opSourceAccount in\nforbiddenSigners ∪ empty?"}
    SACheck -->|Yes| ErrForbidden1["ErrForbiddenSigner"]
    SACheck -->|No| NextAuth["Next auth entry"]

    CredType -->|Address| GetSigner["auth.Credentials.Address.Address.String()"]
    GetSigner --> AddrCheck{"authEntrySigner in\nforbiddenSigners?"}
    AddrCheck -->|Yes| ErrForbidden2["ErrForbiddenSigner"]
    AddrCheck -->|No| NextAuth

    CredType -->|Other| UnsupportedType["error: unsupported auth entry type"]
    NextAuth --> IterAuth
    IterAuth -->|Done| OK["return nil"]

    style OK fill:#e8f5e9
    style ErrForbidden1 fill:#ffebee
    style ErrForbidden2 fill:#ffebee
    style UnsupportedType fill:#ffebee
```

### Credentials Type Comparison

| Type | When used | Signer identity | Check in `CheckForForbiddenSigners` |
|------|-----------|-----------------|--------------------------------------|
| `SorobanCredentialsSourceAccount` | Contract uses transaction source account's auth | `opSourceAccount` | Checked against `forbiddenSigners ∪ {""}` |
| `SorobanCredentialsAddress` | Contract specifies explicit signer address | `auth.Credentials.Address.Address` | Checked against `forbiddenSigners` only |

### Integration Points

- `adjustParamsForSoroban()` in `internal/services/transaction_service.go` calls `CheckForForbiddenSigners` passing the channel account public key as the forbidden signer
- On `ErrForbiddenSigner`, the GraphQL mutation resolver (`mutations.resolvers.go`) maps it to `FORBIDDEN_SIGNER` extension code in the error response
- `AuthSigner` struct is available but `AuthorizeEntry` is not called server-side; the server returns pre-assembled but unsigned auth entries to clients via `BuildTransaction`

### Error Types

| Error | Type | Description |
|-------|------|-------------|
| `ErrForbiddenSigner` | `errors.New` sentinel | Auth entry requires a forbidden signer (e.g. channel account) |
| `UnsupportedCredentialsTypeError` | struct with `CredentialsType` field | Auth entry has credentials type other than SourceAccount or Address |

---

**Topics:** [[entries/index]] | [[entries/authentication]]
