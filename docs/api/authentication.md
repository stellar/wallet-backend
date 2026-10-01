# Request authentication

For anyone writing a client in any language. After reading it you can sign a request so a wallet-backend with `CLIENT_AUTH_PUBLIC_KEYS` set accepts it, and check your signer against a fixed example.

Authentication is off unless the operator sets `CLIENT_AUTH_PUBLIC_KEYS`. With it set, every request to `/graphql/query` must carry a JWT signed by one of those Stellar keys. `/health` and `/api-metrics` never need one.

## Token format

The token is a JWS (compact serialization) with algorithm `EdDSA`. The signing key is the Ed25519 key behind a Stellar secret seed (`S...`). The server looks the public key (`G...`) up in `CLIENT_AUTH_PUBLIC_KEYS`.

```http
Authorization: Bearer <token>
```

Header:

```json
{"alg":"EdDSA","typ":"JWT"}
```

Payload claims:

| Claim | Type | Value |
|---|---|---|
| `sub` | string | Signer's Stellar public key, strkey `G...`. Must be one of the configured keys. |
| `iat` | number | Issued-at, Unix seconds. |
| `exp` | number | Expiry, Unix seconds. See [Time window](#time-window). |
| `methodAndPath` | string | HTTP method, one space, request target including the query string. For GraphQL: `POST /graphql/query`. |
| `bodyHash` | string | Lowercase hex SHA-256 of the exact request body bytes. For an empty body, the hash of zero bytes. |

No other claims are read. Claim order inside the JSON does not matter; the server parses it, it does not compare bytes.

## Time window

The server rejects the token when any of these hold. `MAX` is `CLIENT_AUTH_MAX_TIMEOUT_SECONDS`, default 15 seconds.

| Rule | Error class |
|---|---|
| `exp` or `iat` missing | 401 |
| `exp` later than now + `MAX` | 401 |
| `exp - iat` greater than `MAX` | 401 |
| `exp` in the past | 401, counted in `wallet_auth_expired_signatures_total` |

Sign right before sending. A short window (5 to 10 seconds) is enough for one request and limits replay.

## Binding to the request

`methodAndPath` must equal the server's view of the request: `<METHOD> <path>?<query>`. The server trims surrounding whitespace, nothing else. A proxy that rewrites the path breaks the signature.

`bodyHash` must be the SHA-256 of the bytes the server reads. The server reads at most `CLIENT_AUTH_MAX_BODY_SIZE_BYTES` (default 102400). A larger body is hashed truncated and fails verification, so keep requests under that size or raise the limit on the server.

Same body bytes means the same JSON serialization. Serialize once, hash those bytes, send those bytes.

## Signing procedure

1. Build the request body bytes.
2. `bodyHash = hex(sha256(body))`.
3. `methodAndPath = method + " " + path_with_query`.
4. Set `iat = now`, `exp = now + N` with `N` at most `MAX`.
5. Build the JWT with header `{"alg":"EdDSA","typ":"JWT"}` and the five claims.
6. Sign with the Ed25519 private key derived from the Stellar seed (decode the `S...` strkey to 32 raw bytes, that is the Ed25519 seed).
7. Send with `Authorization: Bearer <token>`.

## Worked example

Fixed inputs so you can compare output byte for byte. The seed is the bytes `01 02 ... 20`. Never use it outside tests.

| Input | Value |
|---|---|
| Secret seed | `SAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQTCQKRMFYYDENBWHA5DYPSBF5K` |
| Public key | `GB43KVROR7TFJ6KAPCYRF2FJROTZAH4FHLTJLPWX4DRZCC5NASLGITR6` |
| Raw Ed25519 public key (hex) | `79b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664` |
| Method and path | `POST /graphql/query` |
| `iat` | `1767225600` (2026-01-01T00:00:00Z) |
| `exp` | `1767225610` |

Body, exactly these bytes, no trailing newline:

```json
{"query":"{ accountByAddress(address: \"GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H\") { address } }"}
```

`bodyHash`:

```text
ec4dfc30a8263f6c4de70e779fc323ebcfb3ebf73b0197a78dfd3476bc89e26e
```

Payload (any key order is fine):

```json
{
  "bodyHash": "ec4dfc30a8263f6c4de70e779fc323ebcfb3ebf73b0197a78dfd3476bc89e26e",
  "exp": 1767225610,
  "iat": 1767225600,
  "methodAndPath": "POST /graphql/query",
  "sub": "GB43KVROR7TFJ6KAPCYRF2FJROTZAH4FHLTJLPWX4DRZCC5NASLGITR6"
}
```

Token produced from the payload above with keys sorted as shown:

```text
eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJib2R5SGFzaCI6ImVjNGRmYzMwYTgyNjNmNmM0ZGU3MGU3NzlmYzMyM2ViY2ZiM2ViZjczYjAxOTdhNzhkZmQzNDc2YmM4OWUyNmUiLCJleHAiOjE3NjcyMjU2MTAsImlhdCI6MTc2NzIyNTYwMCwibWV0aG9kQW5kUGF0aCI6IlBPU1QgL2dyYXBocWwvcXVlcnkiLCJzdWIiOiJHQjQzS1ZST1I3VEZKNktBUENZUkYyRkpST1RaQUg0RkhMVEpMUFdYNERSWkNDNU5BU0xHSVRSNiJ9.g_J11H2T3N2QnABso1qaEJzMbvtMLoDTZ4zMX3Oki93fOEsaq8R8b11Q4K2KT-eyLxcUjiOidwjDosk-9bnTDA
```

Ed25519 signatures are deterministic, so a signer that serializes the payload with the same key order produces this exact token. With a different key order the signature differs but the server still accepts it. The token itself is expired; it is for comparing signers, not for sending.

Request:

```bash
curl -s -X POST http://localhost:8001/graphql/query \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer <TOKEN>" \
  --data-binary '{"query":"{ accountByAddress(address: \"GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H\") { address } }"}'
```

## Responses on failure

| Condition | Status | Body |
|---|---|---|
| Header missing, not `Bearer`, bad signature, unknown `sub`, claim mismatch, expired | 401 | `{"error":"Not authorized."}` |
| Server could not read the body | 500 | `{"error":"An error occurred while processing this request."}` |

The server logs the exact reason at error level; the response does not include it.

## Go client

`pkg/wbclient` ships a signer: `auth.NewJWTManager(seed, publicKey, maxTimeout)` and `auth.NewHTTPRequestSigner(manager)`. `wbclient.NewClient(baseURL, signer)` signs every request with a 5-second window. See [Go client](go-client.md).

## Where in the code

| Path | Role |
|---|---|
| `pkg/wbclient/auth/claims.go` | Claim set and validation rules |
| `pkg/wbclient/auth/jwt_manager.go` | Signing and parsing; multi-key parser |
| `pkg/wbclient/auth/jwt_http_signer_verifier.go` | `methodAndPath` and body handling on both sides |
| `pkg/wbclient/auth/helpers.go` | `HashBody` |
| `internal/serve/middleware/middleware.go` | 401 vs 500 mapping, expired-signature metric |
| `internal/serve/serve.go` | `buildRequestAuthVerifier`, which routes are protected |
