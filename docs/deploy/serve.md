# Serve

Step 4 of [deploying wallet-backend](README.md). After it the GraphQL API answers queries, with authentication if you want it.

## Contents

- [Start the API](#start-the-api)
- [Enable authentication](#enable-authentication)
- [Where in the code](#where-in-the-code)

Config names use the environment-variable form. Every variable and its default is in the [configuration reference](configuration.md).

## Start the API

1. Set the environment.

   ```bash
   DATABASE_URL=<DATABASE_URL>
   RPC_URL=<RPC_URL>
   NETWORK_PASSPHRASE=<NETWORK_PASSPHRASE>
   PORT=8001
   CLIENT_AUTH_PUBLIC_KEYS=<G_PUBLIC_KEY>
   ```

   Leave `CLIENT_AUTH_PUBLIC_KEYS` unset to run without authentication. See [Enable authentication](#enable-authentication).

2. Start the server.

   ```bash
   wallet-backend serve
   ```

3. Verify with a real query. Introspection is off unless `GRAPHQL_INTROSPECTION_ENABLED=true`.

   ```graphql
   query Account($address: String!) {
     accountByAddress(address: $address) {
       address
     }
   }
   ```

   ```json
   { "address": "<G_ADDRESS>" }
   ```

   ```bash
   curl -s localhost:8001/graphql/query \
     -H 'Content-Type: application/json' \
     -d '{"query":"query Account($address: String!) { accountByAddress(address: $address) { address } }","variables":{"address":"<G_ADDRESS>"}}'
   ```

   ```json
   { "data": { "accountByAddress": { "address": "<G_ADDRESS>" } } }
   ```

   With authentication on, add `-H 'Authorization: Bearer <JWT>'`. Without it the server returns 401.

`GET /health` on the API port runs the same check as the ingester: 500 when the RPC cannot be reached, 503 when the RPC reports itself unhealthy or when ingest is more than 50 ledgers behind the RPC. Point load-balancer health checks at it only if a lagging ingester should take the API out of rotation.

## Enable authentication

1. Generate a Stellar keypair for each client. With the Stellar CLI:

   ```bash
   stellar keys generate <CLIENT_NAME>
   stellar keys address <CLIENT_NAME>
   ```

   Any Stellar SDK works too. The client keeps the secret key. The server gets only the `G...` public key.

2. Set the public keys on the API and restart it.

   ```bash
   CLIENT_AUTH_PUBLIC_KEYS=<G_KEY_1>,<G_KEY_2>
   ```

3. Verify: an unsigned request to `/graphql/query` returns 401. `/health` and `/api-metrics` stay open.

Token format and signing steps are in [request authentication](../api/authentication.md).

Next, optional: [backfill history](backfill.md).

## Where in the code

| Path | Role |
| --- | --- |
| `internal/serve/serve.go` | API routes and GraphQL server |
| `internal/serve/httphandler/health.go` | `/health` logic and the 50-ledger threshold |
