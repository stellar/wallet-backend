# Getting started

For someone with Docker who wants to see wallet-backend work on testnet. After this tutorial you have it running locally, ingesting testnet, and answering GraphQL queries about an account you funded.

## Steps

1. **Clone and start the stack.**

   ```bash
   git clone https://github.com/stellar/wallet-backend.git
   cd wallet-backend
   docker compose up -d
   ```

   | Service | Starts after | Job | Port |
   | --- | --- | --- | --- |
   | `db` | nothing | TimescaleDB on PostgreSQL 17 | 5432 |
   | `stellar-rpc` | nothing | Stellar RPC on testnet | 8000 |
   | `migrate` | `db` healthy | Runs `migrate up` once, then exits | none |
   | `ingest` | `migrate` done, `stellar-rpc` healthy | Live ingestion; `/health` | 8002 |
   | `api` | `migrate` done, `stellar-rpc` healthy | GraphQL server; `/health` | 8001 |

   Verify: `docker compose logs migrate` shows `Successfully applied <N> migrations up.` and `Executed protocol migration: 001_sep41.sql`.

2. **Wait for RPC to sync and ingest to catch up.** RPC must join testnet before `ingest` and `api` start. Check its health:

   ```bash
   curl -s -X POST localhost:8000 -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
   ```

   ```json
   {"jsonrpc":"2.0","id":1,"result":{"status":"healthy","latestLedger":<LEDGER>,"oldestLedger":<LEDGER>,"ledgerRetentionWindow":<N>}}
   ```

   On an empty database, `ingest` loads balances from the latest history archive checkpoint, then streams ledgers. Watch for these lines in `docker compose logs -f ingest`:

   ```text
   msg="Populating from checkpoint ledger = <LEDGER>"
   msg="Starting ingestion from ledger: <LEDGER>"
   msg="Ingested ledger <LEDGER> in <SECONDS>s"
   ```

   Verify: `curl -s localhost:8002/health` returns `{"backend_latest_ledger":<LEDGER>,"status":"ok"}`. Until ingest is within 50 ledgers of RPC it returns HTTP 503 with `{"error":"wallet backend is not in sync with the RPC", ...}`.

3. **Fund an account and query it.** Create a keypair with the [Stellar CLI](https://developers.stellar.org/docs/tools/cli), or use any testnet address you hold. Fund it with friendbot:

   ```bash
   stellar keys generate quickstart
   stellar keys address quickstart
   curl "https://friendbot.stellar.org/?addr=<G_ADDRESS>"
   ```

   GraphQL takes POST requests at `/graphql/query` on port 8001. The quickstart sets no `CLIENT_AUTH_PUBLIC_KEYS`, so requests need no token.

   ```bash
   curl -s localhost:8001/graphql/query -H 'Content-Type: application/json' \
     -d '{"query":"query Account($address: String!) { accountByAddress(address: $address) { address } }","variables":{"address":"<G_ADDRESS>"}}'
   ```

   ```json
   {"data":{"accountByAddress":{"address":"<G_ADDRESS>"}}}
   ```

   Verify: the response has no `errors` key. `accountByAddress` returns an `Account` for any valid address, and its history and balances fill in once ingest processes the funding ledger.

4. **See the funding as state changes.** Friendbot's `create_account` operation gives the account an `AccountCreatedChange`, a `BalanceChange` with reason `CREDIT`, and a `SignerAddedChange`.

   ```graphql
   query Changes($address: String!) {
     accountByAddress(address: $address) {
       stateChanges(first: 10) {
         edges { node {
           __typename category reason ledgerNumber
           transaction { hash }
           ... on AccountCreatedChange { creatorAddress }
           ... on BalanceChange { tokenId amount }
         } }
       }
     }
   }
   ```

   ```bash
   curl -s localhost:8001/graphql/query -H 'Content-Type: application/json' \
     -d '{"query":"query Changes($address: String!) { accountByAddress(address: $address) { stateChanges(first: 10) { edges { node { __typename category reason ledgerNumber transaction { hash } ... on AccountCreatedChange { creatorAddress } ... on BalanceChange { tokenId amount } } } } } }","variables":{"address":"<G_ADDRESS>"}}'
   ```

   One node of the response:

   ```json
   {"__typename":"AccountCreatedChange","category":"ACCOUNT","reason":"CREATE","ledgerNumber":<LEDGER>,"transaction":{"hash":"<TX_HASH>"},"creatorAddress":"<FRIENDBOT_ADDRESS>"}
   ```

   Verify: `edges` is not empty. If it is, wait for `backend_latest_ledger` in step 2 to pass the funding ledger.

5. **Read the XLM balance.**

   ```graphql
   query Balances($address: String!) {
     accountByAddress(address: $address) {
       balances(first: 10) {
         edges { node {
           __typename balance tokenId tokenType
           ... on NativeBalance { minimumBalance lastModifiedLedger }
         } }
       }
     }
   }
   ```

   ```bash
   curl -s localhost:8001/graphql/query -H 'Content-Type: application/json' \
     -d '{"query":"query Balances($address: String!) { accountByAddress(address: $address) { balances(first: 10) { edges { node { __typename balance tokenId tokenType ... on NativeBalance { minimumBalance lastModifiedLedger } } } } } }","variables":{"address":"<G_ADDRESS>"}}'
   ```

   ```json
   {"__typename":"NativeBalance","balance":"<AMOUNT>","tokenId":"<NATIVE_SAC_CONTRACT_ID>","tokenType":"NATIVE","minimumBalance":"<AMOUNT>","lastModifiedLedger":<LEDGER>}
   ```

   Verify: `balance` matches the amount friendbot sent, with 7 decimal places. `tokenId` is the native asset's Stellar Asset Contract ID.

6. **Explore the schema.** The quickstart sets `GRAPHQL_INTROSPECTION_ENABLED=true`. It is off by default.

   ```bash
   curl -s localhost:8001/graphql/query -H 'Content-Type: application/json' \
     -d '{"query":"{ __schema { queryType { fields { name } } } }"}'
   ```

   ```json
   {"data":{"__schema":{"queryType":{"fields":[{"name":"transactionByHash"},{"name":"accountByAddress"},{"name":"operationById"}]}}}}
   ```

   Verify: the three query names appear. Every type and field is listed in the [GraphQL schema reference](api/schema.md).

7. **Stop and clean up.**

   | Command | Keeps |
   | --- | --- |
   | `docker compose stop` | Containers and data. `docker compose start` resumes. |
   | `docker compose down` | The `db-data` volume. The RPC container has no volume, so it syncs again on the next `up`. |
   | `docker compose down -v` | Nothing. |

## Next steps

- [Running wallet-backend](operations/running.md) for a production deployment and mainnet.
- [Configuration reference](configuration.md) for every flag and environment variable.
- [Request authentication](api/authentication.md) to turn on JWT auth with `CLIENT_AUTH_PUBLIC_KEYS`.
