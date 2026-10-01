# wallet-backend

[![CI](https://github.com/stellar/wallet-backend/actions/workflows/go.yaml/badge.svg?branch=main)](https://github.com/stellar/wallet-backend/actions/workflows/go.yaml)
[![Release](https://img.shields.io/github/v/release/stellar/wallet-backend?display_name=tag)](https://github.com/stellar/wallet-backend/releases)
[![Docker](https://img.shields.io/docker/v/stellar/wallet-backend?label=docker&sort=semver)](https://hub.docker.com/r/stellar/wallet-backend)
[![Go](https://img.shields.io/github/go-mod/go-version/stellar/wallet-backend)](go.mod)
[![License](https://img.shields.io/github/license/stellar/wallet-backend)](LICENSE)

A Stellar indexer for wallets: every transaction, operation and balance change for every account, plus current balances for every asset and token, served over GraphQL.

- Run it against your own stellar-rpc and PostgreSQL. One process ingests the ledger, another serves the API.
- Ask for an account and get its transactions, operations, state changes and balances with cursor pagination.
- Balances cover XLM, classic assets, Stellar Asset Contracts, SEP-41 tokens and liquidity pool shares.

Docs on `main` describe unreleased behavior. For the version you run, read the docs at its tag: `https://github.com/stellar/wallet-backend/tree/<TAG>/docs`.

## Requirements

| Component | Version |
|---|---|
| PostgreSQL with the TimescaleDB extension | 17 with TimescaleDB 2.28 or newer |
| stellar-rpc for the same network | 28.x |
| Go (source builds only) | see `go.mod` |

## Quickstart

Testnet, one machine, the published image. Needs Docker with Compose v2.

```bash
git clone https://github.com/stellar/wallet-backend.git
cd wallet-backend
docker compose up
```

Compose starts TimescaleDB, a testnet stellar-rpc, runs the migrations, then starts ingestion and the API. The first start downloads a checkpoint from the history archive and loads balances before ingesting live ledgers; watch the `ingest` logs. Ingestion is healthy when this returns `{"status":"ok", ...}`:

```bash
curl -s localhost:8002/health
```

Query the API:

```bash
curl -s -X POST localhost:8001/graphql/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ accountByAddress(address: \"GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H\") { address balances(first: 5) { edges { node { __typename } } } } }"}'
```

Pin a release with `WALLET_BACKEND_VERSION=v1.0.0 docker compose up`. For mainnet and real deployments, read [Running wallet-backend](docs/operations/running.md).

## Documentation

| I want to | Read |
|---|---|
| Try it end to end on testnet | [Getting started](docs/getting-started.md) |
| Run it for real: database, ingestion, API, backfill | [Running wallet-backend](docs/operations/running.md) |
| Look up a flag or environment variable | [Configuration reference](docs/configuration.md) |
| Query the API | [GraphQL guide](docs/api/graphql.md), [schema reference](docs/api/schema.md) |
| Sign requests from any language | [Request authentication](docs/api/authentication.md) |
| Use the Go client | [Go client](docs/api/go-client.md) |
| Operate it: sizing, alerts, failures | [Runbook](docs/operations/runbook.md), [Observability](docs/operations/observability.md) |
| Upgrade between releases | [Upgrading](docs/operations/upgrading.md) |
| Understand how it works | [Architecture](docs/architecture/overview.md) |
| Contribute | [CONTRIBUTING](CONTRIBUTING.md), [docs index](docs/README.md) |

## Stability

Releases follow semantic versioning from `v1.0.0`. Within a major version:

- A GraphQL field or enum value is removed only after it has been marked `@deprecated` for at least one minor release.
- Flag and environment variable names do not change.
- Database migrations are forward-only; upgrading is `migrate up` with the new binary.

Release notes list the stellar-rpc, protocol, PostgreSQL and TimescaleDB versions each release was tested with. See [Releasing](docs/releasing.md) for the cadence.

## Support

Bugs and feature requests: [GitHub Issues](https://github.com/stellar/wallet-backend/issues). Questions: see [SUPPORT.md](SUPPORT.md).

## License

[Apache 2.0](LICENSE)
