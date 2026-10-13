# wallet-backend documentation

Each folder is for one kind of reader. Start with the one that matches what you are doing.

| I want to | Start at |
| --- | --- |
| Try it on testnet in a few minutes | [Getting started](getting-started.md) |
| Deploy and operate it | [deploy/](deploy/README.md) |
| Call the API | [api/](#api) |
| Understand how it works | [architecture/](#architecture) |
| Change the code or cut a release | [contributing/](contributing/README.md) |

## Deploy

For operators, in order on a fresh install. The [deploy guide](deploy/README.md) explains each step.

| Page | You will |
| --- | --- |
| [1. Install](deploy/install.md) | Check requirements, create the database, apply the schema |
| [2. Ingest](deploy/ingest.md) | Start live ingestion, pick a ledger source |
| [3. Protocols](deploy/protocols.md) | Set up SEP-41 and fill its data, on a fresh install or when a release adds a protocol |
| [4. Serve](deploy/serve.md) | Start the API, turn on authentication |
| [5. Backfill](deploy/backfill.md) | Optional. Load older history |
| [Upgrading](deploy/upgrading.md) | Move between releases |
| [Runbook](deploy/runbook.md) | Size hardware, set alerts, handle failures |
| [Observability](deploy/observability.md) | Metrics, health endpoints, logs |
| [Configuration](deploy/configuration.md) | Every flag and environment variable per command |

## API

For clients of the GraphQL API.

| Page | Contents |
| --- | --- |
| [GraphQL guide](api/graphql.md) | Queries, pagination, balance semantics, limits |
| [GraphQL schema](api/schema.md) | Every type and field, generated from the schema files |
| [Request authentication](api/authentication.md) | JWT claims, signing procedure, worked example |
| [Go client](api/go-client.md) | `pkg/wbclient` |

## Architecture

How it works, for anyone debugging or changing it.

| Page | Covers |
| --- | --- |
| [Overview](architecture/overview.md) | Processes, data flow, boundaries |
| [Ingestion](architecture/ingestion.md) | Live pipeline, backfill, ledger sources, cursors |
| [Token and balance tracking](architecture/token-tracking.md) | What balances exist and how they stay current |
| [State changes](architecture/state-changes.md) | Categories, reasons, processors |
| [Protocols](architecture/protocols.md) | Classification, processing, data migrations |
| [GraphQL serving](architecture/graphql-serving.md) | Request path, limits, dataloaders |
| [Database](architecture/database.md) | Hypertables, policies, connections |
| [Data model](architecture/data-model.md) | Tables, keys, indexes, cursors |

## Contributing

| Page | You will |
| --- | --- |
| [Adding a protocol](contributing/adding-a-protocol.md) | Implement a new contract protocol |
| [Integration tests](contributing/integration-tests.md) | Run the end-to-end suite |
| [Releasing](contributing/releasing.md) | Cut, soak and promote a release (maintainers) |
