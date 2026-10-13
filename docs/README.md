# wallet-backend documentation

Pick by what you are trying to do.

## Tutorial

| Page | You will |
|---|---|
| [Getting started](getting-started.md) | Run the testnet stack with Compose and make your first queries |

## How-to

| Page | You will |
|---|---|
| [Running wallet-backend](deploy/README.md) | Set up the database, start ingestion and the API, backfill history, enable auth |
| [Upgrading](deploy/upgrading.md) | Move between releases safely |
| [Runbook](deploy/runbook.md) | Size a deployment, set alerts, handle failures |
| [Protocol data migrations](deploy/protocols.md) | Load history for a protocol added later |
| [Adding a protocol](contributing/adding-a-protocol.md) | Implement a new contract protocol |
| [Integration tests](contributing/integration-tests.md) | Run the end-to-end suite |
| [Releasing](contributing/releasing.md) | Cut, soak and promote a release (maintainers) |

## Reference

| Page | Contents |
|---|---|
| [Configuration](deploy/configuration.md) | Every flag and environment variable per command |
| [GraphQL schema](api/schema.md) | Generated from the schema files |
| [Request authentication](api/authentication.md) | JWT claims, signing procedure, worked example |
| [Go client](api/go-client.md) | `pkg/wbclient` |
| [Data model](architecture/data-model.md) | Tables, keys, indexes, cursors |
| [Observability](deploy/observability.md) | Metrics, health, logs |

## Explanation

| Page | Covers |
|---|---|
| [Overview](architecture/overview.md) | Processes, data flow, boundaries |
| [Ingestion](architecture/ingestion.md) | Live loop, backfill, ledger sources, cursors |
| [Token and balance tracking](architecture/token-tracking.md) | What balances exist and how they stay current |
| [State changes](architecture/state-changes.md) | Categories, reasons, processors |
| [Protocols](architecture/protocols.md) | Classification, processing, data migrations |
| [GraphQL serving](architecture/graphql-serving.md) | Request path, limits, dataloaders |
| [Database](architecture/database.md) | Hypertables, policies, connections |
| [GraphQL guide](api/graphql.md) | Queries, pagination, balances semantics, limits |
