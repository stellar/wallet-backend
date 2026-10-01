# wallet-backend documentation

Pick by what you are trying to do.

## Tutorial

| Page | You will |
|---|---|
| [Getting started](getting-started.md) | Run the testnet stack with Compose and make your first queries |

## How-to

| Page | You will |
|---|---|
| [Running wallet-backend](operations/running.md) | Set up the database, start ingestion and the API, backfill history, enable auth |
| [Upgrading](operations/upgrading.md) | Move between releases safely |
| [Runbook](operations/runbook.md) | Size a deployment, set alerts, handle failures |
| [Protocol data migrations](operations/data-migrations.md) | Load history for a protocol added later |
| [Adding a protocol](development/adding-a-protocol.md) | Implement a new contract protocol |
| [Integration tests](development/integration-tests.md) | Run the end-to-end suite |
| [Releasing](releasing.md) | Cut, soak and promote a release (maintainers) |

## Reference

| Page | Contents |
|---|---|
| [Configuration](configuration.md) | Every flag and environment variable per command |
| [GraphQL schema](api/schema.md) | Generated from the schema files |
| [Request authentication](api/authentication.md) | JWT claims, signing procedure, worked example |
| [Go client](api/go-client.md) | `pkg/wbclient` |
| [Data model](architecture/data-model.md) | Tables, keys, indexes, cursors |
| [Observability](operations/observability.md) | Metrics, health, logs |

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
