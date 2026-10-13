# Deploying wallet-backend

For an operator running wallet-backend outside the [Compose quickstart](../getting-started.md). On a fresh install, do the steps in order. Each page ends with a check that the step worked.

| Step | Page | You will |
| --- | --- | --- |
| 1 | [Install](install.md) | Check the requirements, create the database, apply the schema |
| 2 | [Ingest](ingest.md) | Start live ingestion and pick a ledger source |
| 3 | [Protocols](protocols.md) | Classify contracts and fill SEP-41 balances and history. Without this step SEP-41 data stays empty |
| 4 | [Serve](serve.md) | Start the GraphQL API and turn on authentication |
| 5 | [Backfill](backfill.md) | Optional. Load history older than the ledger live ingestion started from |

## Keep it running

| Page | Use it to |
| --- | --- |
| [Upgrading](upgrading.md) | Move between releases, including releases that add a protocol |
| [Runbook](runbook.md) | Size hardware, set alerts, handle failures |
| [Observability](observability.md) | Look up a metric, the health endpoints, the logs |
| [Configuration](configuration.md) | Look up any flag or environment variable |
