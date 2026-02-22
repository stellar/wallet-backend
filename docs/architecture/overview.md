# Architecture Overview

## High-Level Data Flow

<!-- TODO: Create ASCII diagram showing the two main data paths:
     1. Ingestion: Stellar RPC → Ingest Service → Indexer/Processors → TimescaleDB
     2. API: Client → Chi Router → Auth Middleware → GraphQL → Resolvers → DataLoaders → DB
     Read internal/serve/serve.go and cmd/ingest.go for the two entry points. -->

## Two Main Processes

The wallet-backend runs as two separate processes:

- **`serve`** — HTTP server exposing the GraphQL API, health checks, and transaction submission
- **`ingest`** — Ledger ingestion pipeline that reads from Stellar RPC and writes to TimescaleDB

<!-- TODO: Read cmd/serve.go and cmd/ingest.go for startup flags and initialization flow. -->

## Directory Structure

```
cmd/                           CLI commands (Cobra): serve, ingest, migrate, channel-account, distribution-account
internal/
  serve/                       HTTP layer (Chi router)
    httphandler/               HTTP handlers (currently just health check)
    middleware/                 Auth (JWT/Ed25519), metrics, recovery, GraphQL middleware
    graphql/
      schema/*.graphqls        GraphQL schema definitions (source of truth)
      generated/               Auto-generated code (DO NOT edit) — run `make gql-generate`
      resolvers/               Resolver implementations (follow-schema layout)
      dataloaders/             DataLoader batching to prevent N+1 queries
      scalars/                 Custom scalar types (UInt32)
  services/                    Business logic (TransactionService, RPCService, FeeBumpService, IngestService, etc.)
  data/                        Database models and queries (interface-based for testability)
  db/
    dbtest/                    Test database helpers (auto-creates TimescaleDB + runs migrations)
    migrations/*.sql           SQL migrations (format: YYYY-MM-DD.N-description.sql)
  indexer/                     Ledger event processing and type definitions
  signing/                     Pluggable signature providers: ENV (env var), KMS (AWS), ChannelAccount (DB-encrypted)
    store/                     Channel account DB storage
    utils/                     Key encryption utilities
  entities/                    Domain models (Asset, Signer, RPC types, pagination)
  metrics/                     Prometheus metrics collection
  apptracker/                  Error tracking (Sentry integration)
  integrationtests/            End-to-end tests with Docker containers (testcontainers-go)
pkg/                           Public packages: client library, auth utilities, Soroban auth
```

## Dependency Wiring

<!-- TODO: Read internal/serve/serve.go:initHandlerDeps and cmd/ingest.go to document the Options struct wiring pattern.
     Show how services are constructed with their Options structs and how dependencies are injected.
     Include the full chain: CLI flags → config → Options structs → service construction → handler registration. -->

## Key Architectural Patterns

- **Dependency injection via Options structs**: Services are constructed with an Options struct that has a `ValidateOptions()` method. All dependencies (DB, signing clients, other services) are injected through these structs.
- **Interface-based data models**: Each data model implements an interface to support mocking in tests. All models are aggregated in `data.Models`.
- **Schema-first GraphQL**: Schema files are the source of truth. Code is generated from them.
