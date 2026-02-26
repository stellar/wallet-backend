---
description: Main navigation hub for the wallet-backend knowledge graph
type: reference
status: current
subsystem: ingestion
areas: [navigation, index]
---

# wallet-backend Knowledge Graph — Main Index

The wallet-backend is a Go service that ingests Stellar ledger data into TimescaleDB and exposes it via GraphQL. It runs as two processes: `serve` (API) and `ingest` (ledger ingestion), coordinated only through the database.

## Subsystem Maps

- [[ingestion]] — Live/backfill ingestion flow, indexer, retry logic, cursors
- [[data-layer]] — TimescaleDB hypertables, model interfaces, query patterns
- [[signing]] — Signing providers (ENV/KMS), channel account lifecycle, encryption
- [[graphql-api]] — Request flow, resolvers, dataloaders, mutations
- [[services]] — Service patterns, dependency injection, service inventory
- [[authentication]] — JWT/Ed25519 auth, Soroban auth, client library

## Cross-Cutting Entries

These entries connect multiple subsystems:

- [[the database is the sole integration point between the serve and ingest processes]] — Architecture decision that defines process boundaries
- [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — Where ingestion, signing, and data-layer meet
- [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — Signing + ingestion integration point
- [[buildTransaction mutation returns signed xdr to the client rather than submitting it]] — GraphQL + signing design
- [[state changes use a two-axis category-reason taxonomy to classify every account history event]] — Ingestion + GraphQL taxonomy

## Entry Count by Subsystem

| Subsystem | Entries |
|-----------|---------|
| ingestion | 22 |
| data-layer | 19 |
| signing | 6 |
| graphql | 3 |
| services | 2 |
| auth | 3 |
| **Total** | **55** |

Note: some entries appear in multiple subsystems (e.g. data.models spans services+data-layer, buildTransaction spans graphql+signing, row_number/toid span data-layer+graphql). The total reflects unique entries.

Note: 2026-02-26 batch "why-timescaledb-over-vanilla-postgres" added 3 new entries (2 claims, 1 enrichment target) and enriched 2 existing entries.
Note: 2026-02-26 batch "token-ingestion" added 11 new entries and enriched 3 existing entries. New entries cover: G/C address SAC balance split, checkpoint FK disable strategy, DEFERRABLE FK pattern, streaming batch architecture, UNKNOWN contract skip, wazero SEP-41 validation, SEP-41 10-function classification, dummy keypair rationale, RPC rate-limiting, account_contract_tokens append-only design, plus 1 tension entry.
