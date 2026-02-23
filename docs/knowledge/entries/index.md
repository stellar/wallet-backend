---
description: Hub knowledge map — entry point for the wallet-backend knowledge system
type: reference
vault: docs/knowledge
areas: [hub, navigation, index]
---

# Wallet-Backend Knowledge Hub

Entry point for the wallet-backend knowledge system. Navigate by subsystem or by entry type.

## Subsystem Maps

| Subsystem | Knowledge Map | Reference Doc | Entries |
|-----------|--------------|---------------|---------|
| Ingestion | [[entries/ingestion]] | [[references/ingestion-pipeline]] | 0 |
| GraphQL API | [[entries/graphql-api]] | [[references/graphql-api]] | 0 |
| Data Layer | [[entries/data-layer]] | [[references/data-layer]] | 0 |
| Signing & Channels | [[entries/signing]] | [[references/signing-and-channels]] | 0 |
| Services | [[entries/services]] | [[references/services]] | 0 |
| Authentication | [[entries/authentication]] | [[references/authentication]] | 0 |

## System

- [[self/identity]] — What this knowledge system tracks
- [[self/methodology]] — Processing cycle (document → connect → revisit → verify)
- [[self/goals]] — Active goals and threads

## Entry Types

- **Decisions** — Why we chose X (e.g., why TimescaleDB, why channel accounts)
- **Insights** — Non-obvious understanding (e.g., how hypertable chunk skipping works)
- **Patterns** — Reusable implementations (e.g., Options struct DI, dataloader batching)
- **Gotchas** — Traps and surprises (e.g., Stellar RPC quirks, migration ordering)

## Getting Started

New? Read [[manual/getting-started]]. Already familiar? Check [[ops/tasks]] for pending work.
