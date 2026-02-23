---
description: Knowledge map for the data layer subsystem
type: reference
subsystem: data-layer
areas: [knowledge-map, timescaledb, hypertables, migrations, data-models]
vault: docs/knowledge
---

# Data Layer Knowledge Map

The data layer stores ledger events in five TimescaleDB hypertables partitioned by `ledger_created_at`. Balance tables (trustline, native, SAC) are regular PostgreSQL tables with high-fill-factor tuning for update-heavy workloads.

**Key code:** `internal/data/`, `internal/db/migrations/`, `internal/entities/`

## Reference Doc

[[references/data-layer]] — hypertable schema, compression policies, chunk skipping, migration patterns, balance table design

## Decisions

<!-- - [[entries/why-timescaledb]] -->
<!-- - [[entries/one-day-chunk-interval]] -->
<!-- - [[entries/segmentby-account-id]] -->

## Insights

<!-- - [[entries/chunk-skipping-requires-extension]] -->
<!-- - [[entries/bloom-filter-sparse-index]] -->

## Patterns

<!-- - [[entries/interface-based-data-models]] -->
<!-- - [[entries/migration-naming-convention]] -->

## Gotchas

<!-- - [[entries/timescale-extension-must-be-enabled-first]] -->

## Topics

[[entries/index]] | [[references/data-layer]]
