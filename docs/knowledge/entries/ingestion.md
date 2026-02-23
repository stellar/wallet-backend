---
description: Knowledge map for the ingestion pipeline subsystem
type: reference
subsystem: ingestion
areas: [knowledge-map, ingestion, indexer, stellar-rpc]
vault: docs/knowledge
---

# Ingestion Pipeline Knowledge Map

The ingestion pipeline reads Stellar ledger data via the Stellar RPC node and persists it to TimescaleDB. It runs as two modes — live (follows the chain) and backfill (fills historical gaps) — sharing a common Indexer core.

**Key code:** `internal/ingest/`, `internal/services/ingest*.go`, `internal/indexer/`

## Reference Doc

[[references/ingestion-pipeline]] — comprehensive overview with live/backfill flow diagrams, retry logic, processor architecture

## Decisions

<!-- Document decisions here as entries are added -->
<!-- - [[entries/why-pond-worker-pool]] -->

## Insights

<!-- - [[entries/catchup-threshold-behavior]] -->

## Patterns

<!-- - [[entries/indexer-processor-pattern]] -->

## Gotchas

<!-- - [[entries/rpc-rate-limit-behavior]] -->

## Topics

[[entries/index]] | [[references/ingestion-pipeline]]
