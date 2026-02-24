---
description: Knowledge map for the GraphQL API subsystem
type: reference
subsystem: graphql
areas: [knowledge-map, graphql, resolvers, dataloaders, gqlgen]
vault: docs/knowledge
---

# GraphQL API Knowledge Map

The GraphQL API is built with gqlgen (schema-first). It exposes ledger data (transactions, operations, state changes) and transaction mutation endpoints. Dataloaders prevent N+1 queries for relationship fields. Thread safety is enforced by a two-tier model: the shared resolver requires all injected dependencies to be inherently safe, while per-request dataloaders handle the one exception that cannot be shared.

**Key code:** `internal/serve/graphql/`, `internal/serve/serve.go`

## Reference Doc

[[references/graphql-api]] — request flow, middleware stack, resolver patterns, dataloader architecture, complexity scoring

## Decisions

- [[entries/gqlgen schema-first means all graphqls files are the source of truth and make gql-generate must run after every schema change]] — mechanical requirement that follows from schema-first; gotcha for contributors
- [[entries/relay-style cursor pagination on all list fields enables bidirectional traversal via first after and last before]] — covers all three entity types; stable position encoding even under insert load
- [[entries/Balance as an interface allows balancesByAccountAddress to return heterogeneous balance types in a single query]] — explains the interface design for four balance types
- [[entries/mutations do not submit transactions — clients receive signed XDR and submit directly to Stellar network]] — key security boundary; wallet-backend never proxies submission
- [[entries/fee-bump eligibility check prevents the distribution account from sponsoring arbitrary non-tracked accounts]] — explains the IsAccountFeeBumpEligible guard

## Insights

- [[entries/since and until parameters on Account queries enable TimescaleDB chunk exclusion to reduce scan cost for time-bounded account history]] — cross-subsystem link between schema design and TimescaleDB performance
- [[entries/dataloaders are created fresh per HTTP request to prevent cross-request data leakage and stale reads in horizontally-scaled deployments]] — per-request lifecycle; why global dataloaders would be incorrect
- [[entries/GetDBColumnsForFields introspects the live GraphQL field selection set to build column-projected SQL queries reducing SELECT star overhead]] — column projection optimization mechanism
- [[entries/TOID bit masking recovers parent transaction TOID from operation ID by clearing the lower 12 bits that encode op_index]] — enables join-free grouping in the operation dataloader
- [[entries/state change composite ID encodes to_id op_id and sc_order as a dash-separated string to uniquely identify a state change across three dimensions]] — explains the composite key format
- [[entries/three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — when each cursor encoding applies
- [[entries/the resolver struct is constructed once at startup and shared across all requests requiring all injected dependencies to be thread-safe]] — architectural constraint on injected services; the two-tier thread safety model
- [[entries/channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]] — crash-safety mechanism for the mutation flow
- [[entries/ingestion-based channel account release ties unlock to on-chain confirmation by running inside the same DB transaction as ledger commit]] — cross-subsystem: mutation locks, ingestion releases
- [[entries/operation is nullable on BaseStateChange because fee-related state changes arise from ledger fee processing not from a specific operation]] — schema design that surprises newcomers
- [[entries/complexity scoring defaults to 10 when neither first nor last is provided on paginated fields making unbounded queries still incur a non-zero cost]] — complexity scoring edge case

## Patterns

- [[entries/goField forceResolver is required on relationship fields to enable lazy dataloader loading instead of direct struct field access]] — without this directive, gqlgen bypasses the dataloader
- [[entries/goField forceResolver on enum fields enables type conversion from DB integer representation to GraphQL enum in the resolver]] — extends forceResolver beyond relationships to enum conversion
- [[entries/GraphQL fields that map to multiple DB columns require explicit switch cases in getDBColumns to project all required backing columns]] — maintenance hazard for compound fields

## Tensions & Open Questions

- [[entries/opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]] — Relay spec recommendation vs. debug ergonomics; compounded by three cursor encodings that are invisible in API responses
- [[entries/whether the 5ms dataloader batching window and 100-key batch capacity are optimally tuned for production traffic patterns]] — untuned defaults; requires production profiling; operates within the per-request scope constraint

## Topics

[[entries/index]] | [[references/graphql-api]]

---

Agent Notes:
- 2026-02-24: thread safety has two levels in this subsystem — shared resolver (must be inherently safe) and per-request dataloaders (exception that gets isolated). Cross-subsystem path: [[entries/factory pattern for non-thread-safe resources enables safe parallelism]] in ingestion solves the same problem class with the opposite strategy (isolate per-worker vs. require shared safety).
- 2026-02-24: cursor cluster forms a tight chain: relay spec → three cursor types → opacity → debug difficulty. Opaque cursors and three cursor types compound each other; wrong-type errors are silent because encoding is hidden.
