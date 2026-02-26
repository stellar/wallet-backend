---
description: Knowledge map for the service layer — construction patterns, service inventory, and dependency wiring for serve and ingest paths
type: reference
status: current
subsystem: services
areas: [navigation, services]
---

# Services — Knowledge Map

Nine services in `internal/services/` implement the business logic layer. They follow two construction patterns depending on dependency count, and are wired differently in the serve path (`initHandlerDeps`) vs the ingest path (`setupDeps`).

## Construction Patterns

- [[options struct plus validateoptions enables compile-time-visible dependency validation]] — The Options struct pattern for services with 5+ deps; fail-fast misconfiguration
- [[data.models aggregates all database model interfaces into a single injectable struct]] — How 11 data models are passed as a single injectable unit
- [[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — Testing strategy split between mock-based and dbtest-based approaches

## Service Inventory Notes

- **Serve path only**: RPCService, FeeBumpService, ContractMetadataService, TransactionService, ChannelAccountService
- **Ingest path only**: IngestService, TokenIngestionService, ContractValidator, KMSImportService
- **Shared**: RPCService and ContractMetadataService are constructed in both paths independently

## Cross-References

- For signing dependencies: [[signing]] map
- For GraphQL resolver dependencies: [[graphql-api]] map
- For ingestion service: [[ingestion]] map

---

Agent Notes:
- 2026-02-26: [[options struct plus validateoptions enables compile-time-visible dependency validation]] and [[data.models aggregates all database model interfaces into a single injectable struct]] form a complementary pair — Options struct is the container, data.Models is one of its key fields. Reading them together gives the full DI picture for complex services.
- 2026-02-26: ContractValidator is the exception to the options pattern — it takes no deps because [[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] initializes its WASM runtime internally. If you see a service listed as "no deps" in the inventory, follow wazero to understand why.
