---
description: Knowledge map for the services subsystem
type: reference
subsystem: services
areas: [knowledge-map, services, dependency-injection, options-pattern]
vault: docs/knowledge
---

# Services Knowledge Map

The services layer contains business logic above the data layer. All services follow the Options struct + ValidateOptions() pattern for explicit dependency injection and testability.

**Key code:** `internal/services/`

## Reference Doc

[[references/services]] — service pattern, inventory, dependency graph

## Decisions

<!-- - [[entries/why-options-struct-pattern]] -->

## Insights

<!-- - [[entries/validate-options-construction-safety]] -->

## Patterns

<!-- - [[entries/options-struct-di-pattern]] -->

## Gotchas

<!-- No gotchas documented yet -->

## Topics

[[entries/index]] | [[references/services]]
