# H001: GraphQL Column/Sort SQL Injection Through Dynamic SELECT Clauses

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: High
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Only statically known database column names and sort directions should ever reach the SQL-building helpers. An attacker-controlled GraphQL request should not be able to splice arbitrary SQL into the `SELECT` list or `ORDER BY` clause.

## Mechanism

`prepareColumnsWithID` splices the caller-supplied `columns` string directly into SQL, and several query builders interpolate `SortOrder` with `fmt.Sprintf`. If a GraphQL request could smuggle raw text into either value, it would turn the read path into an injection primitive.

## Trigger

Attempt to supply malicious field names or sort-direction text through GraphQL so the generated `columns` string or `SortOrder` contains SQL metacharacters.

## Target Code

- `internal/data/query_utils.go:162-184` — direct column-list splicing sink
- `internal/data/operations.go:124-149` — `SortOrder` interpolation sink
- `internal/serve/graphql/resolvers/utils.go:167-183,229-267` — GraphQL field-to-column mapping
- `internal/serve/graphql/resolvers/utils.go:288-341` — pagination parsing sets sort order

## Evidence

The data layer really does splice `columns` and `SortOrder` into query strings. At first glance that looks like a classic SQL-injection surface.

## Anti-Evidence

The GraphQL layer does not forward raw user strings into either value. `GetDBColumnsForFields` maps collected schema field names through hard-coded struct tags, and `parsePaginationParams` only emits the typed constants `ASC` or `DESC`.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-23
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The dangerous SQL-building helpers are present, but the active GraphQL call path feeds them only schema-derived column names and fixed sort constants, not attacker-controlled text.

### Lesson Learned

For this repository, dynamic SQL sinks in `internal/data` must be paired with a concrete caller that converts network input into `columns` or `SortOrder`. The sink alone is not enough because the resolver layer currently closes those values over gqlgen metadata and typed enums.
