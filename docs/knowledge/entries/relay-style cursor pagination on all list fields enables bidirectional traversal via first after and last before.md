---
context: Covers all three entity types (transactions, operations, state changes); bidirectional traversal enables both forward and reverse pagination without separate endpoints
type: decision
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql]
---

All list fields in the GraphQL schema use the Relay cursor connection spec: `first`/`after` for forward pagination and `last`/`before` for reverse. This is a deliberate design choice rather than offset-based pagination, which becomes expensive at depth. The cursor encodes position stably even as rows are inserted; clients can resume a page walk after a restart without skipping or duplicating records. Three cursor types handle different contexts — see [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]].

The Relay spec's cursor opacity requirement follows directly from this choice: since [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]], the debugging cost is the price paid for stable cursor evolution.

The decision to use `first`/`last` arguments on all paginated fields has a downstream consequence in complexity scoring: since [[complexity scoring defaults to 10 when neither first nor last is provided on paginated fields making unbounded queries still incur a non-zero cost]], the relay spec pagination is not enforced at the complexity layer — clients who omit pagination args bypass the intended bounding.

---

Relevant Notes:
- [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — the three cursor encodings that implement this spec
- [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]] — the spec requirement that produces cursor opacity and its debug trade-off
- [[complexity scoring defaults to 10 when neither first nor last is provided on paginated fields making unbounded queries still incur a non-zero cost]] — downstream consequence: relay spec pagination args are not enforced; clients can omit first/last and still receive a finite complexity score

Areas:
- [[entries/graphql-api]]
