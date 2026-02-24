---
context: Covers all three entity types (transactions, operations, state changes); bidirectional traversal enables both forward and reverse pagination without separate endpoints
type: decision
created: 2026-02-24
---

All list fields in the GraphQL schema use the Relay cursor connection spec: `first`/`after` for forward pagination and `last`/`before` for reverse. This is a deliberate design choice rather than offset-based pagination, which becomes expensive at depth. The cursor encodes position stably even as rows are inserted; clients can resume a page walk after a restart without skipping or duplicating records. Three cursor types handle different contexts — see [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]].

The Relay spec's cursor opacity requirement follows directly from this choice: since [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]], the debugging cost is the price paid for stable cursor evolution.

---

Relevant Notes:
- [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — the three cursor encodings that implement this spec
- [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]] — the spec requirement that produces cursor opacity and its debug trade-off

Areas:
- [[entries/graphql-api]]
