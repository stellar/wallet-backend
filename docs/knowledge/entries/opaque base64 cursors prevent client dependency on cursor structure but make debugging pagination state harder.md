---
context: Opacity is deliberate (prevents clients from constructing cursors) but adds indirection when inspecting raw API responses during development
type: question
created: 2026-02-24
---

All pagination cursors in the API are base64-encoded opaque strings. The opacity prevents clients from relying on cursor internals (which could change with schema evolution) but it has a real cost: debugging a pagination issue requires decoding the base64 string to inspect the position encoded inside. This tension is unresolved — the Relay spec recommends opaque cursors precisely for this reason, but developer tooling (e.g., browser devtools inspection) becomes harder. A potential mitigation is a debug header that logs decoded cursor values server-side without exposing them in the response.

Since [[relay-style cursor pagination on all list fields enables bidirectional traversal via first after and last before]] mandates the Relay spec, cursor opacity follows as a design consequence, not an independent choice. The opacity also compounds the complexity of [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — because cursor encodings are hidden, mixing cursor types across query contexts produces silently incorrect pagination with no visible hint of which encoding was used.

---

Relevant Notes:
- [[relay-style cursor pagination on all list fields enables bidirectional traversal via first after and last before]] — the Relay spec adoption is what drives cursor opacity; the debugging cost follows from this decision
- [[three cursor types serve different query contexts with CursorTypeComposite for root queries and CursorTypeStateChange for state changes]] — opacity means wrong-cursor-type errors are silent; the debug cost is higher when multiple encodings exist

Areas:
- [[entries/graphql-api]]
