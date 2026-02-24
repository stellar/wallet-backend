---
context: CursorTypeInt64 for nested ops under a known tx, CursorTypeComposite for root tx/op list queries, CursorTypeStateChange for state change pagination
type: insight
created: 2026-02-24
---

The cursor system has three encodings serving distinct access patterns. `CursorTypeInt64` is used for operations nested under a parent transaction where the parent's TOID is already known — the cursor is just the operation index. `CursorTypeComposite` is used for root-level transaction and operation list queries where both ledger sequence and transaction/operation index must be encoded together to uniquely identify position. `CursorTypeStateChange` encodes a three-part key (`to_id-op_id-sc_order`) for state change queries, which have a three-dimensional position. Mixing cursor types across query contexts produces silently incorrect pagination boundaries.

Since [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]], the silent-failure risk of mixing cursor types is compounded — there is no visible hint in the API response about which encoding was used when the wrong cursor produces unexpected results.

---

Relevant Notes:
- [[relay-style cursor pagination on all list fields enables bidirectional traversal via first after and last before]] — the Relay spec decision that requires this cursor system
- [[opaque base64 cursors prevent client dependency on cursor structure but make debugging pagination state harder]] — opacity makes mixed-cursor-type errors harder to detect; the three encodings make this a real risk

Areas:
- [[entries/graphql-api]]
