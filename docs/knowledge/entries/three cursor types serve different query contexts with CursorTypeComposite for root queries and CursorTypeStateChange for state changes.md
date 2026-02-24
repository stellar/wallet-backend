---
context: CursorTypeInt64 for nested ops under a known tx, CursorTypeComposite for root tx/op list queries, CursorTypeStateChange for state change pagination
type: insight
created: 2026-02-24
---

The cursor system has three encodings serving distinct access patterns. `CursorTypeInt64` is used for operations nested under a parent transaction where the parent's TOID is already known — the cursor is just the operation index. `CursorTypeComposite` is used for root-level transaction and operation list queries where both ledger sequence and transaction/operation index must be encoded together to uniquely identify position. `CursorTypeStateChange` encodes a three-part key (`to_id-op_id-sc_order`) for state change queries, which have a three-dimensional position. Mixing cursor types across query contexts produces silently incorrect pagination boundaries.
