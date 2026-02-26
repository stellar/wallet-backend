---
context: Format is "to_id-op_id-sc_order"; the dataloader splits on dash to recover all three components for grouping
type: insight
created: 2026-02-24
status: current
subsystem: ingestion
areas: [ingestion, indexer]
---

State changes have no single-column primary key — their position is determined by three values: the transaction TOID (`to_id`), the operation TOID (`op_id`), and a sequencing index within the operation (`sc_order`). The state change dataloader encodes these as a dash-separated string key (e.g., `"1234567890-1234567902-0"`) to use as a map key for grouping. On the way back, the key is split on `-` to recover the three components. This encoding is an implementation artifact, not a schema-level concept — the GraphQL `id` field on state changes uses this format.
