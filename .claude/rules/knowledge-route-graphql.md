---
paths:
  - "internal/serve/graphql/**"
---

# Knowledge Route: GraphQL API

Read before working in this subsystem:
- `docs/knowledge/references/graphql-api.md`
- `docs/knowledge/entries/graphql-api.md`
- Also `docs/knowledge/references/data-layer.md` if adding queries that touch data models

## Section Focus

- **Schema Design** — gqlgen workflow: edit `.graphqls` → run `make gql-generate` → never edit `generated/`
- **DataLoaders** — batch query patterns, per-request creation requirement
- **Mutations** — signing flow, `buildTransaction` returns signed XDR (never submits directly)

## Inline Gotchas

- DataLoader instances are **created per-request** — never share across requests (cross-request data leakage risk)
- `row_number() PARTITION BY` pattern ensures **fair batching** across accounts in paginated queries
- TOID bitmask (`ledger_sequence << 12 | tx_index << 4 | op_index`) avoids a join for operation ordering
- Schema-first: **never edit files in `generated/`** — they are overwritten by `make gql-generate`
