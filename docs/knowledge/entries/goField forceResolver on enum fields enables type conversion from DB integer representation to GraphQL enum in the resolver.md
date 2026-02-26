---
context: DB stores enums as integers for compact storage; the resolver converts to the GraphQL string enum; without forceResolver gqlgen reads the raw integer
type: pattern
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql]
---

Beyond relationship fields, `@goField(forceResolver: true)` is also required on enum fields like `type` and `reason` on `BaseStateChange`. The DB stores these values as SMALLINT (bitmask or integer code) while the GraphQL schema exposes them as string enums. The resolver method performs the integer-to-enum conversion. Without `forceResolver: true`, gqlgen would expose the raw integer field value directly — a type mismatch that would either cause a runtime panic or return an unexpected numeric value to the client. See also [[goField forceResolver is required on relationship fields to enable lazy dataloader loading instead of direct struct field access]] for the relationship case.
