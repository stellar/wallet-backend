---
context: Trustline flags (AUTHORIZED, AUTHORIZED_TO_MAINTAIN_LIABILITIES, CLAWBACK_ENABLED) are stored as a 3-bit bitmask; SMALLINT enables bitwise WHERE clauses without array containment operators
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# flag encoding uses bitmask SMALLINT instead of JSON array for compact storage and efficient querying

Trustline and account flag sets (e.g., `AUTHORIZED`, `AUTHORIZED_TO_MAINTAIN_LIABILITIES`, `CLAWBACK_ENABLED`) are stored in the database as a `SMALLINT` bitmask rather than a `text[]` array or JSONB. Each flag corresponds to a specific bit position matching Stellar's XDR flag constants.

Reading/writing:
```go
// Encoding (Go → DB)
flags := int16(0)
if trustline.Flags.IsAuthorized() { flags |= 1 }
if trustline.Flags.IsAuthorizedToMaintainLiabilities() { flags |= 2 }
if trustline.Flags.IsClawbackEnabled() { flags |= 4 }

// Querying authorized trustlines
WHERE flags & 1 = 1
```

This enables efficient bitwise queries that PostgreSQL can evaluate using a standard B-tree index scan with a predicate. A JSON array would require `@>` containment operators or `unnest()` expansions that cannot use standard indexes. The `SMALLINT` bitmask also requires 2 bytes vs 50-200 bytes for a text array of flag names.

The tradeoff: adding a new flag requires a schema migration to document the new bit position and update application constants.

---

Relevant Notes:
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — these processors populate the flag fields

Areas:
- [[entries/ingestion]]
