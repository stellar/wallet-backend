---
description: Category (BALANCE, SIGNER, FLAGS, etc.) classifies what changed; Reason (DEBIT, ADD, SET, etc.) classifies how it changed; 4 processors produce all 9 categories via different code paths
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, state-changes, indexer, taxonomy]
---

# State changes use a two-axis category-reason taxonomy to classify every account history event

## Context

Account history contains heterogeneous events — payments, signer updates, flag changes, sponsorships, contract deployments. A flexible taxonomy is needed that allows filtering and grouping without a separate table per event type.

## Detail

Every `StateChange` has:
- **Category** (what changed): `BALANCE`, `ACCOUNT`, `SIGNER`, `SIGNATURE_THRESHOLD`, `METADATA`, `FLAGS`, `TRUSTLINE`, `RESERVES`, `BALANCE_AUTHORIZATION`, `AUTHORIZATION`
- **Reason** (how it changed): `DEBIT`, `CREDIT`, `MINT`, `BURN`, `ADD`, `REMOVE`, `UPDATE`, `CREATE`, `MERGE`, `LOW`, `MEDIUM`, `HIGH`, `HOME_DOMAIN`, `DATA_ENTRY`, `SET`, `CLEAR`, `SPONSOR`, `UNSPONSOR`

The mapping is enforced at the schema level (CHECK constraints in the migration) and in the `StateChangeBuilder` fluent API.

Four processors produce state changes:
- `TokenTransferProcessor` (tx-level) → `BALANCE` and `ACCOUNT` categories
- `EffectsProcessor` (op-level) → 7 remaining categories
- `ContractDeployProcessor` (op-level) → `ACCOUNT/CREATE`
- `SACEventsProcessor` (op-level) → `BALANCE_AUTHORIZATION`

`BALANCE_AUTHORIZATION` has two producers — `EffectsProcessor` for classic trustline auth and `SACEventsProcessor` for Soroban SAC auth.

## Implications

- The GraphQL `AccountStateChangeFilterInput` accepts category + reason combinations — both are strongly typed enums.
- The single wide `state_changes` table stores all categories. Most fields are nullable — only the fields relevant to a given category are populated.
- Adding a new category requires: constants in `types.go`, CHECK constraint update in migration, new GraphQL type in `statechange.graphqls`, and dispatch in `convertStateChangeTypes()`.

## Source

`internal/indexer/types/types.go:StateChangeCategory`, `StateChangeReason` (lines 439–475)
`internal/serve/graphql/schema/enums.graphqls` — GraphQL enum definitions
`internal/db/migrations/2025-06-10.4-statechanges.sql` — CHECK constraints

## Related

Since [[state change ordering uses sortkey for deterministic re-ingestion of the same ledger]], the taxonomy's category and reason fields are part of the 18-field SortKey — the classification system directly enables deterministic re-ordering of heterogeneous events.

relevant_notes:
  - "[[state change ordering uses sortkey for deterministic re-ingestion of the same ledger]] — extends this entry: the taxonomy fields are embedded in the SortKey, meaning the classification scheme is what makes deterministic ordering possible"
