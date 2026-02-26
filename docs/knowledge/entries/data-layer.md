---
description: Knowledge map for the data layer — TimescaleDB hypertables, model interfaces, query optimization patterns, and connection pools
type: reference
status: current
subsystem: data-layer
areas: [navigation, data-layer]
---

# Data Layer — Knowledge Map

The data layer provides all database access through model interfaces aggregated in `data.Models`. Five TimescaleDB hypertables hold ledger event data; three standard tables hold current balance state; metadata tables track assets and contract tokens.

## Model Architecture

- [[data.models aggregates all database model interfaces into a single injectable struct]] — The single-struct DI pattern that avoids large constructor signatures
- [[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — Why some models have interfaces and others don't; dbtest.Open for hypertable model tests
- [[deterministic uuid from content hash enables idempotent batch inserts without db roundtrip]] — UUID v5 pattern for TrustlineAsset and ContractModel; no pre-insert SELECT needed

## Why TimescaleDB

- [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — Root architecture decision: PoC evidence plus four pillars (compression, retention, chunk queries, Postgres-extension zero-migration-cost)
- [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]] — Why compression ratios are high: append-only write-once records with repeated column values compress extremely well
- [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] — add_retention_policy drops entire time chunks automatically; no cron jobs or custom DELETE logic needed

## TimescaleDB Hypertables

- [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] — The hidden session setting required for sparse index effectiveness; now includes the architectural motivation (account-history-over-time-window is the primary query shape); dbtest.Open handles this
- [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] — Why ROW() comparisons break vectorized chunk filters; the OR-expanded decomposition
- [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] — MATERIALIZED keyword is load-bearing; why LATERAL gives O(1) join per account transaction

## Balance Tables

- [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — Why current-state tables don't benefit from TimescaleDB partitioning; enriched with why Postgres-extension nature enables standard+hypertable coexistence in one database
- [[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — fillfactor=80 and all 4 autovacuum settings; why defaults are too conservative
- [[deferrable initially deferred foreign keys enable checkpoint population to insert children before parents within a single transaction]] — DEFERRABLE FK pattern on all balance table FK constraints; FK check deferred to COMMIT to allow out-of-order child/parent inserts

## Token Balance Storage

- [[G-address SAC token holdings are stored in trustline_balances not sac_balances because stellar represents them as standard trustlines]] — Critical gotcha: querying sac_balances for a G-address returns nothing even if it holds SAC tokens
- [[sac_balances exclusively tracks contract-address SAC positions because SAC balances for non-contract holders appear in their respective balance tables]] — The C-address restriction on sac_balances; symmetric complement of the G-address gotcha
- [[account_contract_tokens is append-only because sep-41 relationship tracking requires history of which contracts an account has ever interacted with]] — No DELETE path on account_contract_tokens; historical relationships vs current state

## Insert Strategies

- [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — COPY vs upsert tradeoff; when each strategy is appropriate; all four BatchUpsert calls use pgx.Batch.SendBatch() for single round-trip per ledger

## DataLoader Query Patterns

- [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — Why global LIMIT fails in DataLoader contexts; per-parent row ranking
- [[toid bit-masking recovers parent transaction id from operation id without a join]] — SEP-35 encoding; bitmask trick that avoids a lookup table

## Tensions

- Binary COPY vs conflict-safe upsert: backfill uses COPY for speed, live uses upsert for safety — different needs drive different strategies per path.
- Hypertable vs standard table: event data goes in hypertables (chunk pruning, compression), current-state balance data stays in standard tables (upsert-friendly, HOT updates). Adding a new data type requires choosing the right model.

---

Agent Notes:
- 2026-02-26: the three TimescaleDB query optimization techniques form a chain — [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] enables the sparse indexes, [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] makes cursors use them, [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] prevents the planner from undoing the work. All three must be understood together.
- 2026-02-26: [[deterministic uuid from content hash enables idempotent batch inserts without db roundtrip]] connects directly to [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — the UUID idempotency is what makes steps 1-2 of PersistLedgerData crash-safe.
- 2026-02-26: [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] has a cross-subsystem link to [[the database is the sole integration point between the serve and ingest processes]] via pillar 4 (Postgres-extension model) — the zero-migration-cost extension property is what makes the single-database integration point architecturally viable. Following this link leads from data-layer to ingestion architecture decisions.
- 2026-02-26: [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]] is the structural claim that grounds the hypertable vs standard table design split. Follow this entry to understand WHY event tables use hypertables (append-only + time-series = compression wins), then follow [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] to see the negative case: current-state tables lack both properties, which is why they stay row-oriented. This append-only insight also propagates into the ingestion subsystem: it grounds the COPY-for-backfill strategy, the safety of synchronous_commit=off, and the progressive recompression pattern.
- 2026-02-26: [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] has a cross-subsystem consequence in ingestion: the per-batch vs deferred split in [[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] is directly explained by balance table semantics. An agent investigating why catchup defers token/balance writes must read this data-layer entry to understand the answer — the current-state update model makes intermediate states wrong, which is the reason for deferral.
