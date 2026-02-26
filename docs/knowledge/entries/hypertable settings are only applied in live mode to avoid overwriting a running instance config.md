---
description: configureHypertableSettings runs at startup in live mode only; backfill skips it to avoid overwriting chunk intervals, retention, and compression policies of a running live instance
type: decision
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, timescaledb, backfill, hypertables, configuration]
---

# Hypertable settings are only applied in live mode to avoid overwriting a running instance config

## Context

TimescaleDB hypertable settings (chunk interval, retention policy, compression schedule) are configured at runtime via `configureHypertableSettings()`. Backfill jobs often run concurrently with a live ingestion process against the same database.

## Detail

`configureHypertableSettings()` in `internal/ingest/timescaledb.go` applies settings to all five hypertables. In `setupDeps()`, it is only called when `IngestionMode == live`. The backfill path skips it entirely.

If backfill called this function, it would overwrite the live instance's active chunk interval and compression schedule with whatever values were passed on the backfill CLI, potentially invalidating policies that were carefully tuned for production.

The function is idempotent on restart in live mode — calling it multiple times with the same config values is safe.

## Implications

- Hypertable tuning (chunk size, compression, retention) must be done through the live process, not backfill flags. Since [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]], the retention configuration applied here is the only mechanism keeping history tables bounded — no external scheduler or cron job backs it up.
- Operators running backfill alongside a live instance don't need to worry about backfill accidentally changing TimescaleDB policies.
- The settings managed here (retention policy, compression schedule, chunk interval) are the operational surface that exists because [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — each of the three configurable knobs maps directly to one of the four pillars that motivated the TimescaleDB adoption.

## Source

`internal/ingest/timescaledb.go:configureHypertableSettings()`
`internal/ingest/ingest.go:setupDeps()` — conditional call

## Related

The same mode-selection logic that prevents hypertable misconfiguration also governs [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — both entries describe configuration that is intentionally restricted to one mode to prevent interference with a concurrently running instance.

relevant_notes:
  - "[[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — synthesizes with this: both describe mode-exclusive configurations; skipped hypertable settings and synchronous_commit=off are the two backfill-only setup choices that protect a running live instance"
  - "[[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] — extends this: retention policy is the specific setting that prevents unbounded history table growth; this entry governs when it is applied (live mode only); that entry explains what it does (chunk-drop automation)"
  - "[[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — grounds this: the three configurable knobs (compression schedule, retention policy, chunk interval) are the operational surface of the four decision pillars; this entry manages what that decision introduced"
