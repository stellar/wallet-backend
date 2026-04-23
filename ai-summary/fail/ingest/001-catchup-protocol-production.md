# H001: Catchup mode skips protocol state production after a lagged restart

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: High
**Impact**: persistent state corruption
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

If protocol processors are active in production, the catchup path should either run their per-ledger production logic or prove that no protocol state is lost while `latest_ingest_ledger` advances across the catchup range.

## Mechanism

`startLiveIngestion` can call `startBackfilling(..., BackfillModeCatchup)` before resuming live ingestion, and the catchup batch path never calls `protocolProcessorsEligibleForProduction` or `produceProtocolStateForProcessors`. That looks like a gap where protocol history/current-state production could be skipped for ledgers processed during catchup.

## Trigger

1. Let live ingest fall behind far enough to enter catchup mode.
2. Ensure protocol processors are expected to persist state for ledgers in the catchup range.
3. Restart ingest and watch catchup advance the latest cursor without invoking the live protocol-production path.

## Target Code

- `internal/services/ingest_live.go:startLiveIngestion:277-290` — enters catchup mode before live streaming
- `internal/services/ingest_backfill.go:processLedgersInBatch:506-585` — catchup batch path that skips protocol production

## Evidence

The live path calls `produceProtocolStateForProcessors`, but the catchup batch path only processes ledger/indexer data and post-merges token/cache changes. There is no equivalent protocol-production call in catchup mode.

## Anti-Evidence

The current tree has no production `RegisterProcessor(...)` call sites or production `ProtocolProcessor` implementation files, so `GetAllProcessors()` resolves to an empty registry on main. That makes the apparent omission unreachable today.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The bug only becomes real after protocol processors are registered in production. On the current main branch, the processor registry is empty, so catchup is not skipping any reachable protocol-production work.

### Lesson Learned

Recent protocol-infrastructure code in `internal/services` is partially dormant on main. Before filing a protocol-state hypothesis, confirm there is an actual production `ProtocolProcessor` registration path.
