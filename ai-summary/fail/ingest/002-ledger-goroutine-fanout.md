# H002: Unbounded per-ledger transaction fan-out lets on-chain load exhaust ingest workers

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Ledger ingestion should cap in-process transaction parallelism so one large ledger cannot create an unbounded number of concurrent buffers and worker goroutines. The ingest service should apply backpressure at the worker-pool boundary rather than scaling linearly with the raw transaction count in a single ledger.

## Mechanism

`NewIngestService` creates `ledgerIndexerPool := pond.NewPool(0)`, and `Indexer.ProcessLedgerTransactions` submits one job per transaction in the ledger. Each submitted job allocates a fresh `IndexerBuffer` and runs the full participant/state-change pipeline, so a ledger with many transactions turns directly into many concurrent goroutines and buffers with no local concurrency ceiling, giving an attacker an on-chain CPU/RAM amplification path.

## Trigger

1. Submit enough transactions to pack a ledger near the network's maximum transaction count.
2. Let ingest process that ledger in live mode.
3. Observe one `group.Submit` per transaction and matching per-transaction buffer allocation while the unbounded pond pool executes them concurrently.

## Target Code

- `internal/services/ingest.go:135-183` — creates the unbounded `ledgerIndexerPool`
- `internal/indexer/indexer.go:111-147` — submits one task per ledger transaction
- `internal/indexer/indexer.go:122-133` — allocates a new `IndexerBuffer` inside each task

## Evidence

The pool is initialized with `pond.NewPool(0)`, which removes the worker cap. `ProcessLedgerTransactions` iterates every transaction and immediately schedules a goroutine for it, rather than chunking or bounding concurrency.

## Anti-Evidence

Ledger size is bounded by Stellar protocol limits, so the fan-out is not literally infinite. But there is still no ingest-side guardrail that converts those protocol limits into a safe local concurrency limit for this process.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced from `NewIngestService` (ingest.go:135-183) through `ingestLiveLedgers` (ingest_live.go:321-380) to `processLedger` → `indexer.ProcessLedger` → `Indexer.ProcessLedgerTransactions` (indexer.go:111-151). Confirmed `pond.NewPool(0)` creates an unbounded pool and `ProcessLedgerTransactions` submits one goroutine per transaction. However, the live loop processes ledgers sequentially (one at a time), and the fan-out per ledger is bounded by Stellar protocol limits on transactions per ledger close.

### Code Paths Examined

- `internal/services/ingest.go:135-137` — `pond.NewPool(0)` creates unbounded pool; backfill pool at line 146 IS bounded (`pond.NewPool(backfillWorkers)`) showing deliberate design choice
- `internal/services/ingest_live.go:321-380` — live ingestion loop processes ledgers sequentially in a `for` loop, one at a time
- `internal/indexer/indexer.go:111-151` — `ProcessLedgerTransactions` submits one goroutine per transaction via `group.Submit`
- `internal/indexer/indexer_buffer.go:77-92` — `NewIndexerBuffer` allocates empty maps with no pre-sized capacity; lightweight per-goroutine footprint

### Why It Failed

The hypothesis fails the viability checklist on multiple grounds:

1. **No concrete attacker capability against the wallet-backend.** The trigger requires submitting transactions to the Stellar network (a separate system), not sending a request to the wallet-backend's client-facing interface (GraphQL/HTTP). The viability checklist requires "a specific request shape a network client can send" to the wallet-backend itself.

2. **Trust model excludes this attack vector.** The objective context states "The upstream Stellar RPC and history archives are **trusted**." Ledger contents — including the number of transactions per ledger — are trusted data from the upstream. An attack that requires influencing ledger contents falls outside the trust boundary.

3. **Protocol-bounded fan-out, not unbounded.** Stellar protocol limits cap the maximum transactions per ledger (typically ~200 operations on mainnet, set by validator consensus). While `pond.NewPool(0)` is technically unbounded, the actual concurrency is bounded by the network's `max_tx_set_size`. Go routinely handles hundreds or even thousands of concurrent goroutines without issue.

4. **Each `IndexerBuffer` is lightweight.** `NewIndexerBuffer()` creates a struct of empty maps — no pre-allocated data. The per-goroutine memory footprint is trivially small at the scale the protocol allows.

5. **Intentional design.** The code explicitly bounds the backfill pool (`pond.NewPool(backfillWorkers)` with a comment "bounded size to control memory usage") while leaving the per-ledger indexer pool unbounded. This is a deliberate throughput optimization for single-ledger processing, not an oversight.

### Lesson Learned

When assessing resource-amplification hypotheses against ingestion pipelines, verify that the amplification input is attacker-controlled within the trust model. Data volumes determined by the trusted upstream (Stellar network consensus) are not attacker-controlled inputs to the wallet-backend — they are the expected workload the service must handle.
