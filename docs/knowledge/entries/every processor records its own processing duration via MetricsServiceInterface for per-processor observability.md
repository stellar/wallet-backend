---
context: Metrics are recorded via deferred timer.Since() calls at the start of each ProcessOperation/ProcessTransaction; label is the processor name for per-processor Prometheus breakdown
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# every processor records its own processing duration via MetricsServiceInterface for per-processor observability

Each of the nine processors injects a `MetricsServiceInterface` and records its processing duration as a histogram metric. The pattern is consistent across all processors:

```go
func (p *SomeProcessor) ProcessOperation(op Operation, tx Transaction) error {
    defer func(start time.Time) {
        p.metrics.RecordProcessorDuration("some_processor", time.Since(start))
    }(time.Now())
    // ... actual processing
}
```

The `defer` captures `time.Now()` at function entry (via the argument evaluation) and records duration on exit — correctly including error paths. The processor name label enables per-processor Prometheus breakdowns via `processor_duration_seconds{processor="some_processor"}`.

This per-processor instrumentation allows identifying which processor is the bottleneck during high-throughput backfill (e.g., `EffectsProcessor` tends to be the slowest due to its seven-category classification work).

---

Relevant Notes:
- [[nine specialized processors run per transaction in the indexer fan-out]] — all nine instrumented processors
- [[EffectsProcessor is the most complex processor handling seven distinct categories of account state changes]] — the typically slowest processor

Areas:
- [[entries/ingestion]]
