---
context: SAC event parsing is best-effort; malformed or unrecognized event formats log a warning and skip rather than aborting the transaction persist; creates silent data gaps
type: gotcha
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# SACEventsProcessor silently continues on extraction errors rather than failing the entire transaction

`SACEventsProcessor` wraps its event extraction logic in error-tolerant loops: when parsing a SAC diagnostic event fails (malformed XDR, unrecognized function name, unexpected topic count), the processor logs a warning and continues to the next event rather than returning an error.

```go
for _, event := range sacEvents {
    stateChange, err := p.extractEvent(event, op, tx)
    if err != nil {
        p.log.Warnf("failed to extract SAC event: %v", err)
        continue  // silent skip
    }
    results = append(results, stateChange)
}
```

The rationale is fault tolerance: a single malformed SAC event (e.g., from a future protocol change or a non-standard token contract) should not abort processing of the entire transaction. The transaction itself succeeded on-chain; failing ingestion because of a parsing issue would cause unnecessary re-processing and log noise.

The risk: a systematic parsing bug (like the version-specific topic layout issue in `SACEventsProcessor adapts to tx meta version differences`) would silently produce gaps in SAC event coverage across many ledgers before being detected in monitoring.

---

Relevant Notes:
- [[SACEventsProcessor adapts to tx meta version differences for set_authorized event topic layout]] — example of parsing variation that could trigger this path
- [[SACEventsProcessor handles authorization differently for classic accounts vs contract accounts because their authorization storage differs]] — another parsing complexity in the same processor

Areas:
- [[entries/ingestion]]
