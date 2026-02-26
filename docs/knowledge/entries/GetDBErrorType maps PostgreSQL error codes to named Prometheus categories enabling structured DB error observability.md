---
context: GetDBErrorType extracts the PQ error code from errors wrapped in the db layer and maps them to string labels (e.g., unique_violation, foreign_key_violation, connection_exception) used as Prometheus label values
type: pattern
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer]
---

# GetDBErrorType maps PostgreSQL error codes to named Prometheus categories enabling structured DB error observability

When database operations fail, the raw PostgreSQL error is wrapped and propagated up. At the metrics layer, distinguishing error types matters for operations: a spike in `unique_violation` errors signals a deduplication issue, while a spike in `connection_exception` errors signals a connection pool or network problem.

`GetDBErrorType()` unwraps database errors to extract the underlying PostgreSQL error code (from the `pq.Error` struct), then maps it to a human-readable Prometheus label:

| PQ Code | Label |
|---------|-------|
| `23505` | `unique_violation` |
| `23503` | `foreign_key_violation` |
| `08*` | `connection_exception` |
| `57*` | `operator_intervention` |
| ... | ... |
| unknown | `other` |

This label is used as a dimension on the database error counter metric, enabling dashboards to break down error rates by category.

The function is called at the data model layer — after a query fails — so the error type is captured close to the source, before wrapping context could obscure the original PQ code. This is why the function must unwrap through the error chain to find the `pq.Error`.

---

Relevant Notes:
- [[every processor records its own processing duration via MetricsServiceInterface for per-processor observability]] — the broader metrics instrumentation pattern; DB error classification is the data-layer equivalent

Areas:
- [[entries/data-layer]]
