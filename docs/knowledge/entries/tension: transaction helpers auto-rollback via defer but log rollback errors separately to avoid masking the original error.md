---
context: defer tx.Rollback() runs on any return path; if Rollback() itself fails, the error is logged but not returned — the original operation error is preserved as the return value
type: insight
created: 2026-02-24
---

# tension: transaction helpers auto-rollback via defer but log rollback errors separately to avoid masking the original error

All transaction helpers (`RunInTransaction`, `RunInTransactionWithResult`) follow the same error-handling pattern:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil { return err }
defer func() {
    if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
        log.Errorf("rollback error: %v", rbErr)
    }
}()
// ... operation that may return error ...
return tx.Commit()
```

The tension: when the operation inside the transaction fails and returns an error, `defer tx.Rollback()` runs — but if Rollback() also fails (rare, but possible — e.g., connection lost during rollback), there are now two errors. The helper must decide which to return.

Returning the rollback error would mask the original operation error — the caller would see a confusing "rollback failed" error rather than the actual root cause. Returning the original error would silently swallow the rollback failure.

The resolution chosen: return the original error, log the rollback error. This prioritizes debuggability for the common case (rollback errors are rare and usually indicate a connection problem that will surface through other means), while ensuring the caller always sees the error that actually caused the transaction to fail.

The `errors.Is(rbErr, sql.ErrTxDone)` check suppresses the log when rollback was called on an already-committed or already-rolled-back transaction — a normal occurrence when Commit() succeeded but defer still fires.

---

Relevant Notes:
- [[RunInTransactionWithResult generic helper avoids interface{} return type for typed results from inside transactions]] — one of the helpers that implements this pattern
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — the broader context in which transaction error handling matters

Areas:
- [[entries/data-layer]]
