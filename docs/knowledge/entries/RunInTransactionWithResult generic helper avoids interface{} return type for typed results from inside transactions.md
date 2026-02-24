---
context: Go 1.18+ generic function RunInTransactionWithResult[T any](ctx, db, fn func(tx) (T, error)) (T, error) lets callers return typed values from transactional closures without type assertions
type: pattern
created: 2026-02-24
---

# RunInTransactionWithResult generic helper avoids interface{} return type for typed results from inside transactions

`RunInTransaction` wraps a closure in a database transaction with automatic rollback on error. The signature requires the closure to return only an `error` — any result value must be communicated via closure-captured variables:

```go
var result MyType
err := db.RunInTransaction(ctx, func(tx Transaction) error {
    var err error
    result, err = doSomething(tx)
    return err
})
```

This works but is ergonomically awkward and requires a pre-declared variable outside the closure. Before Go generics, returning a typed result from a transaction helper required either: (a) the captured-variable pattern above, or (b) returning `interface{}` and requiring callers to type-assert.

`RunInTransactionWithResult[T]` uses Go 1.18+ generics to solve this cleanly:

```go
result, err := RunInTransactionWithResult(ctx, db, func(tx Transaction) (MyType, error) {
    return doSomething(tx)
})
```

The generic parameter `T` is inferred from the closure's return type. The helper handles rollback-on-error and returns both the typed result and error to the caller. This eliminates the captured-variable boilerplate and avoids `interface{}` anywhere in the call chain.

The rollback behavior follows the same pattern as `RunInTransaction`: `defer tx.Rollback()` with the rollback error logged (not returned) to avoid masking the original operation error.

---

Relevant Notes:
- [[tension: transaction helpers auto-rollback via defer but log rollback errors separately to avoid masking the original error]] — the error-handling design in all transaction helpers including this one

Areas:
- [[entries/data-layer]]
