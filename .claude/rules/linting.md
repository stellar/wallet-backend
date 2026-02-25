# Linting

Configured in `.golangci.yml`. Key rules:

- `wrapcheck`: Errors must be wrapped (except in test files)
- `errcheck`: Blank identifier errors flagged (`_ = someFunc()`)
- `errcheck` relaxed for `.Close()` and `.Rollback()` in test/dbtest files
- Mock files (`*.mock.*.go`) are excluded from all linters
- Imports organized with local prefix `github.com/stellar/wallet-backend` (enforced by `goimports`)

## Quick Reference

```bash
make check       # Run ALL checks (do this before pushing)
make lint        # golangci-lint only
make tidy        # Format code + fix imports + tidy go.mod
make goimports   # Import organization check
```
