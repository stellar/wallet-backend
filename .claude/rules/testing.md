# Testing Conventions

## Mocking

- Generated via `mockery` (config in `.mockery.yml`), output to `mocks.go` in the same package
- Most mocks are hand-written using `testify/mock`
- Pattern: `NewMock<InterfaceName>(t)` with auto-cleanup
- If you change an interface, regenerate its mock

## Database Tests

- Use `dbtest.Open(t)` which creates an isolated TimescaleDB instance with migrations applied
- The `internal/db/dbtest` package auto-enables the TimescaleDB extension and chunk skipping

## Test Structure

- **Table-driven tests** with `t.Run` subtests
- **Assertions**: `require` for must-pass checks, `assert` for non-fatal. Both from `testify`

## Integration Tests

- Controlled by `ENABLE_INTEGRATION_TESTS=true` env var
- Use `testcontainers-go` to spin up Docker containers
- CI coverage minimum: 65%

## Task Completion Checklist

Before considering a task complete, run these checks:

1. `make tidy` — format code, fix imports, tidy go.mod
2. `make check` — run ALL linters and static analysis
3. `make unit-test` — run all unit tests with race detection
4. `make gql-generate` — if you edited any `.graphqls` schema files
5. Regenerate mocks — if you changed any interfaces (run `mockery` or rebuild manually)
