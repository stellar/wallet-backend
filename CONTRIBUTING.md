# Contributing

For anyone sending a change to wallet-backend. After reading you can set up a local stack, run the checks, and open a pull request that is ready for review.

## Before you start

- Pull requests target `main`.
- Open an issue first for anything beyond a small fix, such as a typo or a broken link.
- The [Stellar Contribution Guide](https://github.com/stellar/.github/blob/master/CONTRIBUTING.md) applies to every Stellar repository, including this one.
- The [Code of Conduct](https://github.com/stellar/.github/blob/master/CODE_OF_CONDUCT.md) applies here too.

## Set up

1. Install the Go version named in `go.mod` (Go 1.25.9). Verify: `go version` prints `go1.25.9` or later.
2. Start TimescaleDB, Stellar RPC, and a debug build of the service. Verify: `docker compose ps` lists every service as running.

   ```bash
   docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up --build
   ```

3. Run `make unit-test`. Verify: no `FAIL` lines in the output.
4. Run `make integration-test`. It needs Docker and takes about 30 minutes. Verify: it exits with status 0.
5. Run `make check` before you push. Verify: it exits with status 0 and `git status` shows no files it changed.

## Pull requests

- One logical change per pull request.
- The title is the changelog line: imperative, 72 characters or fewer, no ticket prefix.
- Apply one label so the change lands in the right release-notes section.
- Link the issue the pull request resolves.
- Fill in the pull request template.
- CI must be green.
- One approval from a code owner is required to merge.

| Label | Release-notes section |
| --- | --- |
| `breaking` | Breaking changes |
| `feature` or `enhancement` | Features |
| `fix` or `bug` | Fixes |
| `docs` or `documentation` | Documentation |
| `dependencies` | Dependencies |
| `ci` | CI and tooling |
| `skip-changelog` | Left out of release notes |

## Commits

- Write a short imperative subject line.
- Use the body to explain why the change is needed.
- Squash or rebase away "WIP" commits before the branch merges.

## Schema and interface changes

| Change | What to do |
| --- | --- |
| GraphQL schema | Edit the `.graphqls` file, then run `make gql-generate` and `make gql-docs`. |
| Go interface | Update its hand-written `testify/mock` in the package's `mocks.go` in the same commit. |
| Database schema | Add a file under `internal/db/migrations/` with both a `-- +migrate Up` and a `-- +migrate Down` block. |
| Applied migration | Never edit it. Add a migration that changes it. |

## Docs

Docs live in `docs/`. Run `make docs-check` before you push a docs change. It runs the markdown lint, the link check, the schema-reference freshness check, and the config-reference check.

## Where in the code

| Path | Role |
| --- | --- |
| `Makefile` | Test, check, and codegen targets |
| `.github/release.yml` | Label to release-notes mapping |
| `internal/db/migrations/` | SQL migrations |
