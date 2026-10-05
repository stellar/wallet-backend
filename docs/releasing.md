# Releasing

For maintainers. After reading it you can cut a release candidate, soak it, promote it to public ECR, Docker Hub and GitHub Releases, and write the notes.

## Contents

- [Cadence](#cadence)
- [Versioning](#versioning)
- [Cut a release candidate](#cut-a-release-candidate)
- [Promote](#promote)
- [Release notes](#release-notes)
- [Images](#images)
- [Planning](#planning)
- [Where in the code](#where-in-the-code)

## Cadence

| Trigger | Release |
|---|---|
| A Stellar protocol upgrade is scheduled | A release tested against the new stellar-rpc, before the upgrade date |
| A fix operators need | A patch release when it is ready |

Every release is cut from `main`. There are no release branches and no hotfix branches: a fix lands on `main` first and ships as the next release candidate.

## Versioning

Semantic versioning, tags `vMAJOR.MINOR.PATCH`, release candidates `vMAJOR.MINOR.PATCH-rc.N`.

| Change | Bump |
|---|---|
| Removing a GraphQL field or enum value, renaming a flag or env var, a migration that needs operator action beyond `migrate up` | major |
| New fields, new flags, new commands, deprecations | minor |
| Fixes with no interface change | patch |

A GraphQL field is removed only after it has carried `@deprecated(reason: ...)` for at least one minor release.

## Cut a release candidate

1. Confirm `main` is green and contains everything meant for the release.
2. Run the **Publish Prerelease** workflow with `version` set to the next `vX.Y.Z-rc.N`.
   It builds `main`, pushes the image to the staging registry with that tag, and creates a GitHub prerelease with generated notes.
3. Deploy the rc to staging and leave it there for at least one full day of ingestion. Watch `wallet_ingestion_lag_ledgers`, error rates, and the API p99.
4. A problem means a fix on `main` and a new rc (`rc.N+1`). Never patch the rc.

## Promote

1. Run the **Promote Release** workflow with `prerelease_tag` (the rc) and `release_version` (`vX.Y.Z`).
   It re-tags the exact staging digest into the production registry, into public ECR as `public.ecr.aws/stellar/wallet-backend:vX.Y.Z` and `:latest`, and into Docker Hub as `stellar/wallet-backend:vX.Y.Z` and `:latest`, verifies every digest matches, and creates the GitHub release.
2. Open the release and fill the two `<fill in>` lines under **Tested with** (stellar-rpc version and protocol number). PostgreSQL and TimescaleDB versions are filled from the workflow.
3. Add an **Upgrade notes** section when operators must do anything beyond pulling the image and running `migrate up`.

The binary inside a promoted image reports the rc version it was built as (for example `v1.2.0-rc.2`) plus the commit, because promotion copies the digest instead of rebuilding. The GitHub release and image tags carry the final version.

## Release notes

Notes are generated from merged PR titles, grouped by label (`.github/release.yml`):

| Label | Section |
|---|---|
| `breaking` | Breaking changes |
| `feature`, `enhancement` | Features |
| `fix`, `bug` | Fixes |
| `docs`, `documentation` | Documentation |
| `dependencies` | Dependencies |
| `ci` | CI and tooling |
| `skip-changelog` | excluded |

A PR title is therefore a changelog line: imperative, specific, under 72 characters.

## Images

| Registry | Tags | Who |
|---|---|---|
| `public.ecr.aws/stellar/wallet-backend` | `vX.Y.Z`, `latest` | public, primary; the repository is declared in `stellar/terraform` |
| `docker.io/stellar/wallet-backend` | `vX.Y.Z`, `latest` | public, kept for existing users until it is deprecated; same digest |
| staging registry | `vX.Y.Z-rc.N`, commit SHA | internal |
| production registry | `vX.Y.Z` | internal |

`latest` always points at the newest promoted release. Operators should pin `vX.Y.Z`.

## Planning

Open a GitHub milestone named for the next version when work for it starts. Close it on promotion.

## Where in the code

| Path | Role |
|---|---|
| `.github/workflows/publish-prerelease.yml` | Build and tag an rc |
| `.github/workflows/promote-release.yml` | Re-tag, public ECR and Docker Hub push, GitHub release |
| `.github/workflows/build.yml` | Commit-SHA builds for every push to `main` |
| `.github/release.yml` | Release-notes categories |
| `Makefile` (`docker-build`, `VERSION`) | Build args and labels |
| `main.go`, `cmd/version.go` | Version injection and the `version` command |
