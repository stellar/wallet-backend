# Contributing to Wallet Backend

Go backend service for Stellar wallet applications. Provides a REST API (Chi
router) for transaction submission, account management, payment history, and
channel accounts with fee sponsorship.

For the Stellar organization's general contribution guidelines, see the
[Stellar Contribution Guide](https://github.com/stellar/.github/blob/master/CONTRIBUTING.md).

## Prerequisites

| Tool             | Version   | Install                                                                    |
| ---------------- | --------- | -------------------------------------------------------------------------- |
| Go               | >= 1.23.2 | [go.dev/dl](https://go.dev/dl/)                                           |
| Docker + Compose | Latest    | [docker.com](https://docs.docker.com/get-docker/)                         |
| golangci-lint    | Latest    | `brew install golangci-lint` or [docs](https://golangci-lint.run/)         |

PostgreSQL 14 is provided via Docker Compose — no manual install needed.

## Getting Started

### Quick Setup with an LLM

If you use an LLM-powered coding assistant, you can automate the setup. The repo
includes a quick start guide ([`quick-start-guide.md`](quick-start-guide.md)) that
checks your environment, installs missing tools, configures `.env`, and verifies
the build.

Point your LLM assistant at `quick-start-guide.md` and ask it to follow the steps.

If you don't use an LLM assistant, follow the manual setup below.

### Docker Compose (quickest path)

```bash
git clone https://github.com/stellar/wallet-backend.git
cd wallet-backend
cp .env.example .env
# Fill in required values (see Environment Variables below)
NETWORK=testnet docker compose up
```

This starts:
- **PostgreSQL 14** (`db`) — database on port 5432
- **Stellar RPC** (`stellar-rpc`) — blockchain data source
- **API server** (`api`) — REST endpoint
- **Ingestion service** (`ingest`) — indexes Stellar ledger data

### Local Development (Go binary + Docker services)

For active development:

```bash
cp .env.example .env
set -a
source .env
set +a

# Start dependencies only
NETWORK=testnet docker compose up -d db stellar-rpc

# Run migrations
go run main.go migrate up

# Create channel accounts (for fee sponsorship)
go run main.go channel-account ensure 5

# Start API and ingestion in separate terminals
go run main.go serve     # Terminal 1
go run main.go ingest    # Terminal 2
```

### Environment Variables

Copy `.env.example` to `.env`. Required variables from `.env.example`:

| Variable                                  | How to set up                                                            |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| `CLIENT_AUTH_PUBLIC_KEYS`                 | Generate a Stellar keypair and use the public key                        |
| `DISTRIBUTION_ACCOUNT_PUBLIC_KEY`         | Generate a Stellar keypair for the distribution account                  |
| `DISTRIBUTION_ACCOUNT_PRIVATE_KEY`        | Private key for the distribution account (use `ENV` signature provider)  |
| `DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER` | `ENV` for local dev (uses env var above). `KMS` for production           |
| `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE`   | Any passphrase for encrypting channel account keys                       |

**Not in `.env.example` but needed for local Go binary dev** (Docker Compose
sets these internally for containerized services):

| Variable             | Value for local dev                                                 |
| -------------------- | ------------------------------------------------------------------- |
| `DATABASE_URL`       | `postgres://postgres@localhost:5432/wallet-backend?sslmode=disable` |
| `NETWORK`            | `testnet` (recommended) or `pubnet`                                 |
| `NETWORK_PASSPHRASE` | `Test SDF Network ; September 2015` (testnet)                       |
| `RPC_URL`            | `http://localhost:8000` for host-side `go run`. Do **not** set this in `.env` for containerized services; inside Docker they use `http://stellar-rpc:8000`. |
| `STELLAR_ENVIRONMENT`| `development` for local dev                                        |

For integration tests, also set `CLIENT_AUTH_PRIVATE_KEY`,
`PRIMARY_SOURCE_ACCOUNT_PRIVATE_KEY`, and
`SECONDARY_SOURCE_ACCOUNT_PRIVATE_KEY`.

## Key Commands

```bash
go run main.go serve        # Start API server
go run main.go ingest       # Start ingestion service
go run main.go migrate up   # Run database migrations
go test ./...               # Run unit tests (requires DATABASE_URL)
```

The Makefile provides Docker targets: `make docker-build`, `make docker-push`.

## Code Conventions

- **Formatting:** `gofmt`. Enforced by `golangci-lint`.
- **Linting:** `golangci-lint`. Config in `.golangci.yml`.
- **Tests:** All changes must be covered by tests. Coverage threshold: 65%
  (enforced in CI).
- **Documentation:** Exported functions, types, and constants must have doc
  comments per [Effective Go](https://golang.org/doc/effective_go.html#commentary).

## Testing

**Unit tests:** Require `DATABASE_URL` to be set.

```bash
go test ./...
```

**Integration tests:** Require `db` and `stellar-rpc` Docker services running.

```bash
go run main.go integration-tests
```

## Pull Requests

- Branch from `develop` (not `main`)
- PR titles start with: `feat`, `fix`, `refactor`, `ci`, or `doc`
- No mixed concerns — keep refactoring separate from features
- Code must be formatted with `gofmt`
- All changes must be covered by tests
- Follow [Effective Go](https://golang.org/doc/effective_go.html) and
  [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

**CI runs on every PR:** lint + format checks, build, unit tests (65% coverage
threshold), integration tests. See `.github/workflows/go.yaml`.

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- Start with the relevant issue number: `#123 Fix bug in XYZ module`

## Related Repositories

This repo (`stellar/wallet-backend`) is a general-purpose Stellar wallet
backend service. It is **separate** from the Freighter-specific backends:

- [stellar/freighter-backend](https://github.com/stellar/freighter-backend)
  (TypeScript) — V1 backend powering Freighter's balances, subscriptions,
  feature flags
- [stellar/freighter-backend-v2](https://github.com/stellar/freighter-backend-v2)
  (Go) — V2 backend powering collectibles, RPC health, protocols

## Security

This service handles distribution account keys and channel account keys.

- **Never log private keys** or encryption passphrases
- **Use `ENV` signature provider** for local dev only — production uses KMS
- **Validate all inputs** — the API is exposed to wallet clients
- **Report vulnerabilities** via the
  [Stellar Security Policy](https://github.com/stellar/.github/blob/master/SECURITY.md)
  — not public issues

## Code of Conduct

See the [Stellar Code of Conduct](https://github.com/stellar/.github/blob/master/CODE_OF_CONDUCT.md).
