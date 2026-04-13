# Wallet Backend — Quick Start Guide

Evaluate the contributor's machine against all prerequisites for wallet-backend
(Go service), install what's missing, and run the initial setup.

## Step 1: Check all prerequisites

Run every check below and collect results. Report all at once — don't stop at
the first failure.

For each tool, try the version command first. If it fails (e.g., sandbox
restrictions), fall back to `which <tool>` to confirm presence.

```bash
# Go >= 1.23.2
go version 2>&1 || which go

# Docker
docker --version 2>&1 || which docker

# Docker Compose
docker compose version 2>&1 || docker-compose --version 2>&1 || which docker-compose

# golangci-lint
golangci-lint --version 2>&1 || which golangci-lint

# Make
make --version 2>&1 || which make

# PostgreSQL client (optional — for debugging)
psql --version 2>&1 || echo "psql not installed (optional)"
```

## Step 2: Present results

Show a clear summary:

```
Wallet Backend — Prerequisites Check
=====================================
  Go             1.23.x         >= 1.23.2 required    OK
  Docker         27.x.x         any                   OK
  Docker Compose 2.x.x          any                   OK
  golangci-lint  1.x.x          any                   OK
  Make           3.x            any                   OK
  psql           16.x           optional              OK
```

## Step 3: Install missing tools

Present the missing tools and ask the user: "I can install [list] automatically.
Want me to proceed?"

If the user confirms, **run the install commands** for each missing tool. After
each install, re-check the version to confirm it succeeded. If an install fails,
report the error and continue with the next tool.

If the user declines, skip to Step 4 and note the missing tools in the final
summary.

**Auto-installable (run after user confirms):**

- **Go**: Download from [go.dev/dl](https://go.dev/dl/) or
  `brew install go` (macOS) / `sudo apt install golang-go` (Linux)
- **Docker**: `brew install --cask docker` (macOS) or follow
  [docs.docker.com](https://docs.docker.com/engine/install/) (Linux)
- **golangci-lint**: `brew install golangci-lint` (macOS) or
  `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.2`

## Step 4: Configure environment

Check if `.env` exists. If not:

```bash
cp .env.example .env
```

Then read `.env` and check which required variables are empty. For each empty
required variable, tell the user the value or how to set it up:

**Variables from `.env.example`:**

| Variable                                  | Value or setup                                                           |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| `CLIENT_AUTH_PUBLIC_KEYS`                 | Generate a Stellar keypair (e.g., via `stellar-sdk` or Stellar Laboratory) and use the public key |
| `DISTRIBUTION_ACCOUNT_PUBLIC_KEY`         | Generate another Stellar keypair for the distribution account            |
| `DISTRIBUTION_ACCOUNT_PRIVATE_KEY`        | Private key of the distribution account keypair                          |
| `DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER` | `ENV` for local dev                                                      |
| `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE`   | Any passphrase (e.g., `local-dev-passphrase`)                            |

**Variables NOT in `.env.example` but needed for local Go binary dev:**

For containerized services, `docker-compose.yaml` supplies some of these values,
but `NETWORK` must still be set for Docker Compose as well (it is referenced as
`${NETWORK}` with no default). If running `go run main.go` directly on the host,
add these to `.env` or export them in your shell:

| Variable             | Value for local dev                                                    |
| -------------------- | ---------------------------------------------------------------------- |
| `DATABASE_URL`       | `postgres://postgres@localhost:5432/wallet-backend?sslmode=disable`    |
| `NETWORK`            | `testnet` (recommended for dev) or `pubnet`                            |
| `NETWORK_PASSPHRASE` | `Test SDF Network ; September 2015` (testnet)                          |
| `RPC_URL`            | `http://localhost:8000` for host-side `go run`. Do **not** set this in `.env` for containerized services; inside Docker they use `http://stellar-rpc:8000`. |
| `STELLAR_ENVIRONMENT`| `development` for local dev                                                   |

Skip any variable that already has a value.

## Step 5: Run initial setup

Ask the user which setup path they prefer:

### Option A: Full Docker Compose (simplest)

```bash
NETWORK=testnet docker compose up
```

This starts everything: Postgres, Stellar RPC, API server, and ingestion.

### Option B: Local Go binary + Docker services (for active development)

```bash
# Start dependencies
NETWORK=testnet docker compose up -d db stellar-rpc

# Wait for DB to be ready, then run migrations
go run main.go migrate up

# Create channel accounts for fee sponsorship
go run main.go channel-account ensure 5

# Start API and ingestion (separate terminals)
go run main.go serve     # Terminal 1
go run main.go ingest    # Terminal 2
```

## Step 6: Verify

```bash
golangci-lint run  # Run lint checks
go test ./...      # Run unit tests (requires DATABASE_URL)
```

If both pass, the setup is working.

## Step 7: Summary

At the end, produce a final summary:

```
Setup Complete
==============
  Prerequisites: [list with versions]
  Installed: [list of tools installed]
  Configured: .env with required variables

  Next steps:
  1. NETWORK=testnet docker compose up          (full setup)
     OR
  1. NETWORK=testnet docker compose up -d db stellar-rpc
  2. go run main.go migrate up
  3. go run main.go serve       (terminal 1)
  4. go run main.go ingest      (terminal 2)

  Manual action needed:
  - [ ] Generate Stellar keypairs for CLIENT_AUTH and DISTRIBUTION_ACCOUNT
  - [ ] Set CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE
```
