# Environment Variables

See `docker-compose.yaml` for full examples and default values.

## Core

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `RPC_URL` | Stellar RPC endpoint |
| `NETWORK` | Network name: `testnet` or `pubnet` |
| `NETWORK_PASSPHRASE` | Stellar network passphrase |
| `PORT` | HTTP server port (default: 8001) |
| `LOG_LEVEL` | Logging level: `debug`, `info`, `warn`, `error` |

## Authentication

| Variable | Description |
|----------|-------------|
| `CLIENT_AUTH_PUBLIC_KEYS` | Comma-separated Ed25519 public keys for JWT auth |

## Signing

| Variable | Description |
|----------|-------------|
| `DISTRIBUTION_ACCOUNT_PUBLIC_KEY` | Distribution account Stellar public key |
| `DISTRIBUTION_ACCOUNT_PRIVATE_KEY` | Distribution account private key (for ENV provider) |
| `DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER` | Signing provider: `ENV` or `KMS` |
| `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE` | Encryption key for channel account private keys in DB |
| `NUM_CHANNEL_ACCOUNTS` | Number of channel accounts to maintain |

## Ingestion

| Variable | Description |
|----------|-------------|
| `INGEST_START_LEDGER` | Ledger number to begin ingestion from |

## AWS KMS (when using KMS signing)

| Variable | Description |
|----------|-------------|
| `AWS_KMS_KEY_ARN` | ARN of the KMS key for signing |
| `AWS_REGION` | AWS region for KMS |

## Error Tracking

| Variable | Description |
|----------|-------------|
| `SENTRY_DSN` | Sentry DSN for error reporting |
| `SENTRY_ENV` | Sentry environment tag |
