# Wallet-Backend

A backend for Stellar wallet applications that provides transaction submission, account management, and 
payment tracking capabilities. 

## Overview

The wallet-backend service provides several key functionalities:

- Account sponsorship and creation
- Transaction submission and tracking
- Payment history tracking
- Fee bump transaction support
- Channel account management
- Transaction submission service (TSS)

## Local Development Setup
In this section, we will go through the steps required to start the wallet-backend server for local development
and contribution.

### Prerequisites

- Go 1.23.2 or later
- Docker and Docker Compose

### Running the Server

1. Clone the repository:
```bash
git clone https://github.com/stellar/wallet-backend.git
cd wallet-backend
```

2. The wallet-backend spins up different services through its docker-compose file. There are two ways to start the
server:

#### Docker
This is the simplest and quickest way to start wallet-backend server. All the relevant services will be started as
docker services in their respective containers. This config is defined in `docker-compose.yaml` file in the main directory.

1. Copy the example `.env.example`:
```bash
cp .env.example .env
```

2. Add the environment variables:
```bash
CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE=
DATABASE_URL=postgres://postgres@localhost:5432/wallet-backend?sslmode\=disable
DISTRIBUTION_ACCOUNT_PRIVATE_KEY=
DISTRIBUTION_ACCOUNT_PUBLIC_KEY=
LOG_LEVEL=TRACE
NETWORK=testnet
PORT=8001
RPC_URL=http://localhost:8000
SERVER_BASE_URL=http://localhost:8001
STELLAR_ENVIRONMENT=development
TRACKER_DSN=
WALLET_SIGNING_KEY=

# TSS RPC Caller Channel Settings
TSS_RPC_CALLER_CHANNEL_BUFFER_SIZE=1000
TSS_RPC_CALLER_CHANNEL_MAX_WORKERS=100

# Error Handler Jitter Channel Settings
ERROR_HANDLER_JITTER_CHANNEL_BUFFER_SIZE=1000
ERROR_HANDLER_JITTER_CHANNEL_MAX_WORKERS=100
ERROR_HANDLER_JITTER_CHANNEL_MIN_WAIT_BETWEEN_RETRIES=10
ERROR_HANDLER_JITTER_CHANNEL_MAX_RETRIES=3

# Error Handler Non-Jitter Channel Settings
ERROR_HANDLER_NON_JITTER_CHANNEL_BUFFER_SIZE=1000
ERROR_HANDLER_NON_JITTER_CHANNEL_MAX_WORKERS=100
ERROR_HANDLER_NON_JITTER_CHANNEL_WAIT_BETWEEN_RETRIES=10
ERROR_HANDLER_NON_JITTER_CHANNEL_MAX_RETRIES=3

# Webhook Handler Channel Settings
WEBHOOK_CHANNEL_MAX_BUFFER_SIZE=1000
WEBHOOK_CHANNEL_MAX_WORKERS=100
WEBHOOK_CHANNEL_MAX_RETRIES=3
WEBHOOK_CHANNEL_MIN_WAIT_BETWEEN_RETRIES=10
```

3. Start the containers:
```bash
docker-compose up
```

If things are set up correctly, you will see 4 containers started under the wallet-backend docker service: `api`, `db`, 
`ingest` and `stellar-rpc`. The `api` service is used to interact with the different set of APIs exposed by the 
wallet-backend while `ingest` service ingests the relevant payments data from the `stellar-rpc` service 
we started earlier.

#### Local + Docker

This second way of setting up is preferable for more active development where you would 
like to add debug points to the code. 

1. Start the `db` and `stellar-rpc` containers:
```bash
NETWORK=testnet docker-compose up -d db stellar-rpc
```

2. Create a shell script `env.sh` and add the required environment variables:
```bash
#!/bin/bash
export CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE=
export DATABASE_URL=postgres://postgres@localhost:5432/wallet-backend?sslmode\=disable
export DISTRIBUTION_ACCOUNT_PRIVATE_KEY=
export DISTRIBUTION_ACCOUNT_PUBLIC_KEY=
export LOG_LEVEL=TRACE
export NETWORK=testnet
export PORT=8001
export RPC_URL=http://localhost:8000
export SERVER_BASE_URL=http://localhost:8001
export STELLAR_ENVIRONMENT=development
export TRACKER_DSN=
export WALLET_SIGNING_KEY=

# TSS RPC Caller Channel Settings
export TSS_RPC_CALLER_CHANNEL_BUFFER_SIZE=1000
export TSS_RPC_CALLER_CHANNEL_MAX_WORKERS=100

# Error Handler Jitter Channel Settings
export ERROR_HANDLER_JITTER_CHANNEL_BUFFER_SIZE=1000
export ERROR_HANDLER_JITTER_CHANNEL_MAX_WORKERS=100
export ERROR_HANDLER_JITTER_CHANNEL_MIN_WAIT_BETWEEN_RETRIES=10
export ERROR_HANDLER_JITTER_CHANNEL_MAX_RETRIES=3

# Error Handler Non-Jitter Channel Settings
export ERROR_HANDLER_NON_JITTER_CHANNEL_BUFFER_SIZE=1000
export ERROR_HANDLER_NON_JITTER_CHANNEL_MAX_WORKERS=100
export ERROR_HANDLER_NON_JITTER_CHANNEL_WAIT_BETWEEN_RETRIES=10
export ERROR_HANDLER_NON_JITTER_CHANNEL_MAX_RETRIES=3

# Webhook Handler Channel Settings
export WEBHOOK_CHANNEL_MAX_BUFFER_SIZE=1000
export WEBHOOK_CHANNEL_MAX_WORKERS=100
export WEBHOOK_CHANNEL_MAX_RETRIES=3
export WEBHOOK_CHANNEL_MIN_WAIT_BETWEEN_RETRIES=10
```

3. Instead of spinning up `api` and `ingest` as docker services like we did earlier, we will run them locally.

   1. **API**
      1. Run migrations:
        ```bash
        go run main.go migrate up
        ```

      2. Generate channel accounts
        ```bash
        go run main.go channel-account ensure 5
        ```

      3. Start API server
        ```bash
        go run main.go serve
        ```

   2. **Ingest**
      1. In a separate terminal tab, run the ingestion service:
        ```bash
        go run main.go ingest
        ```

This allows us to establish a dev cycle where you can make changes to the code and restart the `api` and `ingest` services
to test them. Based on the IDE you are using, you can add the build configurations for these services, along with 
the environment variables to add breakpoints to your code.