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
NETWORK=testnet
STELLAR_ENVIRONMENT=development
WALLET_SIGNING_KEY=
```

Note that `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE` is required to be set to a non-empty value. For development purposes,
you can set it to any value. For production, you should set it to a secure passphrase. For `WALLET_SIGNING_KEY`, and 
`DISTRIBUTION_ACCOUNT_PRIVATE_KEY` and `DISTRIBUTION_ACCOUNT_PUBLIC_KEY`, you can generate them using the Stellar CLI or by using tools like lab.stellar.org. Note that the `DISTRIBUTION_ACCOUNT_PUBLIC_KEY` is the public key of the account that will be used to sponsor accounts for channel accounts, and therefore must be a valid Stellar account.

3. Start the containers:

```bash
docker compose up
```

If things are set up correctly, you will see 4 containers started under the wallet-backend docker service: `api`, `db`, 
`ingest` and `stellar-rpc`. The `api` service is used to interact with the different set of APIs exposed by the 
wallet-backend while `ingest` service ingests the relevant payments data from the `stellar-rpc` service 
we started earlier.

#### Local + Docker

This second way of setting up is preferable for more active development where you would 
like to add debug points to the code. 

1. Create and run the `env.sh` script that exports each of the environment variables with the contents shared in the previous section. Note that some variables need to be added to the script to use localhost URLs instead of the URLs usable within the docker network.

```bash
# Add the following to the env.sh file in addition to the other variables:
export RPC_URL=http://localhost:8000
export SERVER_BASE_URL=http://localhost:8001
```

2. Start the `db` and `stellar-rpc` containers:

```bash
docker compose up -d db stellar-rpc
```

3. Instead of spinning up `api` and `ingest` as docker services like we did earlier, we will run them locally.

   1. **API**
      1. Source the `env.sh` file:

        ```bash
        source env.sh
        ```

      2. Run migrations:

        ```bash
        go run main.go migrate up
        ```

      3. Generate channel accounts

        ```bash
        go run main.go channel-account ensure 5
        ```

      4. Start API server

        ```bash
        go run main.go serve
        ```

   2. **Ingest**
      1. In a separate terminal tab, source the `env.sh` file and run the ingestion service:

        ```bash
        source env.sh
        go run main.go ingest
        ```

This allows us to establish a dev cycle where you can make changes to the code and restart the `api` and `ingest` services
to test them. Based on the IDE you are using, you can add the build configurations for these services, along with 
the environment variables to add breakpoints to your code.

### Testing

To run the tests, you can use the following command:

```bash
go test ./...
```

Note that you must set up your environment as defined in the previous section to run the tests, where the database and stellar-rpc are running in docker containers. Alternatively, you could run the database and stellar-rpc locally and run the tests without docker.