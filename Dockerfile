# Step 1: Build Go API
FROM golang:1.22.4-bullseye as api-build
ARG GIT_COMMIT

WORKDIR /src/wallet-backend
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -o /bin/wallet-backend -ldflags "-X main.GitCommit=$GIT_COMMIT" .


# Step 2: Install Stellar Core and copy over app binary
FROM ubuntu:jammy AS core-build

ARG STELLAR_CORE_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl wget gnupg apt-utils gpg && \
    curl -sSL https://apt.stellar.org/SDF.asc | gpg --dearmor >/etc/apt/trusted.gpg.d/SDF.gpg && \
    echo "deb https://apt.stellar.org jammy stable" >/etc/apt/sources.list.d/SDF.list && \
    echo "deb https://apt.stellar.org jammy testing" >/etc/apt/sources.list.d/SDF-testing.list && \
    echo "deb https://apt.stellar.org jammy unstable" >/etc/apt/sources.list.d/SDF-unstable.list && \
    apt-get update && \
    apt-get install -y stellar-core=${STELLAR_CORE_VERSION} && \
    apt-get clean

COPY --from=api-build /bin/wallet-backend /app/
COPY --from=api-build /src/wallet-backend/internal/ingest/config/stellar-core_pubnet.cfg /app/config/stellar-core_pubnet.cfg
COPY --from=api-build /src/wallet-backend/internal/ingest/config/stellar-core_testnet.cfg /app/config/stellar-core_testnet.cfg

ENV CAPTIVE_CORE_BIN_PATH /usr/bin/stellar-core
ENV CAPTIVE_CORE_CONFIG_DIR /app/config/

EXPOSE 8001
WORKDIR /app
ENTRYPOINT ["./wallet-backend"]
