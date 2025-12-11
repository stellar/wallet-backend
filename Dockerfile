# Step 1: Build Go API with debug symbols
FROM golang:1.24.6-bullseye AS api-build
ARG GIT_COMMIT

WORKDIR /src/wallet-backend

# Install delve debugger
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Copy dependency files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code after dependencies are cached
COPY . ./

# Build with debug flags (disable optimizations and inlining)
RUN go build -gcflags="all=-N -l" -o /bin/wallet-backend -ldflags "-X main.GitCommit=$GIT_COMMIT" .

# Step 2: Final image with delve
FROM ubuntu:jammy AS core-build

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl wget gnupg apt-utils gpg bash && \
    curl -sSL https://apt.stellar.org/SDF.asc | gpg --dearmor >/etc/apt/trusted.gpg.d/SDF.gpg && \
    echo "deb https://apt.stellar.org jammy stable" >/etc/apt/sources.list.d/SDF.list && \
    echo "deb https://apt.stellar.org jammy testing" >/etc/apt/sources.list.d/SDF-testing.list && \
    echo "deb https://apt.stellar.org jammy unstable" >/etc/apt/sources.list.d/SDF-unstable.list && \
    rm -rf /var/lib/apt/lists/*

COPY --from=api-build /bin/wallet-backend /app/
COPY --from=api-build /go/bin/dlv /usr/local/bin/
COPY --from=api-build /src/wallet-backend/config /app/config

# Expose API port and debug port
EXPOSE 8001 40000
WORKDIR /app

# Default: run wallet-backend directly (command passed from K8s or docker run)
# For debugging, override command in K8s to use: dlv exec ./wallet-backend --headless --listen=:40000 --api-version=2 --accept-multiclient --continue -- <cmd>
ENTRYPOINT ["./wallet-backend"]
