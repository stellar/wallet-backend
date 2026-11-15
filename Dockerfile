# Build stage: compiles the wallet-backend binary.
FROM golang:1.27.0-bookworm AS api-build
ARG VERSION=dev
ARG GIT_COMMIT
# BUILD_MODE=debug adds delve and disables optimizations, for use via docker-compose.dev.yaml.
ARG BUILD_MODE=release

WORKDIR /src/wallet-backend

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN mkdir -p /out/bin && \
    if [ "$BUILD_MODE" = "debug" ]; then \
        go install github.com/go-delve/delve/cmd/dlv@latest && \
        CGO_ENABLED=0 go build -gcflags="all=-N -l" \
            -ldflags "-X main.Version=$VERSION -X main.GitCommit=$GIT_COMMIT" \
            -o /out/bin/wallet-backend . && \
        cp "$(go env GOPATH)/bin/dlv" /out/bin/; \
    else \
        CGO_ENABLED=0 go build -trimpath \
            -ldflags "-s -w -X main.Version=$VERSION -X main.GitCommit=$GIT_COMMIT" \
            -o /out/bin/wallet-backend .; \
    fi

# Runtime stage: minimal Debian image running as a non-root user.
FROM debian:bookworm-slim
ARG VERSION=dev
ARG GIT_COMMIT

LABEL org.opencontainers.image.source="https://github.com/stellar/wallet-backend" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$GIT_COMMIT" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 wallet-backend && \
    useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin wallet-backend

COPY --from=api-build /out/bin/ /usr/local/bin/

WORKDIR /app
USER 10001:10001
EXPOSE 8001

ENTRYPOINT ["wallet-backend"]
