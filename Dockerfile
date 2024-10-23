# Step 1: Build Go API
FROM golang:1.23.2-bullseye as api-build
ARG GIT_COMMIT

WORKDIR /src/wallet-backend
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -o /bin/wallet-backend -ldflags "-X main.GitCommit=$GIT_COMMIT" .

# Use the official stellar/soroban-rpc image as the base
FROM stellar/soroban-rpc

# Install bash or sh
RUN apt-get update && apt-get install -y bash

# Step 2: Install Stellar Core and copy over app binary
FROM ubuntu:jammy AS core-build

COPY --from=api-build /bin/wallet-backend /app/

EXPOSE 8001
WORKDIR /app
ENTRYPOINT ["./wallet-backend"]
