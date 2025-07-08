# Prepend docker command with sudo if necessary.
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# Extract short commit hash from the current checked-out branch.
LABEL ?= $(shell git rev-parse --short HEAD)$(and $(shell git status -s),-dirty-$(shell id -u -n))

# When building the application for deployment, set the TAG parameter according to your organization's DockerHub repository.
TAG ?= stellar/wallet-backend:$(LABEL)

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

# Version of Stellar Core to be installed. Choose from jammy builds at https://apt.stellar.org/pool/stable/s/stellar-core/
STELLAR_CORE_VERSION ?= 21.0.0-1872.c6f474133.jammy

# ==================================================================================== #
# QUALITY & PREPARATION
# ==================================================================================== #
tidy: ## Tidy modfiles and format source files
	@echo "==> Tidying module files..."
	go mod tidy -v
	@echo "==> Formatting code..."
	go fmt ./...
	$(shell go env GOPATH)/bin/gofumpt -l -w .
	@echo "==> Fixing imports..."
	@command -v $(shell go env GOPATH)/bin/goimports >/dev/null 2>&1 || { go install golang.org/x/tools/cmd/goimports@v0.31.0; }
	@find . -type f -name "*.go" ! -path "*mock*" | xargs $(shell go env GOPATH)/bin/goimports -local "github.com/stellar/wallet-backend" -w

fmt: ## Check if code is formatted with gofmt
	@echo "==> Checking formatting..."
	@test -z $(shell gofmt -l .) || (echo "ERROR: Unformatted files found. Run 'make tidy' or 'gofmt -w .'"; exit 1)
	@echo "Format check passed."

vet: ## Run go vet checks
	@echo "==> Running go vet..."
	go vet ./...

lint: ## Run golangci-lint linter (requires: brew install golangci-lint or equivalent)
	@echo "==> Running golangci-lint..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo >&2 "ERROR: golangci-lint not found. Install it: https://golangci-lint.run/usage/install/"; exit 1; }
	golangci-lint run ./...

generate: ## Run go generate
	@echo "==> Running go generate..."
	go generate ./...

shadow: ## Run shadow analysis to find shadowed variables
	@echo "==> Running shadow analyzer..."
	@if ! command -v $(shell go env GOPATH)/bin/shadow >/dev/null 2>&1; then \
		echo "Installing shadow..."; \
		go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@v0.31.0; \
	fi
	$(shell go env GOPATH)/bin/shadow ./...

exhaustive: ## Check exhaustiveness of switch statements
	@echo "==> Running exhaustive..."
	@command -v exhaustive >/dev/null 2>&1 || { go install github.com/nishanths/exhaustive/cmd/exhaustive@v0.12.0; }
	$(shell go env GOPATH)/bin/exhaustive -default-signifies-exhaustive ./...

deadcode: ## Find unused code
	@echo "==> Checking for deadcode..."
	@if ! command -v $(shell go env GOPATH)/bin/deadcode >/dev/null 2>&1; then \
		echo "Installing deadcode..."; \
		go install golang.org/x/tools/cmd/deadcode@v0.31.0; \
	fi
	@output=$$($(shell go env GOPATH)/bin/deadcode -test ./...); \
	if [ -n "$$output" ]; then \
		echo "🚨 Deadcode found:"; \
		echo "$$output"; \
		exit 1; \
	else \
		echo "✅ No deadcode found"; \
	fi

fix-imports: ## Fix import formatting and organization
	@echo "==> Fixing imports..."
	@command -v $(shell go env GOPATH)/bin/goimports >/dev/null 2>&1 || { go install golang.org/x/tools/cmd/goimports@v0.31.0; }
	@find . -type f -name "*.go" ! -path "*mock*" | xargs $(shell go env GOPATH)/bin/goimports -local "github.com/stellar/wallet-backend" -w
	@echo "✅ Imports fixed."

goimports: ## Check import formatting and organization
	@echo "==> Checking imports..."
	@command -v $(shell go env GOPATH)/bin/goimports >/dev/null 2>&1 || { go install golang.org/x/tools/cmd/goimports@v0.31.0; }
	@non_compliant_files=$$(find . -type f -name "*.go" ! -path "*mock*" | xargs $(shell go env GOPATH)/bin/goimports -local "github.com/stellar/wallet-backend" -l); \
	if [ -n "$$non_compliant_files" ]; then \
		echo "🚨 The following files are not compliant with goimports:"; \
		echo "$$non_compliant_files"; \
		exit 1; \
	else \
		echo "✅ All files are compliant with goimports."; \
	fi

govulncheck: ## Check for known vulnerabilities
	@echo "==> Running vulnerability check..."
	@command -v govulncheck >/dev/null 2>&1 || { go install golang.org/x/vuln/cmd/govulncheck@latest; }
	$(shell go env GOPATH)/bin/govulncheck ./...

check: tidy fmt vet lint generate shadow exhaustive deadcode goimports govulncheck ## Run all checks
	@echo "✅ All checks completed successfully"

# ==================================================================================== #
# GRAPHQL
# ==================================================================================== #
gql-generate: ## Generate GraphQL code using gqlgen
	@echo "==> Generating GraphQL code..."
	@command -v $(shell go env GOPATH)/bin/gqlgen >/dev/null 2>&1 || { go install github.com/99designs/gqlgen@v0.17.76; }
	$(shell go env GOPATH)/bin/gqlgen generate
	@echo "✅ GraphQL code generated successfully"

gql-init: ## Initialize gqlgen configuration (only run once)
	@echo "==> Initializing gqlgen..."
	@command -v $(shell go env GOPATH)/bin/gqlgen >/dev/null 2>&1 || { go install github.com/99designs/gqlgen@v0.17.76; }
	$(shell go env GOPATH)/bin/gqlgen init
	@echo "✅ gqlgen initialized successfully"

gql-validate: ## Validate GraphQL schema
	@echo "==> Validating GraphQL schema..."
	@command -v $(shell go env GOPATH)/bin/gqlgen >/dev/null 2>&1 || { go install github.com/99designs/gqlgen@v0.17.76; }
	$(shell go env GOPATH)/bin/gqlgen validate
	@echo "✅ GraphQL schema is valid"

# ==================================================================================== #
# TESTING
# ==================================================================================== #
unit-test: ## Run unit tests
	@echo "==> Running unit tests..."
	ENABLE_INTEGRATION_TESTS=false go test -v -race ./...

docker-build:
	$(SUDO) docker build \
		--file Dockerfile \
		--pull \
		--label org.opencontainers.image.created="$(BUILD_DATE)" \
		--tag $(TAG) \
		--build-arg GIT_COMMIT=$(LABEL) \
		--build-arg STELLAR_CORE_VERSION=$(STELLAR_CORE_VERSION) \
		--platform linux/amd64 \
		.

docker-push:
	$(SUDO) docker push $(TAG)

go-install:
	go install -ldflags "-X main.GitCommit=$(LABEL)" .