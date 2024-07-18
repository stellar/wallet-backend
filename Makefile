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