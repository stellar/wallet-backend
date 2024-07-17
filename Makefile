# Check if we need to prepend docker command with sudo
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

LABEL ?= $(shell git rev-parse --short HEAD)$(and $(shell git status -s),-dirty-$(shell id -u -n))
# If TAG is not provided set default value
TAG ?= stellar/wallet-backend:$(LABEL)
# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

docker-build:
	$(SUDO) docker build \
		--file Dockerfile \
		--pull \
		--label org.opencontainers.image.created="$(BUILD_DATE)" \
		--tag $(TAG) \
		--build-arg GIT_COMMIT=$(LABEL) \
		--platform linux/amd64 \
		.

docker-push:
	$(SUDO) docker push $(TAG)

go-install:
	go install -ldflags "-X main.GitCommit=$(LABEL)" .