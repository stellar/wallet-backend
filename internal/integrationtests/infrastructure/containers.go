package infrastructure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	walletBackendDBContainerName     = "wallet-backend-db"
	walletBackendAPIContainerName    = "wallet-backend-api"
	walletBackendIngestContainerName = "wallet-backend-ingest"
	redisContainerName               = "wallet-backend-redis"
	walletBackendContainerAPIPort    = "8002"
	walletBackendContainerIngestPort = "8003"
	walletBackendContainerTag        = "integration-test"
	walletBackendDockerfile          = "Dockerfile"
	networkPassphrase                = "Standalone Network ; February 2017"
	protocolVersion                  = DefaultProtocolVersion // Default protocol version for Stellar Core upgrades
)

// WalletBackendContainer holds API and Ingest container references
type WalletBackendContainer struct {
	API    *TestContainer
	Ingest *TestContainer
}

// TestContainer wraps a testcontainer with connection string helper
type TestContainer struct {
	testcontainers.Container
	MappedPortStr    string
	ConnectionString string
}

// GetConnectionString returns the HTTP connection string for the container
func (c *TestContainer) GetConnectionString(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("getting container host: %w", err)
	}

	port, err := c.MappedPort(ctx, nat.Port(c.MappedPortStr))
	if err != nil {
		return "", fmt.Errorf("getting mapped port: %w", err)
	}

	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// GetHost returns the host for the container
func (c *TestContainer) GetHost(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("getting container host: %w", err)
	}

	return host, nil
}

// GetPort returns the port for the container
func (c *TestContainer) GetPort(ctx context.Context) (string, error) {
	port, err := c.MappedPort(ctx, nat.Port(c.MappedPortStr))
	if err != nil {
		return "", fmt.Errorf("getting mapped port: %w", err)
	}

	return port.Port(), nil
}

// getGitCommitHash returns the current git commit hash (short form)
// Falls back to "latest" if not in a git repository or if git command fails
func getGitCommitHash() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return "latest"
	}
	return strings.TrimSpace(string(output))
}

// imageExists checks if a Docker image exists locally
func imageExists(imageName string) bool {
	cmd := exec.Command("docker", "images", "-q", imageName)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return len(strings.TrimSpace(string(output))) > 0
}

// shouldRebuildImage determines if the Docker image should be rebuilt
// Returns true if FORCE_REBUILD=true or if the image doesn't exist
func shouldRebuildImage(imageTag string) bool {
	// Check for force rebuild environment variable
	if os.Getenv("FORCE_REBUILD") == "true" {
		log.Info("FORCE_REBUILD=true, rebuilding image")
		return true
	}

	// Check if image exists
	if !imageExists(imageTag) {
		log.Infof("Image %s not found, building it", imageTag)
		return true
	}

	log.Infof("Using existing image: %s", imageTag)
	return false
}

// ensureWalletBackendImage builds or verifies the wallet-backend Docker image exists
// Returns the full image name that can be used for container creation
func ensureWalletBackendImage(ctx context.Context, tag string) (string, error) {
	// Build image tag with git commit hash
	commitHash := getGitCommitHash()
	imageTag := fmt.Sprintf("%s-%s", tag, commitHash)
	fullImageName := fmt.Sprintf("wallet-backend:%s", imageTag)

	// Check if we need to rebuild
	if shouldRebuildImage(fullImageName) {
		log.Ctx(ctx).Infof("Building wallet-backend image: %s", fullImageName)

		// Get the directory of the current source file for relative paths
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			return "", fmt.Errorf("failed to get caller information")
		}
		dir := filepath.Dir(filename)
		contextPath := filepath.Join(dir, "../../../")
		dockerfilePath := filepath.Join(contextPath, walletBackendDockerfile)

		// Build Docker image using docker build command
		cmd := exec.Command("docker", "build",
			"-t", fullImageName,
			"--build-arg", fmt.Sprintf("GIT_COMMIT=%s", commitHash),
			"-f", dockerfilePath,
			contextPath,
		)

		// Capture output for debugging
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Ctx(ctx).Errorf("Docker build failed: %s", string(output))
			return "", fmt.Errorf("building wallet-backend image: %w", err)
		}

		log.Ctx(ctx).Infof("✅ Successfully built wallet-backend image: %s", fullImageName)
	}

	return fullImageName, nil
}

// createRedisContainer starts a Redis container for caching
func createRedisContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name:  redisContainerName,
		Image: "redis:7-alpine",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Networks:     []string{testNetwork.Name},
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating redis container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Redis container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: "6379",
	}, nil
}

// createCoreDBContainer starts a PostgreSQL container for Stellar Core
func createCoreDBContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name:  "core-postgres",
		Image: "postgres:9.6.17-alpine",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "mysecretpassword",
			"POSTGRES_DB":       "stellar",
		},
		Networks:     []string{testNetwork.Name},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating core postgres container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Core DB container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: "5432",
	}, nil
}

// createStellarCoreContainer starts a Stellar Core container in standalone mode
func createStellarCoreContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	// Get the directory of the current source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("failed to get caller information")
	}
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-core",
		Image: "stellar/stellar-core:24",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(dir, "config", "standalone-core.cfg"),
				ContainerFilePath: "/stellar-core.cfg",
				FileMode:          0o644,
			},
			{
				HostFilePath:      filepath.Join(dir, "config", "core-start.sh"),
				ContainerFilePath: "/start",
				FileMode:          0o755,
			},
		},
		Entrypoint: []string{"/bin/bash"},
		Cmd:        []string{"/start", "standalone"},
		Networks:   []string{testNetwork.Name},
		ExposedPorts: []string{
			"11625/tcp", // Peer port
			"11626/tcp", // HTTP port
			"1570/tcp",  // History archive port
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("11626/tcp"),
			wait.ForHTTP("/info").
				WithPort("11626/tcp").
				WithPollInterval(2*time.Second),
			wait.ForLog("Ledger close complete: 8"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating stellar-core container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Stellar Core container")

	testContainer := &TestContainer{
		Container:     container,
		MappedPortStr: "11626",
	}

	// Trigger protocol upgrade
	if err := triggerProtocolUpgrade(ctx, testContainer, protocolVersion); err != nil {
		return nil, fmt.Errorf("triggering protocol upgrade: %w", err)
	}

	return testContainer, nil
}

// createRPCContainer starts a Stellar RPC container for testing
func createRPCContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	// Get the directory of the current source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("failed to get caller information")
	}
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-rpc",
		Image: "stellar/stellar-rpc:24.0.0",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(dir, "config", "captive-core.cfg"),
				ContainerFilePath: "/config/captive-core.cfg",
				FileMode:          0o644,
			},
			{
				HostFilePath:      filepath.Join(dir, "config", "stellar_rpc_config.toml"),
				ContainerFilePath: "/config/stellar_rpc_config.toml",
				FileMode:          0o644,
			},
		},
		Cmd:          []string{"--config-path", "/config/stellar_rpc_config.toml"},
		Networks:     []string{testNetwork.Name},
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8000/tcp"),
			wait.ForExec([]string{"sh", "-c", `curl -s -X POST http://localhost:8000 -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","method":"getHealth","id":1}' | grep -q '"result"'`}).
				WithPollInterval(2*time.Second).
				WithExitCodeMatcher(func(exitCode int) bool { return exitCode == 0 }),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating RPC container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created RPC container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: "8000",
	}, nil
}

// createWalletDBContainer starts a TimescaleDB container for wallet-backend
func createWalletDBContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name:  walletBackendDBContainerName,
		Image: "timescale/timescaledb:latest-pg17",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Env: map[string]string{
			"POSTGRES_HOST_AUTH_METHOD": "trust",
			"POSTGRES_DB":               "wallet-backend",
		},
		Networks:     []string{testNetwork.Name},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp"),
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating wallet-db container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Wallet Backend DB container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: "5432",
	}, nil
}

// createWalletBackendIngestContainer creates a new wallet-backend ingest container using the shared network
func createWalletBackendIngestContainer(ctx context.Context, name string, imageName string,
	testNetwork *testcontainers.DockerNetwork, clientAuthKeyPair *keypair.Full, distributionAccountKeyPair *keypair.Full,
) (*TestContainer, error) {
	// Prepare container request
	containerRequest := testcontainers.ContainerRequest{
		Name:  name,
		Image: imageName,
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Entrypoint: []string{"sh", "-c"},
		Cmd: []string{
			"./wallet-backend migrate up && ./wallet-backend ingest",
		},
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", walletBackendContainerIngestPort)},
		Env: map[string]string{
			"RPC_URL":                          "http://stellar-rpc:8000",
			"DATABASE_URL":                     "postgres://postgres@wallet-backend-db:5432/wallet-backend?sslmode=disable",
			"PORT":                             walletBackendContainerIngestPort,
			"LOG_LEVEL":                        "DEBUG",
			"NETWORK":                          "standalone",
			"ARCHIVE_URL":                      "http://stellar-core:1570",
			"CHECKPOINT_FREQUENCY":             "8",
			"GET_LEDGERS_LIMIT":                "200",
			"SKIP_TX_META":                     "false",
			"LEDGER_BACKEND_TYPE":              "rpc",
			"START_LEDGER":                     "0",
			"END_LEDGER":                       "0",
			"NETWORK_PASSPHRASE":               networkPassphrase,
			"CLIENT_AUTH_PUBLIC_KEYS":          clientAuthKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PUBLIC_KEY":  distributionAccountKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PRIVATE_KEY": distributionAccountKeyPair.Seed(),
			"DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER": "ENV",
			"STELLAR_ENVIRONMENT":                     "integration-test",
			"REDIS_HOST":                              redisContainerName,
			"REDIS_PORT":                              "6379",
		},
		Networks: []string{testNetwork.Name},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		// If we got a container reference, print its logs
		if container != nil {
			logs, logErr := container.Logs(ctx)
			if logErr == nil {
				defer logs.Close()              //nolint:errcheck
				logBytes, _ := io.ReadAll(logs) //nolint:errcheck
				log.Ctx(ctx).Errorf("Container logs:\n%s", string(logBytes))
			}
		}
		return nil, fmt.Errorf("creating wallet-backend ingest container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Wallet Backend Ingest container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: walletBackendContainerIngestPort,
	}, nil
}

// createWalletBackendAPIContainer creates a new wallet-backend container using the shared network
func createWalletBackendAPIContainer(ctx context.Context, name string, imageName string,
	testNetwork *testcontainers.DockerNetwork, clientAuthKeyPair *keypair.Full, distributionAccountKeyPair *keypair.Full,
) (*TestContainer, error) {
	// Prepare container request
	containerRequest := testcontainers.ContainerRequest{
		Name:  name,
		Image: imageName,
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Entrypoint: []string{"sh", "-c"},
		Cmd: []string{
			"./wallet-backend channel-account ensure 15 && ./wallet-backend serve",
		},
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", walletBackendContainerAPIPort)},
		Env: map[string]string{
			"RPC_URL":                          "http://stellar-rpc:8000",
			"DATABASE_URL":                     "postgres://postgres@wallet-backend-db:5432/wallet-backend?sslmode=disable",
			"PORT":                             walletBackendContainerAPIPort,
			"GRAPHQL_COMPLEXITY_LIMIT":         "2000",
			"LOG_LEVEL":                        "DEBUG",
			"NETWORK":                          "standalone",
			"NETWORK_PASSPHRASE":               networkPassphrase,
			"CLIENT_AUTH_PUBLIC_KEYS":          clientAuthKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PUBLIC_KEY":  distributionAccountKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PRIVATE_KEY": distributionAccountKeyPair.Seed(),
			"DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER": "ENV",
			"NUMBER_CHANNEL_ACCOUNTS":                 "15",
			"CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE":   "GB3SKOV2DTOAZVYUXFAM4ELPQDLCF3LTGB4IEODUKQ7NDRZOOESSMNU7",
			"STELLAR_ENVIRONMENT":                     "integration-test",
			"REDIS_HOST":                              redisContainerName,
			"REDIS_PORT":                              "6379",
		},
		Networks:   []string{testNetwork.Name},
		WaitingFor: wait.ForHTTP("/health").WithPort(walletBackendContainerAPIPort + "/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		// If we got a container reference, print its logs
		if container != nil {
			logs, logErr := container.Logs(ctx)
			if logErr == nil {
				defer logs.Close()              //nolint:errcheck
				logBytes, _ := io.ReadAll(logs) //nolint:errcheck
				log.Ctx(ctx).Errorf("Container logs:\n%s", string(logBytes))
			}
		}
		return nil, fmt.Errorf("creating wallet-backend API container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Wallet Backend API container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: walletBackendContainerAPIPort,
	}, nil
}

// triggerProtocolUpgrade triggers a protocol upgrade on Stellar Core
func triggerProtocolUpgrade(ctx context.Context, container *TestContainer, version int) error {
	// Get container's HTTP endpoint
	coreURL, err := container.GetConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("getting core connection string: %w", err)
	}

	// Build upgrade URL with parameters
	upgradeURL := fmt.Sprintf("%s/upgrades?mode=set&upgradetime=1970-01-01T00:00:00Z&protocolversion=%d", coreURL, version)

	// Make HTTP GET request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(upgradeURL)
	if err != nil {
		return fmt.Errorf("triggering protocol upgrade: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("protocol upgrade failed with status code: %d", resp.StatusCode)
	}

	log.Ctx(ctx).Infof("⬆️  Triggered Stellar Core protocol upgrade to version %d", version)
	return nil
}
