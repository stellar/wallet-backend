// Package infrastructure provides container setup for integration tests
package infrastructure

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/require"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	WalletBackendContainerName = "wallet-backend"
	WalletBackendContainerPort = "8002"
	WalletBackendContainerTag  = "integration-test"
	WalletBackendDockerfile    = "Dockerfile"
	WalletBackendContext       = "../../../"
)

// TestContainer wraps a testcontainer with connection string helper
type TestContainer struct {
	testcontainers.Container
	MappedPortStr string
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

// SharedContainers provides shared container management for integration tests
type SharedContainers struct {
	TestNetwork            *testcontainers.DockerNetwork
	PostgresContainer      *TestContainer
	StellarCoreContainer   *TestContainer
	RPCContainer           *TestContainer
	WalletDBContainer      *TestContainer
	WalletBackendContainer *TestContainer
	ClientAuthKeyPair             *keypair.Full
	PrimarySourceAccountKeyPair   *keypair.Full
	SecondarySourceAccountKeyPair *keypair.Full
	DistributionAccountKeyPair    *keypair.Full
}

// NewSharedContainers creates and starts all containers needed for integration tests
func NewSharedContainers(t *testing.T) *SharedContainers {
	shared := &SharedContainers{}

	ctx := context.Background()

	// Create network
	var err error
	shared.TestNetwork, err = network.New(ctx)
	require.NoError(t, err)

	// Start PostgreSQL for Stellar Core
	shared.PostgresContainer, err = createPostgresContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start Stellar Core
	shared.StellarCoreContainer, err = createStellarCoreContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Create and fund accounts
	shared.ClientAuthKeyPair = shared.createAndFundAccount(ctx, t)
	shared.PrimarySourceAccountKeyPair = shared.createAndFundAccount(ctx, t)
	shared.SecondarySourceAccountKeyPair = shared.createAndFundAccount(ctx, t)
	shared.DistributionAccountKeyPair = shared.createAndFundAccount(ctx, t)

	// Start Stellar RPC
	shared.RPCContainer, err = createRPCContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start PostgreSQL for wallet-backend
	shared.WalletDBContainer, err = createWalletDBContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start wallet-backend service
	shared.WalletBackendContainer, err = createWalletBackendAPIContainer(ctx, WalletBackendContainerName, WalletBackendContainerTag, shared.TestNetwork)
	require.NoError(t, err)

	return shared
}

// Cleanup cleans up shared containers after all tests complete
func (s *SharedContainers) Cleanup(ctx context.Context) {
	if s.WalletBackendContainer != nil {
		_ = s.WalletBackendContainer.Terminate(ctx) //nolint:errcheck
	}
	if s.RPCContainer != nil {
		_ = (*s.RPCContainer).Terminate(ctx) //nolint:errcheck
	}
	if s.StellarCoreContainer != nil {
		_ = (*s.StellarCoreContainer).Terminate(ctx) //nolint:errcheck
	}
	if s.PostgresContainer != nil {
		_ = (*s.PostgresContainer).Terminate(ctx) //nolint:errcheck
	}
	if s.WalletDBContainer != nil {
		_ = (*s.WalletDBContainer).Terminate(ctx) //nolint:errcheck
	}
	if s.TestNetwork != nil {
		_ = s.TestNetwork.Remove(ctx) //nolint:errcheck
	}
}

// createAndFundAccount creates and funds an account using the standalone network friendbot
func (s *SharedContainers) createAndFundAccount(ctx context.Context, t *testing.T) *keypair.Full {
	kp := keypair.MustRandom()
	coreURL, err := s.StellarCoreContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get stellar-core connection string")

	friendbotURL := fmt.Sprintf("%s/friendbot?addr=%s", coreURL, kp.Address())

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(friendbotURL)
	require.NoError(t, err, "failed to fund account %s", kp.Address())
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "friendbot returned non-200 status for account %s", kp.Address())
	log.Ctx(ctx).Infof("💰 Funded account: %s", kp.Address())

	return kp
}

// createPostgresContainer starts a PostgreSQL container for Stellar Core
func createPostgresContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
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
		return nil, fmt.Errorf("creating postgres container: %w", err)
	}

	return &TestContainer{
		Container: container,
		MappedPortStr: "5432",
	}, nil
}

// createStellarCoreContainer starts a Stellar Core container in standalone mode
func createStellarCoreContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	// Get the directory of the current source file
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-core",
		Image: "stellar/stellar-core:22",
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

	return &TestContainer{
		Container: container,
		MappedPortStr: "11626",
	}, nil
}

// createRPCContainer starts a Stellar RPC container for testing
func createRPCContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	// Get the directory of the current source file
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-rpc",
		Image: "stellar/stellar-rpc:latest",
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

	return &TestContainer{
		Container: container,
		MappedPortStr: "8000",
	}, nil
}

// createWalletDBContainer starts a PostgreSQL container for wallet-backend
func createWalletDBContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name:  "wallet-db",
		Image: "postgres:14-alpine",
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Env: map[string]string{
			"POSTGRES_HOST_AUTH_METHOD": "trust",
			"POSTGRES_DB":               "wallet-backend",
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
		return nil, fmt.Errorf("creating wallet-db container: %w", err)
	}

	return &TestContainer{
		Container: container,
		MappedPortStr: "5432",
	}, nil
}

// createWalletBackendAPIContainer creates a new wallet-backend container using the shared network
func createWalletBackendAPIContainer(ctx context.Context, name string, tag string, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name: name,
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    WalletBackendContext,
			Dockerfile: WalletBackendDockerfile,
			KeepImage:  true,
			Tag:        tag,
		},
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Cmd: []string{
			"sh", "-c",
			"./wallet-backend migrate up && ./wallet-backend channel-account ensure 7 && ./wallet-backend serve",
		},
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", WalletBackendContainerPort)},
		Env: map[string]string{
			"RPC_URL":                          		"http://stellar-rpc:8000",
			"DATABASE_URL":                     		"postgres://postgres@wallet-db:5432/wallet-backend?sslmode=disable",
			"PORT":                             		WalletBackendContainerPort,
			"LOG_LEVEL":                        		"DEBUG",
			"NETWORK":                          		"standalone",
			"NETWORK_PASSPHRASE":               		"Standalone Network ; February 2017",
			"CLIENT_AUTH_PUBLIC_KEYS":          		"GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			"DISTRIBUTION_ACCOUNT_PUBLIC_KEY":  		"GC2BRLN55MHAW6QPKJBTXARC35IWK55DX6OGDPRTANYWXVLS3LPY5BWR",
			"DISTRIBUTION_ACCOUNT_PRIVATE_KEY": 		"SAJKJRHPCQFZH5PBLXOM2OBXZJT323F7N23Y7ZFNW4ABUBMM334HB7PY",
			"DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER": 	"ENV",
			"NUMBER_CHANNEL_ACCOUNTS":                 	"7",
			"CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE":   	"GB3SKOV2DTOAZVYUXFAM4ELPQDLCF3LTGB4IEODUKQ7NDRZOOESSMNU7",
			"STELLAR_ENVIRONMENT":                     	"integration-test",
		},
		Networks:   []string{testNetwork.Name},
		WaitingFor: wait.ForHTTP("/health").WithPort(WalletBackendContainerPort + "/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating wallet-backend container: %w", err)
	}

	return &TestContainer{
		Container: container,
		MappedPortStr: WalletBackendContainerPort,
	}, nil
}
