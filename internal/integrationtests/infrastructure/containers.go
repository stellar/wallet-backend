// Package infrastructure provides container setup for integration tests
package infrastructure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stellar/wallet-backend/internal/entities"
)

const (
	walletBackendContainerName = "wallet-backend"
	walletBackendContainerPort = "8002"
	walletBackendContainerTag  = "integration-test"
	walletBackendDockerfile    = "Dockerfile"
	walletBackendContext       = "../../"
	networkPassphrase          = "Standalone Network ; February 2017"
	protocolVersion            = 23 // Default protocol version for Stellar Core upgrades
)

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

// SharedContainers provides shared container management for integration tests
type SharedContainers struct {
	TestNetwork                   *testcontainers.DockerNetwork
	PostgresContainer             *TestContainer
	StellarCoreContainer          *TestContainer
	RPCContainer                  *TestContainer
	WalletDBContainer             *TestContainer
	WalletBackendContainer        *TestContainer
	clientAuthKeyPair             *keypair.Full
	primarySourceAccountKeyPair   *keypair.Full
	secondarySourceAccountKeyPair *keypair.Full
	distributionAccountKeyPair    *keypair.Full
	masterAccount                 *txnbuild.SimpleAccount
	masterKeyPair                 *keypair.Full
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
	shared.PostgresContainer, err = createCoreDBContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start Stellar Core
	shared.StellarCoreContainer, err = createStellarCoreContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start Stellar RPC
	shared.RPCContainer, err = createRPCContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Initialize master account for funding
	shared.masterKeyPair = keypair.Root(networkPassphrase)
	shared.masterAccount = &txnbuild.SimpleAccount{
		AccountID: shared.masterKeyPair.Address(),
		Sequence:  0,
	}

	// Create keypairs for all test accounts
	shared.clientAuthKeyPair = keypair.MustRandom()
	shared.primarySourceAccountKeyPair = keypair.MustRandom()
	shared.secondarySourceAccountKeyPair = keypair.MustRandom()
	shared.distributionAccountKeyPair = keypair.MustRandom()

	// Create and fund all accounts in a single transaction
	shared.createAndFundAccounts(ctx, t, []*keypair.Full{
		shared.clientAuthKeyPair,
		shared.primarySourceAccountKeyPair,
		shared.secondarySourceAccountKeyPair,
		shared.distributionAccountKeyPair,
	})

	// Start PostgreSQL for wallet-backend
	shared.WalletDBContainer, err = createWalletDBContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Start wallet-backend service
	shared.WalletBackendContainer, err = createWalletBackendAPIContainer(ctx, walletBackendContainerName,
		walletBackendContainerTag, shared.TestNetwork, shared.clientAuthKeyPair, shared.distributionAccountKeyPair)
	require.NoError(t, err)

	return shared
}

func (s *SharedContainers) GetClientAuthKeyPair(ctx context.Context) *keypair.Full {
	return s.clientAuthKeyPair
}

func (s *SharedContainers) GetPrimarySourceAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.primarySourceAccountKeyPair
}

func (s *SharedContainers) GetSecondarySourceAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.secondarySourceAccountKeyPair
}

func (s *SharedContainers) GetDistributionAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.distributionAccountKeyPair
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

// createAndFundAccounts creates and funds multiple accounts in a single transaction using the master account
func (s *SharedContainers) createAndFundAccounts(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
	// Build CreateAccount operations for all accounts
	ops := make([]txnbuild.Operation, len(accounts))
	for i, kp := range accounts {
		ops[i] = &txnbuild.CreateAccount{
			Destination:   kp.Address(),
			Amount:        "10000", // Fund each with 10,000 XLM
			SourceAccount: s.masterKeyPair.Address(),
		}
	}

	// Build transaction with all operations
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	require.NoError(t, err)

	// Sign with master key
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err)

	// Get RPC URL and submit
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err)

	txXDR, err := tx.Base64()
	require.NoError(t, err)

	// Submit transaction to RPC
	client := &http.Client{Timeout: 30 * time.Second}
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit account creation transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "account creation transaction failed with status: %s", sendResult.Status)

	// Wait for transaction to be confirmed
	hash := sendResult.Hash
	var confirmed bool
	for range 20 {
		time.Sleep(500 * time.Millisecond)
		txResult, err := getTransactionFromRPC(client, rpcURL, hash)
		if err == nil && txResult.Status == entities.SuccessStatus {
			confirmed = true
			break
		}
	}
	require.True(t, confirmed, "transaction not confirmed after 10 seconds")

	// Log funded accounts
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("💰 Funded account: %s", kp.Address())
	}
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
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-core",
		Image: "stellar/stellar-core:23",
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
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	containerRequest := testcontainers.ContainerRequest{
		Name:  "stellar-rpc",
		Image: "stellar/stellar-rpc:23.0.4",
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
	log.Ctx(ctx).Infof("🔄 Created Wallet Backend DB container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: "5432",
	}, nil
}

// createWalletBackendAPIContainer creates a new wallet-backend container using the shared network
func createWalletBackendAPIContainer(ctx context.Context, name string, tag string,
	testNetwork *testcontainers.DockerNetwork, clientAuthKeyPair *keypair.Full, distributionAccountKeyPair *keypair.Full,
) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name: name,
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    walletBackendContext,
			Dockerfile: walletBackendDockerfile,
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
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", walletBackendContainerPort)},
		Env: map[string]string{
			"RPC_URL":                          "http://stellar-rpc:8000",
			"DATABASE_URL":                     "postgres://postgres@wallet-db:5432/wallet-backend?sslmode=disable",
			"PORT":                             walletBackendContainerPort,
			"LOG_LEVEL":                        "DEBUG",
			"NETWORK":                          "standalone",
			"NETWORK_PASSPHRASE":               "Standalone Network ; February 2017",
			"CLIENT_AUTH_PUBLIC_KEYS":          clientAuthKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PUBLIC_KEY":  distributionAccountKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PRIVATE_KEY": distributionAccountKeyPair.Seed(),
			"DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER": "ENV",
			"NUMBER_CHANNEL_ACCOUNTS":                 "7",
			"CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE":   "GB3SKOV2DTOAZVYUXFAM4ELPQDLCF3LTGB4IEODUKQ7NDRZOOESSMNU7",
			"STELLAR_ENVIRONMENT":                     "integration-test",
		},
		Networks:   []string{testNetwork.Name},
		WaitingFor: wait.ForHTTP("/health").WithPort(walletBackendContainerPort + "/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Reuse:            true,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating wallet-backend container: %w", err)
	}
	log.Ctx(ctx).Infof("🔄 Created Wallet Backend API container")

	return &TestContainer{
		Container:     container,
		MappedPortStr: walletBackendContainerPort,
	}, nil
}

// submitTransactionToRPC submits a transaction XDR to the RPC endpoint
func submitTransactionToRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSendTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params": map[string]string{
			"transaction": txXDR,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCSendTransactionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}

// getTransactionFromRPC polls RPC for transaction status
func getTransactionFromRPC(client *http.Client, rpcURL, hash string) (*entities.RPCGetTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTransaction",
		"params": map[string]string{
			"hash": hash,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCGetTransactionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}
