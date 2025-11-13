// Package infrastructure provides container setup for integration tests
package infrastructure

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

const (
	walletBackendDBContainerName     = "wallet-backend-db"
	walletBackendAPIContainerName    = "wallet-backend-api"
	walletBackendIngestContainerName = "wallet-backend-ingest"
	redisContainerName               = "redis"
	walletBackendContainerAPIPort    = "8002"
	walletBackendContainerIngestPort = "8003"
	walletBackendContainerTag        = "integration-test"
	walletBackendDockerfile          = "Dockerfile"
	networkPassphrase                = "Standalone Network ; February 2017"
	protocolVersion                  = DefaultProtocolVersion // Default protocol version for Stellar Core upgrades
)

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

// SharedContainers provides shared container management for integration tests
type SharedContainers struct {
	// Docker infrastructure
	TestNetwork            *testcontainers.DockerNetwork
	PostgresContainer      *TestContainer
	StellarCoreContainer   *TestContainer
	RPCContainer           *TestContainer
	WalletDBContainer      *TestContainer
	RedisContainer         *TestContainer
	WalletBackendContainer *WalletBackendContainer

	// HTTP client for RPC calls (reusable, safe for concurrent use)
	httpClient *http.Client

	// Test accounts
	clientAuthKeyPair             *keypair.Full
	primarySourceAccountKeyPair   *keypair.Full
	secondarySourceAccountKeyPair *keypair.Full
	distributionAccountKeyPair    *keypair.Full
	sponsoredNewAccountKeyPair    *keypair.Full
	balanceTestAccount1KeyPair    *keypair.Full
	balanceTestAccount2KeyPair    *keypair.Full
	masterKeyPair                 *keypair.Full
	masterAccount                 *txnbuild.SimpleAccount

	// Deployed contracts
	usdcContractAddress string
	eurcContractAddress string
	// sep41ContractAddress is the contract address (C...) of a deployed custom SEP-41 token.
	// This represents a non-SAC token contract that implements the SEP-41 token interface.
	// Used for testing contract balances for both G-addresses and C-addresses.
	sep41ContractAddress string
	// holderContractAddress is the contract address (C...) of a simple deployed contract
	// that can hold token balances. This contract doesn't implement token functionality itself,
	// but can receive and hold balances from both SAC and SEP-41 tokens.
	// Used for testing C-address token balances.
	holderContractAddress string
}

// NewSharedContainers creates and starts all containers needed for integration tests
// initializeContainerInfrastructure sets up Docker network and core infrastructure containers.
//
//nolint:unparam // t kept for API consistency with other setup methods
func (s *SharedContainers) initializeContainerInfrastructure(ctx context.Context, t *testing.T) error {
	var err error

	// Create network
	s.TestNetwork, err = network.New(ctx)
	if err != nil {
		return fmt.Errorf("creating test network: %w", err)
	}

	// Start Redis container
	s.RedisContainer, err = createRedisContainer(ctx, s.TestNetwork)
	if err != nil {
		return fmt.Errorf("creating Redis container: %w", err)
	}

	// Start PostgreSQL for Stellar Core
	s.PostgresContainer, err = createCoreDBContainer(ctx, s.TestNetwork)
	if err != nil {
		return fmt.Errorf("creating core DB container: %w", err)
	}

	// Start Stellar Core
	s.StellarCoreContainer, err = createStellarCoreContainer(ctx, s.TestNetwork)
	if err != nil {
		return fmt.Errorf("creating Stellar Core container: %w", err)
	}

	// Start Stellar RPC
	s.RPCContainer, err = createRPCContainer(ctx, s.TestNetwork)
	if err != nil {
		return fmt.Errorf("creating RPC container: %w", err)
	}

	return nil
}

// setupTestAccounts initializes keypairs, creates accounts, and establishes trustlines.
//
//nolint:unparam // error return kept for API consistency with other setup methods
func (s *SharedContainers) setupTestAccounts(ctx context.Context, t *testing.T) error {
	// Initialize master account for funding
	s.masterKeyPair = keypair.Root(networkPassphrase)
	s.masterAccount = &txnbuild.SimpleAccount{
		AccountID: s.masterKeyPair.Address(),
		Sequence:  0,
	}

	// Create keypairs for all test accounts
	s.clientAuthKeyPair = keypair.MustRandom()
	s.primarySourceAccountKeyPair = keypair.MustRandom()
	s.secondarySourceAccountKeyPair = keypair.MustRandom()
	s.distributionAccountKeyPair = keypair.MustRandom()
	s.balanceTestAccount1KeyPair = keypair.MustRandom()
	s.balanceTestAccount2KeyPair = keypair.MustRandom()

	// We are not funding this account, as it will be funded by the primary account through a sponsored account creation operation
	s.sponsoredNewAccountKeyPair = keypair.MustRandom()

	// Create and fund all accounts in a single transaction
	s.createAndFundAccounts(ctx, t, []*keypair.Full{
		s.clientAuthKeyPair,
		s.primarySourceAccountKeyPair,
		s.secondarySourceAccountKeyPair,
		s.distributionAccountKeyPair,
		s.balanceTestAccount1KeyPair,
		s.balanceTestAccount2KeyPair,
	})

	// Create trustlines - these will be used for the account balances test
	s.createUSDCTrustlines(ctx, t, []*keypair.Full{
		s.balanceTestAccount1KeyPair,
		s.balanceTestAccount2KeyPair,
	})

	// Create EURC trustlines for balance test account 1 only
	s.createEURCTrustlines(ctx, t)

	return nil
}

// setupContracts deploys and initializes all Soroban contracts and tokens.
func (s *SharedContainers) setupContracts(ctx context.Context, t *testing.T, dir string) error {
	// Deploy native asset SAC so it can be used in smart contract operations
	s.deployNativeAssetSAC(ctx, t)
	log.Ctx(ctx).Info("✅ Native asset SAC deployed successfully")

	// ============================================================================
	// SEP-41 Token Contract Setup
	// ============================================================================
	//
	// This section deploys and initializes a SEP-41 compliant token contract for
	// integration testing. We use the standard token contract from stellar/soroban-examples
	// which provides a complete implementation of the SEP-41 interface.
	//
	// SEP-41 Interface Overview:
	//   - initialize: __constructor(admin, decimal, name, symbol) - Must be called after deployment
	//   - mint: mint(to, amount) - Mints tokens to an address (admin only)
	//   - transfer: transfer(from, to, amount) - Transfers tokens between addresses
	//   - burn: burn(from, amount) - Burns tokens from an address
	//   - Query: balance(address), decimals(), name(), symbol(), allowance(from, spender)
	//
	// Token Configuration:
	//   - Name: "SEP41 Token"
	//   - Symbol: "SEP41"
	//   - Decimals: 7
	//   - Admin: Master test account
	//
	// Why a Custom Token Contract?
	// This creates a custom token contract (not a Stellar Asset Contract) that implements
	// the SEP-41 token interface. When this contract mints or transfers tokens to G-addresses,
	// it creates contract data entries with key [Balance, G-address] in the ledger.
	// This is different from classic trustlines and allows testing of pure contract tokens.
	//
	// References:
	//   - SEP-41 Standard: https://stellar.org/protocol/sep-41
	//   - Token Contract: https://github.com/stellar/soroban-examples/tree/main/token

	// Step 1: Read the token contract WASM file
	sep41WasmBytes, err := os.ReadFile(filepath.Join(dir, "testdata", "soroban_token_contract.wasm"))
	if err != nil {
		return fmt.Errorf("reading SEP-41 token WASM file: %w", err)
	}

	// Step 2: Upload the WASM bytecode to the ledger
	sep41WasmHash := s.uploadContractWasm(ctx, t, sep41WasmBytes)
	log.Ctx(ctx).Infof("✅ Uploaded SEP41 contract WASM with hash: %x", sep41WasmHash)

	// Step 3: Build constructor arguments for the token contract
	adminAccountID := xdr.MustAddress(s.masterKeyPair.Address())
	adminSCAddress := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &adminAccountID,
	}
	adminArg := xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &adminSCAddress,
	}

	decimalXDR := xdr.Uint32(7)
	decimalArg := xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  &decimalXDR,
	}

	nameStr := xdr.ScString("SEP41 Token")
	nameArg := xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &nameStr,
	}

	symbolStr := xdr.ScString("SEP41")
	symbolArg := xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &symbolStr,
	}

	sep41ConstructorArgs := []xdr.ScVal{adminArg, decimalArg, nameArg, symbolArg}

	// Step 4: Deploy and initialize the contract atomically
	s.sep41ContractAddress = s.deployContractWithConstructor(ctx, t, sep41WasmHash, sep41ConstructorArgs)
	log.Ctx(ctx).Infof("✅ Deployed and initialized SEP-41 token contract at: %s (admin=%s, decimals=7, name=SEP41 Token, symbol=SEP41)",
		s.sep41ContractAddress, s.masterKeyPair.Address())

	// Deploy Holder Contract
	holderWasmBytes, err := os.ReadFile(filepath.Join(dir, "testdata", "soroban_increment_contract.wasm"))
	if err != nil {
		return fmt.Errorf("reading holder contract WASM file: %w", err)
	}
	holderWasmHash := s.uploadContractWasm(ctx, t, holderWasmBytes)
	log.Ctx(ctx).Infof("✅ Uploaded holder contract WASM with hash: %x", holderWasmHash)

	s.holderContractAddress = s.deployContractWithConstructor(ctx, t, holderWasmHash, []xdr.ScVal{})
	log.Ctx(ctx).Infof("✅ Deployed holder contract at: %s", s.holderContractAddress)

	// Create SEP-41 Balance for G-Address
	s.mintSEP41Tokens(ctx, t, s.sep41ContractAddress, s.balanceTestAccount1KeyPair.Address(), TestSEP41MintStroops)
	log.Ctx(ctx).Infof("✅ Minted SEP-41 tokens to %s", s.balanceTestAccount1KeyPair.Address())

	// Create SAC Balance for C-Address
	s.invokeContractTransfer(ctx, t, s.usdcContractAddress, s.masterKeyPair.Address(), s.holderContractAddress, TestUSDCTransferStroops)
	log.Ctx(ctx).Infof("✅ Transferred USDC SAC tokens to contract %s", s.holderContractAddress)

	// Create SEP-41 Balance for C-Address
	s.mintSEP41Tokens(ctx, t, s.sep41ContractAddress, s.holderContractAddress, TestSEP41MintStroops)
	log.Ctx(ctx).Infof("✅ Minted SEP-41 tokens to contract %s", s.holderContractAddress)

	// Wait for the contracts to be present in latest checkpoint
	log.Ctx(ctx).Info("🕒 Waiting for contracts to be present in latest checkpoint...")
	time.Sleep(CheckpointWaitDuration)

	return nil
}

// setupWalletBackend initializes wallet-backend database and service containers.
//
//nolint:unparam // t kept for API consistency with other setup methods
func (s *SharedContainers) setupWalletBackend(ctx context.Context, t *testing.T) error {
	var err error

	// Start PostgreSQL for wallet-backend
	s.WalletDBContainer, err = createWalletDBContainer(ctx, s.TestNetwork)
	if err != nil {
		return fmt.Errorf("creating wallet DB container: %w", err)
	}

	// Build or verify wallet-backend Docker image
	walletBackendImage, err := ensureWalletBackendImage(ctx, walletBackendContainerTag)
	if err != nil {
		return fmt.Errorf("ensuring wallet backend image: %w", err)
	}

	// Start wallet-backend ingest
	s.WalletBackendContainer = &WalletBackendContainer{}
	s.WalletBackendContainer.Ingest, err = createWalletBackendIngestContainer(ctx, walletBackendIngestContainerName,
		walletBackendImage, s.TestNetwork, s.clientAuthKeyPair, s.distributionAccountKeyPair)
	if err != nil {
		return fmt.Errorf("creating wallet backend ingest container: %w", err)
	}

	// Start wallet-backend service
	s.WalletBackendContainer.API, err = createWalletBackendAPIContainer(ctx, walletBackendAPIContainerName,
		walletBackendImage, s.TestNetwork, s.clientAuthKeyPair, s.distributionAccountKeyPair)
	if err != nil {
		return fmt.Errorf("creating wallet backend API container: %w", err)
	}

	return nil
}

func NewSharedContainers(t *testing.T) *SharedContainers {
	// Get the directory of the current source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		require.Fail(t, "failed to get caller information")
	}
	dir := filepath.Dir(filename)

	shared := &SharedContainers{
		// Initialize reusable HTTP client for RPC calls
		httpClient: &http.Client{Timeout: DefaultHTTPTimeout},
	}
	ctx := context.Background()

	// Initialize container infrastructure
	err := shared.initializeContainerInfrastructure(ctx, t)
	require.NoError(t, err, "failed to initialize container infrastructure")

	// Setup test accounts
	err = shared.setupTestAccounts(ctx, t)
	require.NoError(t, err, "failed to setup test accounts")

	// Setup contracts
	err = shared.setupContracts(ctx, t, dir)
	require.NoError(t, err, "failed to setup contracts")

	// Setup wallet backend
	err = shared.setupWalletBackend(ctx, t)
	require.NoError(t, err, "failed to setup wallet backend")

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

func (s *SharedContainers) GetSponsoredNewAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.sponsoredNewAccountKeyPair
}

func (s *SharedContainers) GetBalanceTestAccount1KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount1KeyPair
}

func (s *SharedContainers) GetBalanceTestAccount2KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount2KeyPair
}

func (s *SharedContainers) GetMasterKeyPair(ctx context.Context) *keypair.Full {
	return s.masterKeyPair
}

// Cleanup cleans up shared containers after all tests complete
func (s *SharedContainers) Cleanup(ctx context.Context) {
	if s.WalletBackendContainer.API != nil {
		_ = s.WalletBackendContainer.API.Terminate(ctx) //nolint:errcheck
	}
	if s.WalletBackendContainer.Ingest != nil {
		_ = s.WalletBackendContainer.Ingest.Terminate(ctx) //nolint:errcheck
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
	if s.RedisContainer != nil {
		_ = (*s.RedisContainer).Terminate(ctx) //nolint:errcheck
	}
	if s.TestNetwork != nil {
		_ = s.TestNetwork.Remove(ctx) //nolint:errcheck
	}
}

// deployNativeAssetSAC deploys the Stellar Asset Contract for the native asset (XLM)
func (s *SharedContainers) deployNativeAssetSAC(ctx context.Context, t *testing.T) {
	// Create the InvokeHostFunction operation to deploy the native asset contract
	nativeAsset := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
					FromAsset: &nativeAsset,
				},
				Executable: xdr.ContractExecutable{
					Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the deployment operation
	_, err := executeSorobanOperation(ctx, t, s, deployOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy native asset SAC")
}

// deployCreditAssetSAC deploys the Stellar Asset Contract for a credit asset (non-native)
func (s *SharedContainers) deployCreditAssetSAC(ctx context.Context, t *testing.T, assetCode, issuer string) {
	// Create the credit asset XDR based on asset code length
	var creditAsset xdr.Asset

	if len(assetCode) <= 4 {
		// AlphaNum4 for asset codes 1-4 characters (e.g., "USDC")
		var assetCode4 xdr.AssetCode4
		copy(assetCode4[:], assetCode)
		creditAsset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
			AlphaNum4: &xdr.AlphaNum4{
				AssetCode: assetCode4,
				Issuer:    xdr.MustAddress(issuer),
			},
		}
	} else {
		// AlphaNum12 for asset codes 5-12 characters
		var assetCode12 xdr.AssetCode12
		copy(assetCode12[:], assetCode)
		creditAsset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum12,
			AlphaNum12: &xdr.AlphaNum12{
				AssetCode: assetCode12,
				Issuer:    xdr.MustAddress(issuer),
			},
		}
	}

	// Create the InvokeHostFunction operation to deploy the credit asset contract
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
					FromAsset: &creditAsset,
				},
				Executable: xdr.ContractExecutable{
					Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the deployment operation
	_, err := executeSorobanOperation(ctx, t, s, deployOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy credit asset SAC for %s", assetCode)
}

// createAndFundAccounts creates and funds multiple accounts in a single transaction using the master account
func (s *SharedContainers) createAndFundAccounts(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
	// Build CreateAccount operations for all accounts
	ops := make([]txnbuild.Operation, len(accounts))
	for i, kp := range accounts {
		ops[i] = &txnbuild.CreateAccount{
			Destination:   kp.Address(),
			Amount:        DefaultFundingAmount, // Fund each with 10,000 XLM
			SourceAccount: s.masterKeyPair.Address(),
		}
	}

	// Execute the account creation operations
	_, err := executeClassicOperation(ctx, t, s, ops, []*keypair.Full{s.masterKeyPair})
	require.NoError(t, err, "failed to create and fund accounts")

	// Log funded accounts
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("💰 Funded account: %s", kp.Address())
	}
}

// createTrustlines creates test trustlines for balance test accounts
func (s *SharedContainers) createUSDCTrustlines(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
	// Create test asset issued by master account
	testAsset := txnbuild.CreditAsset{
		Code:   "USDC",
		Issuer: s.masterKeyPair.Address(),
	}
	// Convert to XDR Asset to get contract ID
	xdrAsset, err := testAsset.ToXDR()
	require.NoError(t, err)
	contractID, err := xdrAsset.ContractID(networkPassphrase)
	require.NoError(t, err)
	s.usdcContractAddress = strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Deploy the SAC for the USDC credit asset
	s.deployCreditAssetSAC(ctx, t, "USDC", s.masterKeyPair.Address())
	log.Ctx(ctx).Infof("✅ Deployed USDC SAC at address: %s", s.usdcContractAddress)

	// Build ChangeTrust operations for all accounts
	ops := make([]txnbuild.Operation, 0)
	for _, kp := range accounts {
		ops = append(ops, &txnbuild.ChangeTrust{
			Line: txnbuild.ChangeTrustAssetWrapper{
				Asset: testAsset,
			},
			Limit:         DefaultTrustlineLimit, // 1 million units
			SourceAccount: kp.Address(),
		}, &txnbuild.Payment{
			Destination:   kp.Address(),
			Amount:        TestUSDCPaymentAmount,
			Asset:         testAsset,
			SourceAccount: s.masterKeyPair.Address(),
		})
	}

	// Build signers list: master key + all account keys
	signers := make([]*keypair.Full, 0, len(accounts)+1)
	signers = append(signers, s.masterKeyPair)
	signers = append(signers, accounts...)

	// Execute the trustline creation operations
	_, err = executeClassicOperation(ctx, t, s, ops, signers)
	require.NoError(t, err, "failed to create USDC trustlines")

	// Log created trustlines
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("🔗 Created balance test trustline for account %s: %s:%s", kp.Address(), testAsset.Code, testAsset.Issuer)
	}
}

// createEURCTrustlines creates EURC trustline for balance test account 1 only
func (s *SharedContainers) createEURCTrustlines(ctx context.Context, t *testing.T) {
	// Create EURC asset issued by master account
	eurcAsset := txnbuild.CreditAsset{
		Code:   "EURC",
		Issuer: s.masterKeyPair.Address(),
	}
	// Convert to XDR Asset to get contract ID
	xdrAsset, err := eurcAsset.ToXDR()
	require.NoError(t, err)
	contractID, err := xdrAsset.ContractID(networkPassphrase)
	require.NoError(t, err)
	s.eurcContractAddress = strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Deploy the SAC for the EURC credit asset
	s.deployCreditAssetSAC(ctx, t, "EURC", s.masterKeyPair.Address())
	log.Ctx(ctx).Infof("✅ Deployed EURC SAC at address: %s", s.eurcContractAddress)

	// Build ChangeTrust operation for balance test account 1 only
	ops := []txnbuild.Operation{
		&txnbuild.ChangeTrust{
			Line: txnbuild.ChangeTrustAssetWrapper{
				Asset: eurcAsset,
			},
			Limit:         DefaultTrustlineLimit, // 1 million units
			SourceAccount: s.balanceTestAccount1KeyPair.Address(),
		},
		&txnbuild.Payment{
			Destination:   s.balanceTestAccount1KeyPair.Address(),
			Amount:        TestUSDCPaymentAmount,
			Asset:         eurcAsset,
			SourceAccount: s.masterKeyPair.Address(),
		},
	}

	// Execute the trustline creation operations
	_, err = executeClassicOperation(ctx, t, s, ops, []*keypair.Full{s.masterKeyPair, s.balanceTestAccount1KeyPair})
	require.NoError(t, err, "failed to create EURC trustline")

	log.Ctx(ctx).Infof("🔗 Created EURC trustline for balance test account 1 %s: %s:%s", s.balanceTestAccount1KeyPair.Address(), eurcAsset.Code, eurcAsset.Issuer)
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

// createWalletDBContainer starts a PostgreSQL container for wallet-backend
func createWalletDBContainer(ctx context.Context, testNetwork *testcontainers.DockerNetwork) (*TestContainer, error) {
	containerRequest := testcontainers.ContainerRequest{
		Name:  walletBackendDBContainerName,
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
			"NETWORK_PASSPHRASE":               networkPassphrase,
			"CLIENT_AUTH_PUBLIC_KEYS":          clientAuthKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PUBLIC_KEY":  distributionAccountKeyPair.Address(),
			"DISTRIBUTION_ACCOUNT_PRIVATE_KEY": distributionAccountKeyPair.Seed(),
			"DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER": "ENV",
			"STELLAR_ENVIRONMENT":                     "integration-test",
			"REDIS_HOST":                              "redis",
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
			"REDIS_HOST":                              "redis",
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

// uploadContractWasm uploads a Soroban contract WASM bytecode to the ledger.
// This is the first step in deploying a Soroban smart contract. The WASM code
// is stored in the ledger and can be referenced by its hash for deployment.
//
// Parameters:
//   - ctx: Context for the operation
//   - t: Testing instance for assertions
//   - wasmBytes: The compiled WASM bytecode to upload
//
// Returns:
//   - xdr.Hash: The hash of the uploaded WASM code, used for deployment
//
// The function:
//  1. Builds an InvokeHostFunction operation with HostFunctionTypeUploadContractWasm
//  2. Simulates the transaction to get resource footprint and fees
//  3. Signs the transaction with the master key
//  4. Submits to RPC and waits for confirmation
func (s *SharedContainers) uploadContractWasm(ctx context.Context, t *testing.T, wasmBytes []byte) xdr.Hash {
	// Create the InvokeHostFunction operation to upload WASM
	uploadOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &wasmBytes,
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the WASM upload operation
	_, err := executeSorobanOperation(ctx, t, s, uploadOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to upload WASM")

	// Compute and return WASM hash from the uploaded bytecode
	wasmHash := xdr.Hash(sha256.Sum256(wasmBytes))
	return wasmHash
}

// deployContract deploys a Soroban contract from previously uploaded WASM bytecode.
// This creates a new contract instance on the ledger with a unique contract address (C...).
// The contract can then be invoked to execute its functions.
//
// Parameters:
//   - ctx: Context for the operation
//   - t: Testing instance for assertions
//   - wasmHash: The hash returned from uploadContractWasm
//
// Returns:
//   - string: The contract address in Stellar strkey format (C...)
//
// The function:
//  1. Generates a random salt for unique contract address
//  2. Builds ContractIdPreimageFromAddress using master account and salt
//  3. Creates InvokeHostFunction with HostFunctionTypeCreateContract
//  4. Simulates, signs, submits, and waits for confirmation
//  5. Computes and returns the contract address
//
// deployContractWithConstructor deploys a Soroban contract with optional constructor arguments.
// This function uses CreateContractArgsV2 which supports passing arguments to the contract's
// __constructor function during deployment (if the contract has one).
//
// For contracts WITHOUT a constructor:
//   - Pass an empty slice: []xdr.ScVal{}
//   - The contract will deploy normally without any initialization
//
// For contracts WITH a constructor (like soroban-examples token):
//   - Pass constructor arguments as []xdr.ScVal
//   - The constructor runs atomically during deployment
//   - Example: token contract needs [admin, decimal, name, symbol]
//
// Parameters:
//   - ctx: Context for logging
//   - t: Testing context for assertions
//   - wasmHash: Hash of the uploaded WASM bytecode
//   - constructorArgs: Constructor arguments (empty slice if no constructor)
//
// Returns:
//   - Contract address (C...) of the deployed contract
//
// Process:
//  1. Generate random salt for unique contract address
//  2. Build CreateContractArgsV2 with constructor arguments
//  3. Simulate to get resource footprint
//  4. Sign and submit deployment transaction
//  5. Wait for confirmation
//  6. Calculate and return contract address
func (s *SharedContainers) deployContractWithConstructor(ctx context.Context, t *testing.T, wasmHash xdr.Hash, constructorArgs []xdr.ScVal) string {
	// Step 1: Generate random salt for unique contract address
	// The salt ensures each deployment creates a different contract address
	salt := make([]byte, 32)
	_, err := rand.Read(salt)
	require.NoError(t, err, "failed to generate salt")
	var saltHash xdr.Uint256
	copy(saltHash[:], salt)

	// Step 2: Create deployer address from master account
	deployerAccountID := xdr.MustAddress(s.masterKeyPair.Address())

	// Step 3: Create ContractIdPreimage for the deployment
	// This will be used both for the deployment operation and to compute the contract address
	preimage := xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &deployerAccountID,
			},
			Salt: saltHash,
		},
	}

	// Step 4: Create the InvokeHostFunction operation to deploy the contract
	// We use CreateContractV2 which supports constructor arguments
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2, // V2 for constructor support
			CreateContractV2: &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &wasmHash,
				},
				ConstructorArgs: constructorArgs, // Pass constructor args (empty slice if none)
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Step 5: Execute the contract deployment operation
	// requireAuth=true because CreateContractV2 with constructor requires deployer authorization
	_, err = executeSorobanOperation(ctx, t, s, deployOp, true, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy contract with constructor")

	// Step 6: Calculate and return the contract address from the preimage
	// The contract address is deterministically computed from the deployer address and salt
	contractAddress, err := s.calculateContractID(networkPassphrase, preimage.MustFromAddress())
	require.NoError(t, err, "failed to calculate contract ID")
	return contractAddress
}

// calculateContractID calculates the contract ID for a wallet creation transaction based on the network passphrase, deployer account and salt.
//
// More info: https://developers.stellar.org/docs/build/smart-contracts/example-contracts/deployer#how-it-works
func (s *SharedContainers) calculateContractID(networkPassphrase string, deployerAddress xdr.ContractIdPreimageFromAddress) (string, error) {
	networkHash := xdr.Hash(sha256.Sum256([]byte(networkPassphrase)))

	hashIDPreimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: networkHash,
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type:        xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &deployerAddress,
			},
		},
	}

	preimageXDR, err := hashIDPreimage.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshaling preimage: %w", err)
	}

	contractIDHash := sha256.Sum256(preimageXDR)
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDHash[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract ID: %w", err)
	}

	return contractID, nil
}

// mintSEP41Tokens invokes the mint function on a SEP-41 token contract to create new tokens.
// This is typically called by the contract admin to issue tokens to an address.
//
// The mint function signature: mint(to: Address, amount: i128)
//
// Note: The soroban-examples token contract requires the transaction to be signed by the
// admin account that was set during initialization. Only the admin can mint new tokens.
//
// For G-addresses (accounts), this creates a contract data entry with key [Balance, G-address]
// that represents the account's balance in the contract's token. This is different from classic
// trustlines - it's pure contract state storage.
//
// Parameters:
//   - ctx: Context for logging
//   - t: Testing instance for assertions
//   - tokenContractAddress: The contract address of the SEP-41 token (C...)
//   - toAddress: Recipient address (can be G... for accounts or C... for contracts)
//   - amount: Amount to mint in stroops/base units (e.g., 5000000000 = 500 tokens with 7 decimals)
func (s *SharedContainers) mintSEP41Tokens(ctx context.Context, t *testing.T, tokenContractAddress, toAddress string, amount int64) {
	// Decode token contract address to get contract ID
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Build recipient ScAddress using helper
	toSCAddress, err := parseAddressToScAddress(toAddress)
	require.NoError(t, err, "failed to parse recipient address")

	// Build the mint invocation
	// Function signature: mint(to: Address, amount: i128)
	mintSym := xdr.ScSymbol("mint")
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &tokenID,
				},
				FunctionName: mintSym,
				Args: xdr.ScVec{
					// Arg 1: recipient address (to: Address)
					{Type: xdr.ScValTypeScvAddress, Address: &toSCAddress},
					// Arg 2: amount to mint (amount: i128)
					// i128 is a 128-bit signed integer used for token amounts.
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: 0,                  // High 64 bits (for very large amounts)
							Lo: xdr.Uint64(amount), // Low 64 bits (sufficient for test amounts)
						},
					},
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(), // Must be admin for mint to succeed
	}

	// Execute using helper with extended retries for mint operations
	_, err = executeSorobanOperation(ctx, t, s, invokeOp, true, ExtendedConfirmationRetries)
	require.NoError(t, err, "failed to mint SEP-41 tokens")
}

// invokeContractTransfer invokes the transfer function on a SEP-41 token contract.
// This transfers tokens from one address to another. Both sender and recipient can be
// either account addresses (G...) or contract addresses (C...).
//
// When transferring to a contract address (C...), this creates a contract data entry
// in the ledger with key [Balance, C-address] that stores the contract's token balance.
//
// Parameters:
//   - ctx: Context for the operation
//   - t: Testing instance for assertions
//   - tokenContractAddress: The contract address of the token (SEP-41/SAC)
//   - fromAddress: Sender address (G... or C...)
//   - toAddress: Recipient address (G... or C...)
//   - amount: Amount to transfer (in stroops/atomic units)
func (s *SharedContainers) invokeContractTransfer(ctx context.Context, t *testing.T, tokenContractAddress, fromAddress, toAddress string, amount int64) {
	// Decode token contract address to get contract ID
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Build addresses using helper
	fromSCAddress, err := parseAddressToScAddress(fromAddress)
	require.NoError(t, err, "failed to parse sender address")

	toSCAddress, err := parseAddressToScAddress(toAddress)
	require.NoError(t, err, "failed to parse recipient address")

	// Build transfer invocation
	transferSym := xdr.ScSymbol("transfer")
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &tokenID,
				},
				FunctionName: transferSym,
				Args: xdr.ScVec{
					{Type: xdr.ScValTypeScvAddress, Address: &fromSCAddress},
					{Type: xdr.ScValTypeScvAddress, Address: &toSCAddress},
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: 0,
							Lo: xdr.Uint64(amount),
						},
					},
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute using helper
	_, err = executeSorobanOperation(ctx, t, s, invokeOp, true, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to transfer tokens")
}

// TestEnvironment holds all initialized services and clients for integration tests
type TestEnvironment struct {
	WBClient              *wbclient.Client
	RPCService            services.RPCService
	PrimaryAccountKP      *keypair.Full
	SecondaryAccountKP    *keypair.Full
	SponsoredNewAccountKP *keypair.Full
	BalanceTestAccount1KP *keypair.Full
	BalanceTestAccount2KP *keypair.Full
	USDCContractAddress   string
	EURCContractAddress   string
	// SEP41ContractAddress is the address of the deployed custom SEP-41 token contract.
	// Used for asserting token IDs in balance queries for non-SAC tokens.
	SEP41ContractAddress string
	// HolderContractAddress is the address of the test contract that holds token balances.
	// Used for querying C-address balances in integration tests.
	HolderContractAddress    string
	ClaimBalanceID           string
	ClawbackBalanceID        string
	LiquidityPoolID          string
	NetworkPassphrase        string
	UseCases                 []*UseCase
	ClaimAndClawbackUseCases []*UseCase
}

// createWalletBackendClient initializes the wallet-backend client
func createWalletBackendClient(ctx context.Context, containers *SharedContainers) (*wbclient.Client, error) {
	wbURL, err := containers.WalletBackendContainer.API.GetConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get wallet-backend connection string: %w", err)
	}

	clientAuthKP := containers.GetClientAuthKeyPair(ctx)
	jwtTokenGenerator, err := auth.NewJWTTokenGenerator(clientAuthKP.Seed())
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT token generator: %w", err)
	}

	requestSigner := auth.NewHTTPRequestSigner(jwtTokenGenerator)
	return wbclient.NewClient(wbURL, requestSigner), nil
}

// createRPCService initializes the RPC service
func createRPCService(ctx context.Context, containers *SharedContainers) (services.RPCService, error) {
	rpcURL, err := containers.RPCContainer.GetConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get RPC connection string: %w", err)
	}

	// Get database connection for metrics
	dbHost, err := containers.WalletDBContainer.GetHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database host: %w", err)
	}
	dbPort, err := containers.WalletDBContainer.GetPort(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database port: %w", err)
	}
	dbURL := fmt.Sprintf("postgres://postgres@%s:%s/wallet-backend?sslmode=disable", dbHost, dbPort)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection pool: %w", err)
	}
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sqlx db: %w", err)
	}

	// Initialize RPC service
	httpClient := &http.Client{Timeout: 30 * time.Second}
	metricsService := metrics.NewMetricsService(sqlxDB)
	rpcService, err := services.NewRPCService(rpcURL, networkPassphrase, httpClient, metricsService)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC service: %w", err)
	}

	// Start tracking RPC health
	go rpcService.TrackRPCServiceHealth(ctx, nil)

	return rpcService, nil
}

// NewTestEnvironment creates and initializes a test environment with all required services
func NewTestEnvironment(ctx context.Context, containers *SharedContainers) (*TestEnvironment, error) {
	// Initialize wallet-backend client
	wbClient, err := createWalletBackendClient(ctx, containers)
	if err != nil {
		return nil, err
	}

	// Initialize RPC service
	rpcService, err := createRPCService(ctx, containers)
	if err != nil {
		return nil, err
	}

	// Get keypairs
	primaryAccountKP := containers.GetPrimarySourceAccountKeyPair(ctx)
	secondaryAccountKP := containers.GetSecondarySourceAccountKeyPair(ctx)
	sponsoredNewAccountKP := containers.GetSponsoredNewAccountKeyPair(ctx)
	balanceTestAccount1KP := containers.GetBalanceTestAccount1KeyPair(ctx)
	balanceTestAccount2KP := containers.GetBalanceTestAccount2KeyPair(ctx)

	// Prepare use cases
	masterAccountKP := containers.GetMasterKeyPair(ctx)
	fixtures, err := NewFixtures(
		ctx,
		networkPassphrase,
		primaryAccountKP,
		secondaryAccountKP,
		sponsoredNewAccountKP,
		balanceTestAccount1KP,
		balanceTestAccount2KP,
		masterAccountKP,
		rpcService,
		containers.holderContractAddress,
		containers.eurcContractAddress,
		containers.sep41ContractAddress,
		containers.usdcContractAddress,
	)
	if err != nil {
		return nil, err
	}
	useCases, err := fixtures.PrepareUseCases(ctx)
	if err != nil {
		return nil, err
	}

	log.Ctx(ctx).Info("✅ Test environment setup complete")

	return &TestEnvironment{
		WBClient:              wbClient,
		RPCService:            rpcService,
		PrimaryAccountKP:      primaryAccountKP,
		SecondaryAccountKP:    secondaryAccountKP,
		SponsoredNewAccountKP: sponsoredNewAccountKP,
		BalanceTestAccount1KP: balanceTestAccount1KP,
		BalanceTestAccount2KP: balanceTestAccount2KP,
		USDCContractAddress:   containers.usdcContractAddress,
		EURCContractAddress:   containers.eurcContractAddress,
		// Pass through contract addresses for test assertions
		SEP41ContractAddress:  containers.sep41ContractAddress,
		HolderContractAddress: containers.holderContractAddress,
		UseCases:              useCases,
		NetworkPassphrase:     networkPassphrase,
		LiquidityPoolID:       fixtures.LiquidityPoolID,
	}, nil
}
