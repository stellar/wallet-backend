package infrastructure

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

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

// initializeContainerInfrastructure sets up Docker network and core infrastructure containers.
func (s *SharedContainers) initializeContainerInfrastructure(ctx context.Context) error {
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
	s.CreateAndFundAccounts(ctx, t, []*keypair.Full{
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
func (s *SharedContainers) setupWalletBackend(ctx context.Context) error {
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
		walletBackendImage, s.TestNetwork, s.clientAuthKeyPair, s.distributionAccountKeyPair, nil)
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

// NewSharedContainers creates and starts all containers needed for integration tests
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
	err := shared.initializeContainerInfrastructure(ctx)
	require.NoError(t, err, "failed to initialize container infrastructure")

	// Setup test accounts
	err = shared.setupTestAccounts(ctx, t)
	require.NoError(t, err, "failed to setup test accounts")

	// Setup contracts
	err = shared.setupContracts(ctx, t, dir)
	require.NoError(t, err, "failed to setup contracts")

	// Setup wallet backend
	err = shared.setupWalletBackend(ctx)
	require.NoError(t, err, "failed to setup wallet backend")

	return shared
}

// GetClientAuthKeyPair returns the client authentication keypair
func (s *SharedContainers) GetClientAuthKeyPair(ctx context.Context) *keypair.Full {
	return s.clientAuthKeyPair
}

// GetPrimarySourceAccountKeyPair returns the primary source account keypair
func (s *SharedContainers) GetPrimarySourceAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.primarySourceAccountKeyPair
}

// GetSecondarySourceAccountKeyPair returns the secondary source account keypair
func (s *SharedContainers) GetSecondarySourceAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.secondarySourceAccountKeyPair
}

// GetSponsoredNewAccountKeyPair returns the sponsored new account keypair
func (s *SharedContainers) GetSponsoredNewAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.sponsoredNewAccountKeyPair
}

// GetBalanceTestAccount1KeyPair returns the first balance test account keypair
func (s *SharedContainers) GetBalanceTestAccount1KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount1KeyPair
}

// GetBalanceTestAccount2KeyPair returns the second balance test account keypair
func (s *SharedContainers) GetBalanceTestAccount2KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount2KeyPair
}

// GetMasterKeyPair returns the master account keypair
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

// TestEnvironment holds all initialized services and clients for integration tests
type TestEnvironment struct {
	Containers            *SharedContainers
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
	HolderContractAddress string
	// MasterAccountAddress is the address of the master/root account that issues classic assets.
	// Used for asserting issuer addresses in trustline and SAC balance tests.
	MasterAccountAddress     string
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
	go func() {
		//nolint:errcheck // Error is expected on context cancellation during shutdown
		rpcService.TrackRPCServiceHealth(ctx, nil)
	}()

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
		Containers:            containers,
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
		MasterAccountAddress:  masterAccountKP.Address(),
		UseCases:              useCases,
		NetworkPassphrase:     networkPassphrase,
		LiquidityPoolID:       fixtures.LiquidityPoolID,
	}, nil
}
