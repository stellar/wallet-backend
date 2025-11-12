// Package infrastructure provides container setup for integration tests
package infrastructure

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
	"github.com/stellar/wallet-backend/internal/entities"
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
	protocolVersion                  = 24 // Default protocol version for Stellar Core upgrades
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
		_, filename, _, _ := runtime.Caller(0)
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
	TestNetwork                   *testcontainers.DockerNetwork
	PostgresContainer             *TestContainer
	StellarCoreContainer          *TestContainer
	RPCContainer                  *TestContainer
	WalletDBContainer             *TestContainer
	RedisContainer                *TestContainer
	WalletBackendContainer        *WalletBackendContainer
	clientAuthKeyPair             *keypair.Full
	primarySourceAccountKeyPair   *keypair.Full
	secondarySourceAccountKeyPair *keypair.Full
	distributionAccountKeyPair    *keypair.Full
	sponsoredNewAccountKeyPair    *keypair.Full
	balanceTestAccount1KeyPair    *keypair.Full
	balanceTestAccount2KeyPair    *keypair.Full
	usdcContractAddress           string
	// sep41ContractAddress is the contract address (C...) of a deployed custom SEP-41 token.
	// This represents a non-SAC token contract that implements the SEP-41 token interface.
	// Used for testing contract balances for both G-addresses and C-addresses.
	sep41ContractAddress string
	// holderContractAddress is the contract address (C...) of a simple deployed contract
	// that can hold token balances. This contract doesn't implement token functionality itself,
	// but can receive and hold balances from both SAC and SEP-41 tokens.
	// Used for testing C-address token balances.
	holderContractAddress string
	masterAccount         *txnbuild.SimpleAccount
	masterKeyPair         *keypair.Full
}

// NewSharedContainers creates and starts all containers needed for integration tests
func NewSharedContainers(t *testing.T) *SharedContainers {
	// Get the directory of the current source file
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	shared := &SharedContainers{}

	ctx := context.Background()

	// Create network
	var err error
	shared.TestNetwork, err = network.New(ctx)
	require.NoError(t, err)

	shared.RedisContainer, err = createRedisContainer(ctx, shared.TestNetwork)
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
	shared.balanceTestAccount1KeyPair = keypair.MustRandom()
	shared.balanceTestAccount2KeyPair = keypair.MustRandom()

	// We are not funding this account, as it will be funded by the primary account through a sponsored account creation operation
	shared.sponsoredNewAccountKeyPair = keypair.MustRandom()

	// Create and fund all accounts in a single transaction
	shared.createAndFundAccounts(ctx, t, []*keypair.Full{
		shared.clientAuthKeyPair,
		shared.primarySourceAccountKeyPair,
		shared.secondarySourceAccountKeyPair,
		shared.distributionAccountKeyPair,
		shared.balanceTestAccount1KeyPair,
		shared.balanceTestAccount2KeyPair,
	})

	// Create trustlines - these will be use for the account balances test
	shared.createTrustlines(ctx, t, []*keypair.Full{
		shared.balanceTestAccount1KeyPair,
		shared.balanceTestAccount2KeyPair,
	})

	// Deploy native asset SAC so it can be used in smart contract operations
	shared.deployNativeAssetSAC(ctx, t)

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
	//   - Name: "USD Coin"
	//   - Symbol: "USDC"
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
	//
	log.Ctx(ctx).Info("📦 Deploying SEP-41 token contract...")

	// Step 1: Read the token contract WASM file
	// This is the standard token contract from soroban-examples v22.0.1
	sep41WasmBytes, err := os.ReadFile(filepath.Join(dir, "testdata", "soroban_token_contract.wasm"))
	require.NoError(t, err, "failed to read SEP-41 token WASM file")

	// Step 2: Upload the WASM bytecode to the ledger
	// This creates a LedgerEntryTypeContractCode entry and returns its hash
	sep41WasmHash := shared.uploadContractWasm(ctx, t, sep41WasmBytes)

	// Step 3: Build constructor arguments for the token contract
	// The soroban-examples token __constructor signature: __constructor(admin, decimal, name, symbol)
	// The constructor runs atomically during deployment, initializing the token in one step
	log.Ctx(ctx).Info("⚙️  Preparing constructor arguments for SEP-41 token...")

	// Arg 1: admin (Address) - The account that can mint tokens
	adminAccountID := xdr.MustAddress(shared.masterKeyPair.Address())
	adminSCAddress := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &adminAccountID,
	}
	adminArg := xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &adminSCAddress,
	}

	// Arg 2: decimal (u32) - Number of decimal places (7 for USDC-like tokens)
	decimalXDR := xdr.Uint32(7)
	decimalArg := xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  &decimalXDR,
	}

	// Arg 3: name (String) - Human-readable token name
	// Important: Use ScvString (not ScvBytes) for Soroban String types
	nameStr := xdr.ScString("USD Coin")
	nameArg := xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &nameStr,
	}

	// Arg 4: symbol (String) - Token symbol
	// Important: Use ScvString (not ScvBytes) for Soroban String types
	symbolStr := xdr.ScString("USDC")
	symbolArg := xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &symbolStr,
	}

	// Build the constructor arguments slice
	sep41ConstructorArgs := []xdr.ScVal{adminArg, decimalArg, nameArg, symbolArg}

	// Step 4: Deploy and initialize the contract atomically
	// The __constructor runs during deployment, so no separate initialization call is needed
	shared.sep41ContractAddress = shared.deployContractWithConstructor(ctx, t, sep41WasmHash, sep41ConstructorArgs)
	log.Ctx(ctx).Infof("✅ Deployed and initialized SEP-41 token contract at: %s (admin=%s, decimals=7, name=USD Coin, symbol=USDC)",
		shared.sep41ContractAddress, shared.masterKeyPair.Address())

	// Deploy Holder Contract
	// This deploys a simple contract that can receive and hold token balances.
	// Any Soroban contract can hold SEP-41 token balances - no special interface required.
	// When tokens are transferred to this contract's address (C...), the token contract
	// creates a contract data entry with key [Balance, C-address] to track the balance.
	log.Ctx(ctx).Info("📦 Deploying token holder contract...")
	holderWasmBytes, err := os.ReadFile(filepath.Join(dir, "testdata", "soroban_increment_contract.wasm"))
	require.NoError(t, err, "failed to read holder contract WASM file")
	holderWasmHash := shared.uploadContractWasm(ctx, t, holderWasmBytes)
	// The increment contract has no constructor, so we pass an empty slice
	shared.holderContractAddress = shared.deployContractWithConstructor(ctx, t, holderWasmHash, []xdr.ScVal{})
	log.Ctx(ctx).Infof("✅ Deployed holder contract at: %s", shared.holderContractAddress)

	// Create SEP-41 Balance for G-Address
	// Mint SEP-41 tokens to balanceTestAccount1. This creates a contract data entry
	// in the ledger: LedgerEntryTypeContractData with key [Balance, G-address].
	// This represents a pure contract token balance (not a classic trustline).
	log.Ctx(ctx).Info("💰 Minting SEP-41 tokens to G-address...")
	shared.mintSEP41Tokens(ctx, t, shared.sep41ContractAddress, shared.balanceTestAccount1KeyPair.Address(), 5000000000) // 500 tokens with 7 decimals
	log.Ctx(ctx).Infof("✅ Minted SEP-41 tokens to %s", shared.balanceTestAccount1KeyPair.Address())

	// Create SAC Balance for C-Address
	// Transfer USDC SAC tokens to the holder contract. This creates a contract data entry
	// in the ledger: LedgerEntryTypeContractData with key [Balance, C-address].
	// This demonstrates that contracts can hold SAC token balances.
	log.Ctx(ctx).Info("💸 Transferring USDC SAC tokens to C-address...")
	shared.invokeContractTransfer(ctx, t, shared.usdcContractAddress, shared.masterKeyPair.Address(), shared.holderContractAddress, 2000000000) // 200 tokens with 7 decimals
	log.Ctx(ctx).Infof("✅ Transferred USDC SAC tokens to contract %s", shared.holderContractAddress)

	// Create SEP-41 Balance for C-Address
	// Transfer SEP-41 tokens to the holder contract. This creates another contract data entry
	// for the holder contract: LedgerEntryTypeContractData with key [Balance, C-address].
	// A contract can hold balances from multiple different tokens simultaneously.
	log.Ctx(ctx).Info("💸 Transferring SEP-41 tokens to C-address...")
	shared.mintSEP41Tokens(ctx, t, shared.sep41ContractAddress, shared.holderContractAddress, 3000000000) // 300 tokens with 7 decimals
	log.Ctx(ctx).Infof("✅ Transferred SEP-41 tokens to contract %s", shared.holderContractAddress)

	// Start PostgreSQL for wallet-backend
	shared.WalletDBContainer, err = createWalletDBContainer(ctx, shared.TestNetwork)
	require.NoError(t, err)

	// Build or verify wallet-backend Docker image
	walletBackendImage, err := ensureWalletBackendImage(ctx, walletBackendContainerTag)
	require.NoError(t, err)

	// Start wallet-backend ingest
	shared.WalletBackendContainer = &WalletBackendContainer{}
	shared.WalletBackendContainer.Ingest, err = createWalletBackendIngestContainer(ctx, walletBackendIngestContainerName,
		walletBackendImage, shared.TestNetwork, shared.clientAuthKeyPair, shared.distributionAccountKeyPair)
	require.NoError(t, err)

	// Start wallet-backend service
	shared.WalletBackendContainer.API, err = createWalletBackendAPIContainer(ctx, walletBackendAPIContainerName,
		walletBackendImage, shared.TestNetwork, shared.clientAuthKeyPair, shared.distributionAccountKeyPair)
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

func (s *SharedContainers) GetSponsoredNewAccountKeyPair(ctx context.Context) *keypair.Full {
	return s.sponsoredNewAccountKeyPair
}

func (s *SharedContainers) GetBalanceTestAccount1KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount1KeyPair
}

func (s *SharedContainers) GetBalanceTestAccount2KeyPair(ctx context.Context) *keypair.Full {
	return s.balanceTestAccount2KeyPair
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

	// Build initial transaction for simulation
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{deployOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to build SAC deployment transaction")

	// Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC connection string")

	// Simulate transaction to get resource footprint
	txXDR, err := tx.Base64()
	require.NoError(t, err, "failed to encode SAC deployment transaction for simulation")

	client := &http.Client{Timeout: 30 * time.Second}
	simulationResult, err := simulateTransactionRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to simulate SAC deployment transaction")
	require.Empty(t, simulationResult.Error, "SAC deployment simulation failed: %s", simulationResult.Error)

	// Apply simulation results to the operation
	deployOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Parse MinResourceFee from string to int64
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	require.NoError(t, err, "failed to parse MinResourceFee")

	// Rebuild transaction with simulation results and increment sequence
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{deployOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to rebuild SAC deployment transaction")

	// Sign with master key
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err, "failed to sign SAC deployment transaction")

	txXDR, err = tx.Base64()
	require.NoError(t, err, "failed to encode signed SAC deployment transaction")

	// Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit SAC deployment transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "SAC deployment transaction failed with status: %s, hash: %s, errorResultXdr: %s", sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)

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
	require.True(t, confirmed, "SAC deployment transaction not confirmed after 10 seconds")

	log.Ctx(ctx).Info("✅ Native asset SAC deployed successfully")
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

// createTrustlines creates test trustlines for balance test accounts
func (s *SharedContainers) createTrustlines(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
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

	// Build ChangeTrust operations for all accounts
	ops := make([]txnbuild.Operation, 0)
	for _, kp := range accounts {
		ops = append(ops, &txnbuild.ChangeTrust{
			Line: txnbuild.ChangeTrustAssetWrapper{
				Asset: testAsset,
			},
			Limit:         "1000000", // 1 million units
			SourceAccount: kp.Address(),
		}, &txnbuild.Payment{
			Destination:   kp.Address(),
			Amount:        "100",
			Asset:         testAsset,
			SourceAccount: s.masterKeyPair.Address(),
		})
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

	// Sign with master key and each account's key
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err)
	for _, kp := range accounts {
		tx, err = tx.Sign(networkPassphrase, kp)
		require.NoError(t, err)
	}

	// Get RPC URL and submit
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err)

	txXDR, err := tx.Base64()
	require.NoError(t, err)

	// Submit transaction to RPC
	client := &http.Client{Timeout: 30 * time.Second}
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit trustline creation transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "trustline creation transaction failed with status: %s", sendResult.Status)

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
	require.True(t, confirmed, "trustline creation transaction not confirmed after 10 seconds")

	// Log created trustlines
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("🔗 Created balance test trustline for account %s: %s:%s", kp.Address(), testAsset.Code, testAsset.Issuer)
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
	_, filename, _, _ := runtime.Caller(0)
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
	_, filename, _, _ := runtime.Caller(0)
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

// simulateTransactionRPC simulates a transaction via RPC to get resource footprint
func simulateTransactionRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSimulateTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "simulateTransaction",
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
		Result entities.RPCSimulateTransactionResult `json:"result"`
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

	// Build initial transaction for simulation
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{uploadOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to build WASM upload transaction")

	// Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC connection string")

	// Simulate transaction to get resource footprint
	txXDR, err := tx.Base64()
	require.NoError(t, err, "failed to encode WASM upload transaction for simulation")

	client := &http.Client{Timeout: 30 * time.Second}
	simulationResult, err := simulateTransactionRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to simulate WASM upload transaction")
	require.Empty(t, simulationResult.Error, "WASM upload simulation failed: %s", simulationResult.Error)

	// Apply simulation results to the operation
	uploadOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Parse MinResourceFee from string to int64
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	require.NoError(t, err, "failed to parse MinResourceFee")

	// Rebuild transaction with simulation results and increment sequence
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{uploadOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to rebuild WASM upload transaction")

	// Sign with master key
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err, "failed to sign WASM upload transaction")

	txXDR, err = tx.Base64()
	require.NoError(t, err, "failed to encode signed WASM upload transaction")

	// Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit WASM upload transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "WASM upload transaction failed with status: %s, hash: %s, errorResultXdr: %s", sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)

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
	require.True(t, confirmed, "WASM upload transaction not confirmed after 10 seconds")

	// Compute WASM hash from the uploaded bytecode
	wasmHash := xdr.Hash(sha256.Sum256(wasmBytes))
	log.Ctx(ctx).Infof("✅ Uploaded contract WASM with hash: %x", wasmHash)

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

	// Step 5: Build initial transaction for simulation
	// Simulation is required to determine resource footprint before actual deployment
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{deployOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to build contract deployment transaction")

	// Get RPC URL for simulation and submission
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC connection string")

	// Step 6: Simulate transaction to get resource footprint
	// If the contract has a constructor, this simulation will execute it
	txXDR, err := tx.Base64()
	require.NoError(t, err, "failed to encode contract deployment transaction for simulation")

	client := &http.Client{Timeout: 30 * time.Second}
	simulationResult, err := simulateTransactionRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to simulate contract deployment transaction")
	require.Empty(t, simulationResult.Error, "contract deployment simulation failed: %s", simulationResult.Error)

	// Step 7: Apply simulation results to the operation
	// The TransactionData contains the resource footprint needed for deployment
	deployOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Parse MinResourceFee from simulation result
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	require.NoError(t, err, "failed to parse MinResourceFee")

	// Step 8: Rebuild transaction with simulation results and increment sequence
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{deployOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true, // Now we increment the sequence number
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to rebuild contract deployment transaction")

	// Step 9: Sign with master key
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err, "failed to sign contract deployment transaction")

	txXDR, err = tx.Base64()
	require.NoError(t, err, "failed to encode signed contract deployment transaction")

	// Step 10: Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit contract deployment transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "contract deployment transaction failed with status: %s, hash: %s, errorResultXdr: %s", sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)

	// Step 11: Wait for transaction to be confirmed on the ledger
	hash := sendResult.Hash
	var confirmed bool
	for range 20 { // Try for up to 10 seconds
		time.Sleep(500 * time.Millisecond)
		txResult, err := getTransactionFromRPC(client, rpcURL, hash)
		if err == nil {
			if txResult.Status == entities.SuccessStatus {
				confirmed = true
				break
			}
			if txResult.Status == entities.FailedStatus {
				require.Fail(t, "contract deployment transaction failed with resultXdr: %s", txResult.ResultXDR)
			}
		}
	}
	require.True(t, confirmed, "contract deployment transaction not confirmed after 20 seconds")

	// Step 12: Calculate the contract address from the preimage
	// The contract address is deterministically computed from the deployer address and salt
	contractAddress, err := s.calculateContractID(networkPassphrase, preimage.MustFromAddress())
	require.NoError(t, err, "failed to encode contract ID")

	log.Ctx(ctx).Infof("✅ Deployed contract at address: %s", contractAddress)
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
//
// Process:
//  1. Decode contract and recipient addresses to XDR format
//  2. Encode amount as i128 XDR type (128-bit integer for large token amounts)
//  3. Build contract invocation with "mint" function
//  4. Simulate to get resource limits (CPU, memory, storage)
//  5. Sign with admin account (master key)
//  6. Submit and wait for confirmation
func (s *SharedContainers) mintSEP41Tokens(ctx context.Context, t *testing.T, tokenContractAddress, toAddress string, amount int64) {
	// Step 1: Decode token contract address to get contract ID
	// Contract addresses (C...) are base32-encoded 32-byte identifiers
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Step 2: Build recipient ScAddress
	// Stellar has two address types: accounts (G...) and contracts (C...)
	// We need to detect which type and encode appropriately for XDR
	var toSCAddress xdr.ScAddress
	if strings.HasPrefix(toAddress, "G") {
		// G-address: Account address (user wallet)
		// When minting to a G-address, the token contract creates a LedgerEntryTypeContractData
		// entry with key [Balance, G-address] to store the account's token balance
		toAccountID := xdr.MustAddress(toAddress)
		toSCAddress = xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &toAccountID,
		}
	} else if strings.HasPrefix(toAddress, "C") {
		// C-address: Contract address (smart contract)
		// Contracts can also hold token balances. The balance is stored similarly but
		// with key [Balance, C-address] instead
		toContractID, err := strkey.Decode(strkey.VersionByteContract, toAddress)
		require.NoError(t, err, "failed to decode recipient contract address")
		var toID xdr.ContractId
		copy(toID[:], toContractID)
		toSCAddress = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &toID,
		}
	} else {
		require.Fail(t, "invalid address format: must start with G or C")
	}

	// Step 3: Build the mint invocation
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
					// For simplicity, we use Lo (lower 64 bits) and set Hi to 0
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

	// Step 4: Build initial transaction for simulation
	// Simulation is mandatory for Soroban contracts to determine resource requirements
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to build mint transaction")

	// Get RPC URL for simulation and submission
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC connection string")

	// Step 5: Simulate transaction to get resource footprint
	// Simulation executes the contract and returns:
	//   - TransactionData: Resource footprint (which ledger entries to read/write)
	//   - MinResourceFee: Minimum fee required for the resources used
	//   - Result: The contract's return value (if successful)
	txXDR, err := tx.Base64()
	require.NoError(t, err, "failed to encode mint transaction for simulation")

	client := &http.Client{Timeout: 30 * time.Second}
	simulationResult, err := simulateTransactionRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to simulate mint transaction")
	require.Empty(t, simulationResult.Error, "mint simulation failed: %s", simulationResult.Error)

	// Step 6: Apply simulation results to the operation
	// The TransactionData contains the resource footprint - which ledger entries
	// the transaction will read/write. This is required for the actual transaction.
	invokeOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Parse MinResourceFee from simulation result
	// This is the fee for resources used (CPU instructions, memory, storage)
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	require.NoError(t, err, "failed to parse MinResourceFee")

	// Step 7: Rebuild transaction with simulation results and increment sequence
	// Now we create the actual transaction with correct resource limits
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee, // Resource fee + base fee
		IncrementSequenceNum: true,                                 // Now we increment sequence
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to rebuild mint transaction")

	// Step 8: Sign with admin key (master key)
	// Only the admin account can mint tokens, so we sign with the master keypair
	// that was set as admin during token initialization
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err, "failed to sign mint transaction")

	txXDR, err = tx.Base64()
	require.NoError(t, err, "failed to encode signed mint transaction")

	// Step 9: Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit mint transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "mint transaction failed with status: %s, hash: %s, errorResultXdr: %s", sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)

	// Step 10: Wait for transaction to be confirmed on the ledger
	// Poll the RPC server until the transaction status becomes "SUCCESS"
	hash := sendResult.Hash
	var confirmed bool
	for range 20 { // Try for up to 10 seconds (20 * 500ms)
		time.Sleep(500 * time.Millisecond)
		txResult, err := getTransactionFromRPC(client, rpcURL, hash)
		if err == nil && txResult.Status == entities.SuccessStatus {
			confirmed = true
			break
		}
	}
	require.True(t, confirmed, "mint transaction not confirmed after 10 seconds")

	log.Ctx(ctx).Infof("✅ Minted %d tokens to %s", amount, toAddress)
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
//
// The function:
//  1. Decodes token contract address to get contract ID
//  2. Builds ScAddress structures for from and to addresses
//  3. Creates InvokeContractArgs with "transfer" function and arguments
//  4. Simulates transaction to get resource footprint
//  5. Signs with appropriate keys (master for account transfers)
//  6. Submits to RPC and waits for confirmation
func (s *SharedContainers) invokeContractTransfer(ctx context.Context, t *testing.T, tokenContractAddress, fromAddress, toAddress string, amount int64) {
	// Decode token contract address to get contract ID
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Build from ScAddress (can be account G... or contract C...)
	var fromSCAddress xdr.ScAddress
	if strings.HasPrefix(fromAddress, "G") {
		// Account address
		fromAccountID := xdr.MustAddress(fromAddress)
		fromSCAddress = xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &fromAccountID,
		}
	} else if strings.HasPrefix(fromAddress, "C") {
		// Contract address
		fromContractID, err := strkey.Decode(strkey.VersionByteContract, fromAddress)
		require.NoError(t, err, "failed to decode sender contract address")
		var fromID xdr.ContractId
		copy(fromID[:], fromContractID)
		fromSCAddress = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &fromID,
		}
	} else {
		require.Fail(t, "invalid from address format: must start with G or C")
	}

	// Build to ScAddress (can be account G... or contract C...)
	var toSCAddress xdr.ScAddress
	if strings.HasPrefix(toAddress, "G") {
		// Account address
		toAccountID := xdr.MustAddress(toAddress)
		toSCAddress = xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &toAccountID,
		}
	} else if strings.HasPrefix(toAddress, "C") {
		// Contract address
		toContractID, err := strkey.Decode(strkey.VersionByteContract, toAddress)
		require.NoError(t, err, "failed to decode recipient contract address")
		var toID xdr.ContractId
		copy(toID[:], toContractID)
		toSCAddress = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &toID,
		}
	} else {
		require.Fail(t, "invalid to address format: must start with G or C")
	}

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

	// Build initial transaction for simulation
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to build transfer transaction")

	// Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC connection string")

	// Simulate transaction to get resource footprint
	txXDR, err := tx.Base64()
	require.NoError(t, err, "failed to encode transfer transaction for simulation")

	client := &http.Client{Timeout: 30 * time.Second}
	simulationResult, err := simulateTransactionRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to simulate transfer transaction")
	require.Empty(t, simulationResult.Error, "transfer simulation failed: %s", simulationResult.Error)

	// Apply simulation results to the operation
	invokeOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Parse MinResourceFee from string to int64
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	require.NoError(t, err, "failed to parse MinResourceFee")

	// Rebuild transaction with simulation results and increment sequence
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "failed to rebuild transfer transaction")

	// Sign with master key (assuming master has authority to transfer)
	tx, err = tx.Sign(networkPassphrase, s.masterKeyPair)
	require.NoError(t, err, "failed to sign transfer transaction")

	txXDR, err = tx.Base64()
	require.NoError(t, err, "failed to encode signed transfer transaction")

	// Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(client, rpcURL, txXDR)
	require.NoError(t, err, "failed to submit transfer transaction")
	require.NotEqual(t, entities.ErrorStatus, sendResult.Status, "transfer transaction failed with status: %s, hash: %s, errorResultXdr: %s", sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)

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
	require.True(t, confirmed, "transfer transaction not confirmed after 10 seconds")

	log.Ctx(ctx).Infof("✅ Transferred %d tokens from %s to %s", amount, fromAddress, toAddress)
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
func createWalletBackendClient(containers *SharedContainers, ctx context.Context) (*wbclient.Client, error) {
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
func createRPCService(containers *SharedContainers, ctx context.Context) (services.RPCService, error) {
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
func NewTestEnvironment(containers *SharedContainers, ctx context.Context) (*TestEnvironment, error) {
	// Initialize wallet-backend client
	wbClient, err := createWalletBackendClient(containers, ctx)
	if err != nil {
		return nil, err
	}

	// Initialize RPC service
	rpcService, err := createRPCService(containers, ctx)
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
	fixtures, err := NewFixtures(ctx, networkPassphrase, primaryAccountKP, secondaryAccountKP, sponsoredNewAccountKP, rpcService)
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
		// Pass through contract addresses for test assertions
		SEP41ContractAddress:  containers.sep41ContractAddress,
		HolderContractAddress: containers.holderContractAddress,
		UseCases:              useCases,
		NetworkPassphrase:     networkPassphrase,
		LiquidityPoolID:       fixtures.LiquidityPoolID,
	}, nil
}
