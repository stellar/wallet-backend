package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/sac"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

const (
	// Redis key prefixes for account token storage
	trustlinesKeyPrefix = "trustlines:"
	contractsKeyPrefix  = "contracts:"

	// redisPipelineBatchSize is the number of operations to batch
	// in a single Redis pipeline for token cache population.
	redisPipelineBatchSize = 50000

	simulateTransactionBatchSize = 20
)

// contractMetadata holds the metadata for a contract token (name, symbol, decimals).
type contractMetadata struct {
	Type     types.ContractType
	Code     string
	Issuer   string
	Name     string
	Symbol   string
	Decimals uint32
}

// checkpointData holds all data collected from processing a checkpoint ledger.
type checkpointData struct {
	// Trustlines maps account addresses (G...) to their trustline assets formatted as "CODE:ISSUER"
	Trustlines map[string][]string
	// Contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	Contracts map[string][]string
	// ContractTypesByContractID tracks the token type for each unique contract ID
	ContractTypesByContractID map[string]types.ContractType
	// ContractIDsByWasmHash groups contract IDs by their WASM hash for batch validation
	ContractIDsByWasmHash map[xdr.Hash][]string
	// ContractCodesByWasmHash maps WASM hashes to their contract code bytes
	ContractCodesByWasmHash map[xdr.Hash][]byte
}

// AccountTokenService manages Redis caching of account token holdings,
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
type AccountTokenService interface {
	// GetCheckpointLedger returns the ledger sequence number of the checkpoint
	// used to populate the initial account token cache.
	GetCheckpointLedger() uint32

	// PopulateAccountTokens performs initial Redis cache population from Stellar
	// history archive for the latest checkpoint. It extracts all trustlines and contract
	// tokens from checkpoint ledger entries and stores them in Redis.
	PopulateAccountTokens(ctx context.Context) error

	// AddTrustlines adds trustline assets to an account's Redis set.
	// Assets should be formatted as "CODE:ISSUER".
	// Returns nil if assets is empty (no-op).
	AddTrustlines(ctx context.Context, accountAddress string, assets []string) error

	// AddContracts adds contract token IDs to an account's Redis set.
	// Assets should be contract addresses starting with "C".
	// Returns nil if assets is empty (no-op).
	AddContracts(ctx context.Context, accountAddress string, contractIDs []string) error

	// GetAccountTrustlines retrieves all classic trustline assets for an account.
	// Returns a slice of assets formatted as "CODE:ISSUER", or empty slice if none exist.
	GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error)

	// GetAccountContracts retrieves all contract token IDs for an account from Redis.
	GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error)
}

var _ AccountTokenService = (*accountTokenService)(nil)

type accountTokenService struct {
	checkpointLedger  uint32
	archive           historyarchive.ArchiveInterface
	contractValidator ContractValidator
	redisStore        *store.RedisStore
	rpcService        RPCService
	contractModel     *data.ContractModel
	pool              pond.Pool
	networkPassphrase string
	trustlinesPrefix  string
	contractsPrefix   string
	dummyAccount      *keypair.Full
}

func NewAccountTokenService(
	networkPassphrase string,
	archiveURL string,
	redisStore *store.RedisStore,
	contractValidator ContractValidator,
	rpcService RPCService,
	contractModel *data.ContractModel,
	pool pond.Pool,
	checkpointFrequency uint32,
) (AccountTokenService, error) {
	var archive historyarchive.ArchiveInterface

	// Only connect to archive if URL is provided (needed for ingest, not for serve)
	if archiveURL != "" {
		var err error
		archive, err = historyarchive.Connect(
			archiveURL,
			historyarchive.ArchiveOptions{
				NetworkPassphrase:   networkPassphrase,
				CheckpointFrequency: checkpointFrequency,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("connecting to history archive: %w", err)
		}
	}

	return &accountTokenService{
		checkpointLedger:  0,
		archive:           archive,
		contractValidator: contractValidator,
		redisStore:        redisStore,
		rpcService:        rpcService,
		contractModel:     contractModel,
		pool:              pool,
		networkPassphrase: networkPassphrase,
		trustlinesPrefix:  trustlinesKeyPrefix,
		contractsPrefix:   contractsKeyPrefix,
		dummyAccount:      keypair.MustRandom(),
	}, nil
}

// PopulateAccountTokens performs initial Redis cache population from Stellar history archive.
// This reads the latest checkpoint ledger and extracts all trustlines and contract tokens that an account has.
// Warning: This is a long-running operation that may take several minutes.
func (s *accountTokenService) PopulateAccountTokens(ctx context.Context) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateAccountTokens requires archive connection")
	}

	defer func() {
		if err := s.contractValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	latestCheckpointLedger, err := getLatestCheckpointLedger(s.archive)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Infof("Populating account token cache from checkpoint ledger = %d", latestCheckpointLedger)
	s.checkpointLedger = latestCheckpointLedger

	// Create checkpoint change reader
	reader, err := ingest.NewCheckpointChangeReader(ctx, s.archive, latestCheckpointLedger)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing checkpoint reader: %v", closeErr)
		}
	}()

	// Collect account tokens from checkpoint
	checkpointData, err := s.collectAccountTokensFromCheckpoint(ctx, reader)
	if err != nil {
		return err
	}

	// Extract contract spec from WASM hash and validate SEP-41 contracts
	s.enrichContractTypes(ctx, checkpointData.ContractTypesByContractID, checkpointData.ContractIDsByWasmHash, checkpointData.ContractCodesByWasmHash)

	// Fetch metadata for non-SAC contracts and store in database
	if err := s.fetchAndStoreContractMetadata(ctx, checkpointData.ContractTypesByContractID); err != nil {
		log.Ctx(ctx).Warnf("Failed to fetch and store contract metadata: %v", err)
		// Don't fail the entire process if metadata fetch fails
	}

	return s.storeAccountTokensInRedis(ctx, checkpointData.Trustlines, checkpointData.Contracts)
}

// validateAccountAddress checks if the account address is valid (non-empty).
func validateAccountAddress(accountAddress string) error {
	if accountAddress == "" {
		return fmt.Errorf("account address cannot be empty")
	}
	return nil
}

// buildTrustlineKey constructs the Redis key for an account's trustlines set.
func (s *accountTokenService) buildTrustlineKey(accountAddress string) string {
	return s.trustlinesPrefix + accountAddress
}

// buildContractKey constructs the Redis key for an account's contracts set.
func (s *accountTokenService) buildContractKey(accountAddress string) string {
	return s.contractsPrefix + accountAddress
}

// AddTrustlines adds trustline assets to an account's Redis set.
// Assets are formatted as "CODE:ISSUER".
// Returns nil if assets is empty (no-op).
func (s *accountTokenService) AddTrustlines(ctx context.Context, accountAddress string, assets []string) error {
	if err := validateAccountAddress(accountAddress); err != nil {
		return err
	}
	if len(assets) == 0 {
		return nil
	}
	key := s.buildTrustlineKey(accountAddress)
	if err := s.redisStore.SAdd(ctx, key, assets...); err != nil {
		return fmt.Errorf("adding trustlines for account %s (key: %s): %w", accountAddress, key, err)
	}
	return nil
}

// AddContracts adds contract token IDs to an account's Redis set.
// Contract IDs are contract addresses starting with "C".
// Returns nil if contractIDs is empty (no-op).
func (s *accountTokenService) AddContracts(ctx context.Context, accountAddress string, contractIDs []string) error {
	if err := validateAccountAddress(accountAddress); err != nil {
		return err
	}
	if len(contractIDs) == 0 {
		return nil
	}
	key := s.buildContractKey(accountAddress)
	if err := s.redisStore.SAdd(ctx, key, contractIDs...); err != nil {
		return fmt.Errorf("adding contracts for account %s (key: %s): %w", accountAddress, key, err)
	}
	return nil
}

// GetAccountTrustlines retrieves all trustlines for an account from Redis.
func (s *accountTokenService) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	if err := validateAccountAddress(accountAddress); err != nil {
		return nil, err
	}
	key := s.buildTrustlineKey(accountAddress)
	trustlines, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	return trustlines, nil
}

// GetAccountContracts retrieves all contract token IDs for an account from Redis.
// For G-address: all non-SAC custom tokens because SAC tokens are already tracked in trustlines
// For C-address: all contract tokens (SAC, custom)
func (s *accountTokenService) GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error) {
	if err := validateAccountAddress(accountAddress); err != nil {
		return nil, err
	}
	key := s.buildContractKey(accountAddress)
	contracts, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting contracts for account %s: %w", accountAddress, err)
	}
	return contracts, nil
}

// GetCheckpointLedger returns the ledger sequence number of the checkpoint used for initial cache population.
func (s *accountTokenService) GetCheckpointLedger() uint32 {
	return s.checkpointLedger
}

// processTrustlineChange extracts trustline information from a ledger change entry.
// Returns the account address and asset string, with skip=true if the entry should be skipped.
func (s *accountTokenService) processTrustlineChange(change ingest.Change) (accountAddress string, assetStr string, skip bool) {
	trustlineEntry := change.Post.Data.MustTrustLine()
	accountAddress = trustlineEntry.AccountId.Address()
	asset := trustlineEntry.Asset

	// Skip liquidity pool shares as they're tracked separately via pool-specific indexing
	// and don't represent traditional trustlines.
	if asset.Type == xdr.AssetTypeAssetTypePoolShare {
		return "", "", true
	}

	var assetType, assetCode, assetIssuer string
	if err := trustlineEntry.Asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return "", "", true
	}

	assetStr = fmt.Sprintf("%s:%s", assetCode, assetIssuer)
	return accountAddress, assetStr, false
}

// processContractBalanceChange extracts contract balance information from a contract data entry.
// Returns the holder address and contract ID, with skip=true if extraction fails.
func (s *accountTokenService) processContractBalanceChange(contractDataEntry xdr.ContractDataEntry) (holderAddress string, skip bool) {
	// Extract the account/contract address from the contract data entry key.
	// We parse using the [Balance, holder_address] format that is followed by SEP-41 tokens.
	// However, this could also be valid for any non-SEP41 contract that mimics the same format.
	var err error
	holderAddress, err = s.extractHolderAddress(contractDataEntry.Key)
	if err != nil {
		return "", true
	}

	return holderAddress, false
}

// processContractInstanceChange extracts contract type information from a contract instance entry.
// Updates the contractTypesByContractID map with SAC types, and returns WASM hash for non-SAC contracts.
func (s *accountTokenService) processContractInstanceChange(
	change ingest.Change,
	contractAddress string,
	contractDataEntry xdr.ContractDataEntry,
	contractTypesByContractID map[string]types.ContractType,
) (wasmHash *xdr.Hash, skip bool) {
	ledgerEntry := change.Post
	_, isSAC := sac.AssetFromContractData(*ledgerEntry, s.networkPassphrase)
	if isSAC {
		contractTypesByContractID[contractAddress] = types.ContractTypeSAC // Verified SAC
		return nil, true
	}

	// For non-SAC contracts, extract WASM hash for later validation
	contractInstance := contractDataEntry.Val.MustInstance()
	if contractInstance.Executable.Type == xdr.ContractExecutableTypeContractExecutableWasm {
		if contractInstance.Executable.WasmHash != nil {
			hash := *contractInstance.Executable.WasmHash
			return &hash, false
		}
	}

	return nil, true
}

// collectAccountTokensFromCheckpoint reads from a ChangeReader and collects all trustlines and contract balances.
// Returns checkpointData containing maps of trustlines, contracts, contract types, and contract IDs grouped by WASM hash.
func (s *accountTokenService) collectAccountTokensFromCheckpoint(
	ctx context.Context,
	reader ingest.ChangeReader,
) (checkpointData, error) {
	data := checkpointData{
		Trustlines:                make(map[string][]string),
		Contracts:                 make(map[string][]string),
		ContractTypesByContractID: make(map[string]types.ContractType),
		ContractIDsByWasmHash:     make(map[xdr.Hash][]string),
		ContractCodesByWasmHash:   make(map[xdr.Hash][]byte),
	}

	entries := 0
	startTime := time.Now()

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return checkpointData{}, fmt.Errorf("checkpoint processing cancelled: %w", ctx.Err())
		default:
		}

		change, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return checkpointData{}, fmt.Errorf("reading checkpoint changes: %w", err)
		}

		//exhaustive:ignore
		switch change.Type {
		case xdr.LedgerEntryTypeTrustline:
			accountAddress, assetStr, skip := s.processTrustlineChange(change)
			if skip {
				continue
			}
			entries++
			data.Trustlines[accountAddress] = append(data.Trustlines[accountAddress], assetStr)

		case xdr.LedgerEntryTypeContractCode:
			contractCodeEntry := change.Post.Data.MustContractCode()
			data.ContractCodesByWasmHash[contractCodeEntry.Hash] = contractCodeEntry.Code
			entries++

		case xdr.LedgerEntryTypeContractData:
			contractDataEntry := change.Post.Data.MustContractData()

			contractAddress, ok := contractDataEntry.Contract.GetContractId()
			if !ok {
				continue
			}
			contractAddressStr := strkey.MustEncode(strkey.VersionByteContract, contractAddress[:])

			//exhaustive:ignore
			switch contractDataEntry.Key.Type {
			case xdr.ScValTypeScvVec:
				holderAddress, skip := s.processContractBalanceChange(contractDataEntry)
				if skip {
					continue
				}
				data.Contracts[holderAddress] = append(data.Contracts[holderAddress], contractAddressStr)
				entries++

			case xdr.ScValTypeScvLedgerKeyContractInstance:
				wasmHash, skip := s.processContractInstanceChange(change, contractAddressStr, contractDataEntry, data.ContractTypesByContractID)
				if skip {
					continue
				}
				// For non-SAC contracts with WASM hash, track for later validation
				data.ContractIDsByWasmHash[*wasmHash] = append(data.ContractIDsByWasmHash[*wasmHash], contractAddressStr)
				entries++
			}
		}
	}

	log.Ctx(ctx).Infof("Processed %d checkpoint entries in %.2f minutes", entries, time.Since(startTime).Minutes())
	return data, nil
}

// enrichContractTypes validates contract specs and enriches the contractTypesByContractID map with SEP-41 classifications.
func (s *accountTokenService) enrichContractTypes(
	ctx context.Context,
	contractTypesByContractID map[string]types.ContractType,
	contractIDsByWasmHash map[xdr.Hash][]string,
	contractCodesByWasmHash map[xdr.Hash][]byte,
) {
	for wasmHash, contractCode := range contractCodesByWasmHash {
		contractType, err := s.contractValidator.ValidateFromContractCode(ctx, contractCode)
		if err != nil {
			log.Ctx(ctx).Warnf("Failed to validate contract code for WASM hash %s: %v", wasmHash.HexString(), err)
			continue
		}
		if contractType == types.ContractTypeUnknown {
			continue
		}

		// We only assign types for validated specs
		for _, contractAddress := range contractIDsByWasmHash[wasmHash] {
			contractTypesByContractID[contractAddress] = contractType
		}
	}
}

// storeAccountTokensInRedis stores all collected trustlines and contracts into Redis using pipelining.
func (s *accountTokenService) storeAccountTokensInRedis(
	ctx context.Context,
	trustlines map[string][]string,
	contracts map[string][]string,
) error {
	startTime := time.Now()

	// Calculate total operations: trustlines + contracts
	totalOps := len(trustlines) + len(contracts)
	redisPipelineOps := make([]store.RedisPipelineOperation, 0, totalOps)

	// Add trustline operations
	for accountAddress, assets := range trustlines {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildTrustlineKey(accountAddress),
			Members: assets,
		})
	}

	// Add contract operations
	for accountAddress, contractAddresses := range contracts {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildContractKey(accountAddress),
			Members: contractAddresses,
		})
	}

	// Execute operations in batches
	for i := 0; i < len(redisPipelineOps); i += redisPipelineBatchSize {
		end := min(i+redisPipelineBatchSize, len(redisPipelineOps))
		if err := s.redisStore.ExecutePipeline(ctx, redisPipelineOps[i:end]); err != nil {
			return fmt.Errorf("executing account tokens pipeline: %w", err)
		}
	}

	log.Ctx(ctx).Infof("Stored %d trustlines and %d contracts in Redis in %.2f minutes", len(trustlines), len(contracts), time.Since(startTime).Minutes())
	return nil
}

// getLatestCheckpointLedger retrieves the most recent checkpoint ledger sequence from the history archive.
// Returns the checkpoint ledger number that is on or before the latest available ledger.
func getLatestCheckpointLedger(archive historyarchive.ArchiveInterface) (uint32, error) {
	// Get latest ledger from archive
	latestLedger, err := archive.GetLatestLedgerSequence()
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger sequence: %w", err)
	}

	// Get checkpoint manager
	manager := archive.GetCheckpointManager()

	// Return the latest checkpoint (on or before latest ledger)
	if manager.IsCheckpoint(latestLedger) {
		return latestLedger, nil
	}
	return manager.PrevCheckpoint(latestLedger), nil
}

// extractHolderAddress extracts the account address from a contract balance entry key.
// Balance entries have a key that is a ScVec with 2 elements:
// - First element: ScSymbol("Balance")
// - Second element: ScAddress (the account/contract holder address)
// Returns the holder address as a Stellar-encoded string, or empty string if invalid.
func (s *accountTokenService) extractHolderAddress(key xdr.ScVal) (string, error) {
	// Verify the key is a vector
	keyVecPtr, ok := key.GetVec()
	if !ok || keyVecPtr == nil {
		return "", fmt.Errorf("key is not a vector")
	}
	keyVec := *keyVecPtr

	// Balance entries should have exactly 2 elements
	if len(keyVec) != 2 {
		return "", fmt.Errorf("key vector length is %d, expected 2", len(keyVec))
	}

	// First element should be the symbol "Balance"
	sym, ok := keyVec[0].GetSym()
	if !ok || sym != "Balance" {
		return "", fmt.Errorf("first element is not 'Balance' symbol")
	}

	// Second element is the ScAddress of the balance holder
	scAddress, ok := keyVec[1].GetAddress()
	if !ok {
		return "", fmt.Errorf("second element is not a valid address")
	}

	// Convert ScAddress to Stellar string format
	// This handles both account addresses (G...) and contract addresses (C...)
	holderAddress, err := scAddress.String()
	if err != nil {
		return "", fmt.Errorf("converting address to string: %w", err)
	}

	return holderAddress, nil
}

// fetchcontractMetadata fetches a single contract method (name, symbol, or decimals) via RPC simulation.
// Returns the XDR ScVal response from the contract function.
func (s *accountTokenService) fetchcontractMetadata(ctx context.Context, contractAddress, functionName string) (xdr.ScVal, error) {
	if err := ctx.Err(); err != nil {
		return xdr.ScVal{}, fmt.Errorf("context error: %w", err)
	}

	// Decode contract ID from string
	contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding contract address: %w", err)
	}
	contractID := xdr.ContractId(contractIDBytes)

	// Build invoke operation
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol(functionName),
				Args:         xdr.ScVec{}, // No arguments needed for metadata functions
			},
		},
	}

	// Build transaction with dummy source account (simulation doesn't need real account)
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: s.dummyAccount.Address(), Sequence: 0},
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("building transaction: %w", err)
	}

	// Encode transaction to XDR
	txXDR, err := tx.Base64()
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("encoding transaction: %w", err)
	}

	// Simulate transaction
	result, err := s.rpcService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("simulating transaction: %w", err)
	}

	if result.Error != "" {
		return xdr.ScVal{}, fmt.Errorf("simulation failed: %s", result.Error)
	}

	if len(result.Results) == 0 {
		return xdr.ScVal{}, fmt.Errorf("no simulation results returned")
	}

	return result.Results[0].XDR, nil
}

// fetchAllcontractMetadata fetches name, symbol, and decimals for a single contract in parallel using pond groups.
// Returns contractMetadata with the fetched values, or zero values for fields that failed to fetch.
// Errors are collected and logged but do not fail the entire operation.
func (s *accountTokenService) fetchAllcontractMetadata(ctx context.Context, contractAddress string) (contractMetadata, error) {
	group := s.pool.NewGroupContext(ctx)

	var (
		name     string
		symbol   string
		decimals uint32
		errs     []error
		mu       sync.Mutex
	)

	// appendError safely appends an error to the errors slice
	appendError := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	// Fetch name
	group.Submit(func() {
		nameVal, err := s.fetchcontractMetadata(ctx, contractAddress, "name")
		if err != nil {
			appendError(fmt.Errorf("fetching name: %w", err))
			return
		}
		nameStr, ok := nameVal.GetStr()
		if !ok {
			appendError(fmt.Errorf("name value is not a string"))
			return
		}
		mu.Lock()
		name = string(nameStr)
		mu.Unlock()
	})

	// Fetch symbol
	group.Submit(func() {
		symbolVal, err := s.fetchcontractMetadata(ctx, contractAddress, "symbol")
		if err != nil {
			appendError(fmt.Errorf("fetching symbol: %w", err))
			return
		}
		symbolStr, ok := symbolVal.GetStr()
		if !ok {
			appendError(fmt.Errorf("symbol value is not a string"))
			return
		}
		mu.Lock()
		symbol = string(symbolStr)
		mu.Unlock()
	})

	// Fetch decimals
	group.Submit(func() {
		decimalsVal, err := s.fetchcontractMetadata(ctx, contractAddress, "decimals")
		if err != nil {
			appendError(fmt.Errorf("fetching decimals: %w", err))
			return
		}
		decimalsU32, ok := decimalsVal.GetU32()
		if !ok {
			appendError(fmt.Errorf("decimals value is not a uint32"))
			return
		}
		mu.Lock()
		decimals = uint32(decimalsU32)
		mu.Unlock()
	})

	// Wait for all three fetches to complete
	if err := group.Wait(); err != nil {
		return contractMetadata{}, fmt.Errorf("waiting for metadata fetch group for contract %s: %w", contractAddress, err)
	}

	if len(errs) > 0 {
		return contractMetadata{}, fmt.Errorf("fetching contract metadata for contract %s: %w", contractAddress, errors.Join(errs...))
	}

	return contractMetadata{
		Name:     name,
		Symbol:   symbol,
		Decimals: decimals,
	}, nil
}

// fetchcontractMetadataBatch fetches metadata for multiple contracts in parallel using pond groups.
// Returns a map of contractID → contractMetadata for successfully fetched contracts.
// Processes contracts in batches to limit RPC load.
func (s *accountTokenService) fetchcontractMetadataBatch(ctx context.Context, metadata map[string]contractMetadata, contractIDs []string) map[string]contractMetadata {
	var mu sync.Mutex

	for i := 0; i < len(contractIDs); i += simulateTransactionBatchSize {
		end := min(i+simulateTransactionBatchSize, len(contractIDs))
		contractIDsBatch := contractIDs[i:end]

		group := s.pool.NewGroupContext(ctx)
		for _, contractID := range contractIDsBatch {
			group.Submit(func() {
				data, err := s.fetchAllcontractMetadata(ctx, contractID)
				if err != nil {
					log.Ctx(ctx).Warnf("Failed to fetch metadata for contract %s: %v", contractID, err)
					return
				}

				// Only store if we successfully fetched at least name and symbol
				if data.Name != "" && data.Symbol != "" {
					mu.Lock()
					existing := metadata[contractID]
					existing.Name = data.Name
					existing.Symbol = data.Symbol
					existing.Decimals = data.Decimals
					metadata[contractID] = existing
					mu.Unlock()
				}
			})
		}

		if err := group.Wait(); err != nil {
			log.Ctx(ctx).Warnf("Error waiting for batch metadata fetch: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	return metadata
}

// fetchAndStoreContractMetadata fetches metadata for all contracts and stores them in the database.
// This extracts non-SAC contract IDs, fetches their metadata via RPC, and stores in the contract_tokens table.
func (s *accountTokenService) fetchAndStoreContractMetadata(ctx context.Context, contractTypesByContractID map[string]types.ContractType) error {
	if len(contractTypesByContractID) == 0 {
		log.Ctx(ctx).Info("No contracts to fetch metadata for")
		return nil
	}

	metadataMap := make(map[string]contractMetadata)
	contractIDs := make([]string, 0)
	for contractID := range contractTypesByContractID {
		metadataMap[contractID] = contractMetadata{
			Type: contractTypesByContractID[contractID],
		}
		contractIDs = append(contractIDs, contractID)
	}

	// Fetch metadata in parallel
	start := time.Now()
	metadataMap = s.fetchcontractMetadataBatch(ctx, metadataMap, contractIDs)
	log.Ctx(ctx).Infof("Fetched metadata for %d contracts in %.2f minutes", len(metadataMap), time.Since(start).Minutes())

	// Parse the SAC code:issuer name and store them individually
	for contractID, contractType := range contractTypesByContractID {
		if contractType == types.ContractTypeSAC {
			metadata := metadataMap[contractID]
			name := metadata.Name
			if name == "" {
				continue
			}

			parts := strings.Split(name, ":")
			if len(parts) != 2 {
				continue
			}

			metadata.Code = parts[0]
			metadata.Issuer = parts[1]

			metadataMap[contractID] = metadata
		}
	}

	// Store in database
	start = time.Now()
	err := s.storecontractMetadataInDB(ctx, metadataMap)
	log.Ctx(ctx).Infof("Stored metadata for %d contracts in %.2f seconds", len(metadataMap), time.Since(start).Seconds())
	return err
}

// storecontractMetadataInDB stores contract metadata in the contract_tokens database table.
// Uses batch insert with ON CONFLICT DO NOTHING to handle existing contracts.
func (s *accountTokenService) storecontractMetadataInDB(ctx context.Context, metadataMap map[string]contractMetadata) error {
	if len(metadataMap) == 0 {
		log.Ctx(ctx).Info("No contract metadata to store in database")
		return nil
	}

	if s.contractModel == nil {
		return fmt.Errorf("contract model is nil - cannot store metadata")
	}

	// Build contracts slice from metadata map
	contracts := make([]*data.Contract, 0, len(metadataMap))
	for contractID, metadata := range metadataMap {
		// Convert Code and Issuer to pointers (nil if empty)
		var code, issuer *string
		if metadata.Code != "" {
			code = &metadata.Code
		}
		if metadata.Issuer != "" {
			issuer = &metadata.Issuer
		}

		contracts = append(contracts, &data.Contract{
			ID:       contractID,
			Type:     string(metadata.Type),
			Code:     code,
			Issuer:   issuer,
			Name:     metadata.Name,
			Symbol:   metadata.Symbol,
			Decimals: int16(metadata.Decimals),
		})
	}

	// Batch insert all contracts
	insertedIDs, err := s.contractModel.BatchInsert(ctx, nil, contracts)
	if err != nil {
		return fmt.Errorf("storing contract metadata in database: %w", err)
	}

	log.Ctx(ctx).Infof("Successfully stored metadata for %d/%d contracts in database", len(insertedIDs), len(metadataMap))
	return nil
}
