package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/sac"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

const (
	// Redis key prefixes for account token storage
	trustlinesKeyPrefix = "trustlines:"
	contractsKeyPrefix  = "contracts:"
	contractTypePrefix  = "contract_type:"

	// redisPipelineBatchSize is the number of operations to batch
	// in a single Redis pipeline for token cache population.
	redisPipelineBatchSize = 50000
)

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

	// GetContractType determines the token type (SAC, SEP41, or UNKNOWN) for a contract.
	// Returns ContractTypeUnknown if the contract ID is not found in the cache.
	GetContractType(ctx context.Context, contractID string) (types.ContractType, error)
}

var _ AccountTokenService = (*accountTokenService)(nil)

type accountTokenService struct {
	checkpointLedger   uint32
	archive            historyarchive.ArchiveInterface
	contractValidator  ContractValidator
	redisStore         *store.RedisStore
	networkPassphrase  string
	trustlinesPrefix   string
	contractsPrefix    string
	contractTypePrefix string
}

func NewAccountTokenService(networkPassphrase string, archiveURL string, redisStore *store.RedisStore, contractValidator ContractValidator, checkpointFrequency uint32) (AccountTokenService, error) {
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
		checkpointLedger:   0,
		archive:            archive,
		contractValidator:  contractValidator,
		redisStore:         redisStore,
		networkPassphrase:  networkPassphrase,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
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
	trustlines, contracts, contractTypesByContractID, contractIDsByWasmHash, contractCodesByWasmHash, err := s.collectAccountTokensFromCheckpoint(ctx, reader)
	if err != nil {
		return err
	}

	// Extract contract spec from WASM hash and validate SEP-41 contracts
	s.enrichContractTypes(ctx, contractTypesByContractID, contractIDsByWasmHash, contractCodesByWasmHash)
	return s.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypesByContractID)
}

// buildTrustlineKey constructs the Redis key for an account's trustlines set.
func (s *accountTokenService) buildTrustlineKey(accountAddress string) string {
	return s.trustlinesPrefix + accountAddress
}

// buildContractKey constructs the Redis key for an account's contracts set.
func (s *accountTokenService) buildContractKey(accountAddress string) string {
	return s.contractsPrefix + accountAddress
}

// buildContractTypeKey constructs the Redis key for a contract's type.
func (s *accountTokenService) buildContractTypeKey(contractID string) string {
	return s.contractTypePrefix + contractID
}

// AddTrustlines adds trustline assets to an account's Redis set.
// Assets are formatted as "CODE:ISSUER".
// Returns nil if assets is empty (no-op).
func (s *accountTokenService) AddTrustlines(ctx context.Context, accountAddress string, assets []string) error {
	if accountAddress == "" {
		return fmt.Errorf("account address cannot be empty")
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
	if accountAddress == "" {
		return fmt.Errorf("account address cannot be empty")
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
	if accountAddress == "" {
		return nil, fmt.Errorf("account address cannot be empty")
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
	if accountAddress == "" {
		return nil, fmt.Errorf("account address cannot be empty")
	}
	key := s.buildContractKey(accountAddress)
	contracts, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting contracts for account %s: %w", accountAddress, err)
	}
	return contracts, nil
}

// GetContractType retrieves the token type (SAC, SEP41, or UNKNOWN) for a given contract ID from Redis.
func (s *accountTokenService) GetContractType(ctx context.Context, contractID string) (types.ContractType, error) {
	if contractID == "" {
		return types.ContractTypeUnknown, fmt.Errorf("contract ID cannot be empty")
	}
	key := s.buildContractTypeKey(contractID)
	tokenType, err := s.redisStore.Get(ctx, key)
	if err != nil {
		return types.ContractTypeUnknown, fmt.Errorf("getting contract type for %s: %w", contractID, err)
	}
	if tokenType == "" {
		return types.ContractTypeUnknown, nil
	}
	return types.ContractType(tokenType), nil
}

// GetCheckpointLedger returns the ledger sequence number of the checkpoint used for initial cache population.
func (s *accountTokenService) GetCheckpointLedger() uint32 {
	return s.checkpointLedger
}

// processTrustlineChange extracts trustline information from a ledger change entry.
// Returns the account address and asset string, with skip=true if the entry should be skipped.
func (s *accountTokenService) processTrustlineChange(ctx context.Context, change ingest.Change) (accountAddress string, assetStr string, skip bool) {
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
		log.Ctx(ctx).Warnf("Failed to extract asset from trustline for account %s: %v", accountAddress, err)
		return "", "", true
	}

	assetStr = fmt.Sprintf("%s:%s", assetCode, assetIssuer)
	return accountAddress, assetStr, false
}

// processContractBalanceChange extracts contract balance information from a contract data entry.
// Returns the holder address and contract ID, with skip=true if extraction fails.
func (s *accountTokenService) processContractBalanceChange(ctx context.Context, contractDataEntry xdr.ContractDataEntry) (holderAddress string, skip bool) {
	// Extract the account/contract address from the contract data entry key.
	// We parse using the [Balance, holder_address] format that is followed by SEP-41 tokens.
	// However, this could also be valid for any non-SEP41 contract that mimics the same format.
	var err error
	holderAddress, err = s.extractHolderAddress(contractDataEntry.Key)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to extract holder address from contract data: %v", err)
		return "", true
	}

	// Extract the contract ID from the contract data entry

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
// Returns maps of trustlines, contracts, contract types, and contract IDs grouped by WASM hash.
func (s *accountTokenService) collectAccountTokensFromCheckpoint(
	ctx context.Context,
	reader ingest.ChangeReader,
) (
	trustlines map[string][]string,
	contracts map[string][]string,
	contractTypesByContractID map[string]types.ContractType,
	contractIDsByWasmHash map[xdr.Hash][]string,
	contractCodesByWasmHash map[xdr.Hash][]byte,
	err error,
) {
	// trustlines maps account addresses (G...) to their trustline assets formatted as "CODE:ISSUER"
	trustlines = make(map[string][]string)
	// contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	contracts = make(map[string][]string)
	// contractTypes tracks the token type for each unique contract ID
	contractTypesByContractID = make(map[string]types.ContractType)
	contractIDsByWasmHash = make(map[xdr.Hash][]string)
	contractCodesByWasmHash = make(map[xdr.Hash][]byte)

	entries := 0
	startTime := time.Now()

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, nil, nil, nil, nil, fmt.Errorf("checkpoint processing cancelled: %w", ctx.Err())
		default:
		}

		change, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("reading checkpoint changes: %w", err)
		}

		//exhaustive:ignore
		switch change.Type {
		case xdr.LedgerEntryTypeTrustline:
			accountAddress, assetStr, skip := s.processTrustlineChange(ctx, change)
			if skip {
				continue
			}
			entries++
			trustlines[accountAddress] = append(trustlines[accountAddress], assetStr)

		case xdr.LedgerEntryTypeContractCode:
			contractCodeEntry := change.Post.Data.MustContractCode()
			contractCodesByWasmHash[contractCodeEntry.Hash] = contractCodeEntry.Code
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
				holderAddress, skip := s.processContractBalanceChange(ctx, contractDataEntry)
				if skip {
					continue
				}
				contracts[holderAddress] = append(contracts[holderAddress], contractAddressStr)
				entries++

			case xdr.ScValTypeScvLedgerKeyContractInstance:
				wasmHash, skip := s.processContractInstanceChange(change, contractAddressStr, contractDataEntry, contractTypesByContractID)
				if skip {
					continue
				}
				// For non-SAC contracts with WASM hash, track for later validation
				contractIDsByWasmHash[*wasmHash] = append(contractIDsByWasmHash[*wasmHash], contractAddressStr)
				entries++
			}
		}
	}

	log.Ctx(ctx).Infof("Processed %d checkpoint entries in %.2f minutes", entries, time.Since(startTime).Minutes())
	return trustlines, contracts, contractTypesByContractID, contractIDsByWasmHash, contractCodesByWasmHash, nil
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
			contractType = types.ContractTypeUnknown
		}

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
	contractTypesByContractID map[string]types.ContractType,
) error {
	startTime := time.Now()

	// Calculate total operations: trustlines + contracts + contract types
	totalOps := len(trustlines) + len(contracts) + len(contractTypesByContractID)
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

	// Add contract type operations
	for contractID, contractType := range contractTypesByContractID {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:    store.OpSet,
			Key:   s.buildContractTypeKey(contractID),
			Value: string(contractType),
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

// extractContractID extracts the contract ID from a ContractData entry and returns it
// as a Stellar-encoded contract address (C...).
func (s *accountTokenService) extractContractID(contractData xdr.ContractDataEntry) (string, error) {
	// Check if the Contract field is of type CONTRACT (not ACCOUNT)
	if contractData.Contract.Type != xdr.ScAddressTypeScAddressTypeContract {
		return "", fmt.Errorf("contract address type is %v, expected ScAddressTypeContract", contractData.Contract.Type)
	}

	if contractData.Contract.ContractId == nil {
		return "", fmt.Errorf("contract ID is nil")
	}

	contractID := *contractData.Contract.ContractId
	// Encode as a Stellar contract address (C...)
	contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract ID: %w", err)
	}

	return contractAddress, nil
}
