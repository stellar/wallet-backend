package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/sac"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	wbdata "github.com/stellar/wallet-backend/internal/data"
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
)

// checkpointData holds all data collected from processing a checkpoint ledger.
type checkpointData struct {
	// Trustlines maps account addresses (G...) to their trustline assets formatted as "CODE:ISSUER"
	TrustlinesByAccountAddress map[string]set.Set[wbdata.TrustlineAsset]
	// Contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	ContractsByHolderAddress map[string]set.Set[string]
	// UniqueTrustlines tracks all unique trustline assets
	UniqueTrustlines set.Set[wbdata.TrustlineAsset]
	// UniqueContractTokens tracks all unique contract tokens
	UniqueContractTokens set.Set[string]
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
	// history archive for a specific checkpoint. It extracts all trustlines and contract
	// tokens from checkpoint ledger entries and stores them in Redis.
	// The checkpointLedger parameter specifies which checkpoint to use for population.
	PopulateAccountTokens(ctx context.Context, checkpointLedger uint32) error

	// GetAccountTrustlines retrieves all classic trustline assets for an account.
	// Returns a slice of assets formatted as "CODE:ISSUER", or empty slice if none exist.
	GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error)

	// GetAccountContracts retrieves all contract token IDs for an account from Redis.
	GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error)

	// ProcessTokenChanges applies trustline and contract balance changes to Redis cache
	// using pipelining for performance. This is called by the indexer for each ledger's
	// state changes during live ingestion.
	ProcessTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
}

var _ AccountTokenService = (*accountTokenService)(nil)

type accountTokenService struct {
	checkpointLedger        uint32
	archive                 historyarchive.ArchiveInterface
	contractValidator       ContractValidator
	redisStore              *store.RedisStore
	contractMetadataService ContractMetadataService
	trustlineAssetModel     wbdata.TrustlineAssetModelInterface
	pool                    pond.Pool
	networkPassphrase       string
	trustlinesPrefix        string
	contractsPrefix         string
}

func NewAccountTokenService(
	networkPassphrase string,
	archive historyarchive.ArchiveInterface,
	redisStore *store.RedisStore,
	contractValidator ContractValidator,
	contractMetadataService ContractMetadataService,
	trustlineAssetModel wbdata.TrustlineAssetModelInterface,
	pool pond.Pool,
) (AccountTokenService, error) {
	// Note: archive can be nil for serve mode (only reads from Redis)
	return &accountTokenService{
		checkpointLedger:        0,
		archive:                 archive,
		contractValidator:       contractValidator,
		redisStore:              redisStore,
		contractMetadataService: contractMetadataService,
		trustlineAssetModel:     trustlineAssetModel,
		pool:                    pool,
		networkPassphrase:       networkPassphrase,
		trustlinesPrefix:        trustlinesKeyPrefix,
		contractsPrefix:         contractsKeyPrefix,
	}, nil
}

// PopulateAccountTokens performs initial Redis cache population from Stellar history archive.
// This reads the specified checkpoint ledger and extracts all trustlines and contract tokens that an account has.
// The checkpoint ledger is calculated and passed in by the caller (ingestService).
// Warning: This is a long-running operation that may take several minutes.
func (s *accountTokenService) PopulateAccountTokens(ctx context.Context, checkpointLedger uint32) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateAccountTokens requires archive connection")
	}

	defer func() {
		if err := s.contractValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	log.Ctx(ctx).Infof("Populating account token cache from checkpoint ledger = %d", checkpointLedger)
	s.checkpointLedger = checkpointLedger

	// Create checkpoint change reader
	reader, err := ingest.NewCheckpointChangeReader(ctx, s.archive, checkpointLedger)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing checkpoint reader: %v", closeErr)
		}
	}()

	// Collect account tokens from checkpoint
	cpData, err := s.collectAccountTokensFromCheckpoint(ctx, reader)
	if err != nil {
		return err
	}

	// Extract contract spec from WASM hash and validate SEP-41 contracts
	s.enrichContractTypes(ctx, cpData.ContractTypesByContractID, cpData.ContractIDsByWasmHash, cpData.ContractCodesByWasmHash)

	// Fetch metadata for contracts and store in database
	// if s.contractMetadataService != nil {
	// 	if err := s.contractMetadataService.FetchAndStoreMetadata(ctx, cpData.ContractTypesByContractID); err != nil {
	// 		log.Ctx(ctx).Warnf("Failed to fetch and store contract metadata: %v", err)
	// 		// Don't fail the entire process if metadata fetch fails
	// 	}
	// }

	return s.storeAccountTokensInRedis(ctx, cpData.TrustlinesByAccountAddress, cpData.ContractsByHolderAddress, cpData.UniqueTrustlines.ToSlice(), cpData.UniqueContractTokens.ToSlice())
}

// validateAccountAddress checks if the account address is valid (non-empty).
func validateAccountAddress(accountAddress string) error {
	if accountAddress == "" {
		return fmt.Errorf("account address cannot be empty")
	}
	return nil
}

// ProcessTokenChanges processes token changes efficiently using Redis pipelining.
// This reduces network round trips from N operations to 1, significantly improving performance
// during live ingestion. Called by the indexer for each ledger's state changes.
//
// For trustlines: handles both ADD (new trustline created) and REMOVE (trustline deleted).
// For contract token balances (SAC, SEP41): only ADD operations are processed (contract tokens are never explicitly removed).
// Internally stores short integer IDs to reduce memory usage.
func (s *accountTokenService) ProcessTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// Calculate capacity: each trustline = 1 op, each contract = 1 op
	trustlineOpsCount := len(trustlineChanges)
	contractOpsCount := len(contractChanges)
	operations := make([]store.RedisPipelineOperation, 0, trustlineOpsCount+contractOpsCount)

	// Convert trustline changes to Redis pipeline operations
	for _, change := range trustlineChanges {
		if change.Asset == "" {
			log.Ctx(ctx).Warnf("Skipping trustline change with empty asset for account %s", change.AccountID)
			continue
		}

		// Parse "CODE:ISSUER" format and get or create asset ID from PostgreSQL
		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			log.Ctx(ctx).Warnf("Skipping trustline change with invalid asset format %s: %v", change.Asset, err)
			continue
		}

		assetID, err := s.trustlineAssetModel.GetOrCreateID(ctx, code, issuer)
		if err != nil {
			return fmt.Errorf("getting asset ID for %s: %w", change.Asset, err)
		}

		key := s.buildTrustlineKey(change.AccountID)
		var op store.RedisOperation

		switch change.Operation {
		case types.TrustlineOpAdd:
			op = store.SetOpAdd
		case types.TrustlineOpRemove:
			op = store.SetOpRemove
		default:
			return fmt.Errorf("unsupported trustline operation: %s", change.Operation)
		}

		operations = append(operations, store.RedisPipelineOperation{
			Op:      op,
			Key:     key,
			Members: []string{strconv.FormatInt(assetID, 10)},
		})
	}

	// Convert contract changes to Redis pipeline operations
	for _, change := range contractChanges {
		if change.ContractID == "" {
			log.Ctx(ctx).Warnf("Skipping contract change with empty contract ID for account %s", change.AccountID)
			continue
		}

		// For contract changes, we always add contract IDs and never remove them.
		// This is because contract balance entries persist in the ledger even when balance is zero,
		// unlike trustlines which can be completely deleted. We track all contracts an account has
		// ever interacted with.
		operations = append(operations, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildContractKey(change.AccountID),
			Members: []string{change.ContractID},
		})
	}

	// Execute all operations in a single pipeline
	if err := s.redisStore.ExecutePipeline(ctx, operations); err != nil {
		return fmt.Errorf("executing token changes pipeline: %w", err)
	}

	return nil
}

// parseAssetString parses a "CODE:ISSUER" formatted asset string into its components.
func parseAssetString(asset string) (code, issuer string, err error) {
	parts := strings.SplitN(asset, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid asset format: expected CODE:ISSUER, got %s", asset)
	}
	return parts[0], parts[1], nil
}

// buildTrustlineKey constructs the Redis key for an account's trustlines set.
func (s *accountTokenService) buildTrustlineKey(accountAddress string) string {
	return s.trustlinesPrefix + accountAddress
}

// buildContractKey constructs the Redis key for an account's contracts set.
func (s *accountTokenService) buildContractKey(accountAddress string) string {
	return s.contractsPrefix + accountAddress
}

// GetAccountTrustlines retrieves all trustlines for an account from Redis.
// Returns asset strings in "CODE:ISSUER" format after resolving from internal IDs.
func (s *accountTokenService) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	if err := validateAccountAddress(accountAddress); err != nil {
		return nil, err
	}
	key := s.buildTrustlineKey(accountAddress)

	// Get IDs from SET
	idStrings, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	if len(idStrings) == 0 {
		return nil, nil
	}

	// Convert string IDs to int64
	ids := make([]int64, 0, len(idStrings))
	for _, idStr := range idStrings {
		id, parseErr := strconv.ParseInt(idStr, 10, 64)
		if parseErr != nil {
			log.Ctx(ctx).Warnf("Skipping invalid asset ID %s for account %s: %v", idStr, accountAddress, parseErr)
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Batch resolve IDs to asset objects from PostgreSQL
	assetRecords, err := s.trustlineAssetModel.BatchGetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("resolving asset IDs for account %s: %w", accountAddress, err)
	}

	// Convert to "CODE:ISSUER" format
	assets := make([]string, 0, len(assetRecords))
	for _, asset := range assetRecords {
		assets = append(assets, asset.AssetKey())
	}
	return assets, nil
}

// GetAccountContracts retrieves all contract token IDs for an account from Redis.
// For G-address: all non-SAC custom tokens because SAC tokens are already tracked in trustlines
// For C-address: all contract tokens (SAC, custom)
// Returns full contract addresses (C...).
func (s *accountTokenService) GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error) {
	if err := validateAccountAddress(accountAddress); err != nil {
		return nil, err
	}
	key := s.buildContractKey(accountAddress)

	contracts, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting contracts for account %s: %w", accountAddress, err)
	}
	if len(contracts) == 0 {
		return nil, nil
	}

	return contracts, nil
}

// GetCheckpointLedger returns the ledger sequence number of the checkpoint used for initial cache population.
func (s *accountTokenService) GetCheckpointLedger() uint32 {
	return s.checkpointLedger
}

// processTrustlineChange extracts trustline information from a ledger change entry.
// Returns the account address and asset string, with skip=true if the entry should be skipped.
func (s *accountTokenService) processTrustlineChange(change ingest.Change) (string, wbdata.TrustlineAsset, bool) {
	trustlineEntry := change.Post.Data.MustTrustLine()
	accountAddress := trustlineEntry.AccountId.Address()
	asset := trustlineEntry.Asset

	// Skip liquidity pool shares as they're tracked separately via pool-specific indexing
	// and don't represent traditional trustlines.
	if asset.Type == xdr.AssetTypeAssetTypePoolShare {
		return "", wbdata.TrustlineAsset{}, true
	}

	var assetType, assetCode, assetIssuer string
	if err := trustlineEntry.Asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return "", wbdata.TrustlineAsset{}, true
	}

	return accountAddress, wbdata.TrustlineAsset{
		Code:   assetCode,
		Issuer: assetIssuer,
	}, false
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
		TrustlinesByAccountAddress: make(map[string]set.Set[wbdata.TrustlineAsset]),
		ContractsByHolderAddress:   make(map[string]set.Set[string]),
		UniqueTrustlines:           set.NewSet[wbdata.TrustlineAsset](),
		UniqueContractTokens:       set.NewSet[string](),
		ContractTypesByContractID:  make(map[string]types.ContractType),
		ContractIDsByWasmHash:      make(map[xdr.Hash][]string),
		ContractCodesByWasmHash:    make(map[xdr.Hash][]byte),
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
			accountAddress, asset, skip := s.processTrustlineChange(change)
			if skip {
				continue
			}
			entries++

			if _, ok := data.TrustlinesByAccountAddress[accountAddress]; !ok {
				data.TrustlinesByAccountAddress[accountAddress] = set.NewSet[wbdata.TrustlineAsset]()
			}
			data.TrustlinesByAccountAddress[accountAddress].Add(asset)
			data.UniqueTrustlines.Add(asset)

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
				if _, ok := data.ContractsByHolderAddress[holderAddress]; !ok {
					data.ContractsByHolderAddress[holderAddress] = set.NewSet[string]()
				}
				data.ContractsByHolderAddress[holderAddress].Add(contractAddressStr)
				data.UniqueContractTokens.Add(contractAddressStr)
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
// Trustline assets are converted to short integer IDs (stored in PostgreSQL) to reduce memory usage.
// Contract addresses are stored directly as full strings.
func (s *accountTokenService) storeAccountTokensInRedis(
	ctx context.Context,
	trustlinesByAccountAddress map[string]set.Set[wbdata.TrustlineAsset],
	contractsByAccountAddress map[string]set.Set[string],
	uniqueTrustlines []wbdata.TrustlineAsset,
	_ []string, // uniqueContractTokens - no longer needed, contracts stored directly
) error {
	startTime := time.Now()

	// Batch-assign IDs to all unique trustline assets using PostgreSQL
	assetIDMap, err := s.trustlineAssetModel.BatchInsert(ctx, uniqueTrustlines)
	if err != nil {
		return fmt.Errorf("batch assigning asset IDs: %w", err)
	}
	log.Ctx(ctx).Infof("Assigned IDs to %d unique assets", len(assetIDMap))

	// Build pipeline operations
	totalOps := len(trustlinesByAccountAddress) + len(contractsByAccountAddress)
	redisPipelineOps := make([]store.RedisPipelineOperation, 0, totalOps)

	// Add trustline operations with asset IDs from PostgreSQL
	for accountAddress, assets := range trustlinesByAccountAddress {
		assetIDs := make([]string, 0, assets.Cardinality())
		for asset := range assets.Iter() {
			if id, ok := assetIDMap[asset.Code+":"+asset.Issuer]; ok {
				assetIDs = append(assetIDs, strconv.FormatInt(id, 10))
			}
		}
		if len(assetIDs) > 0 {
			redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
				Op:      store.SetOpAdd,
				Key:     s.buildTrustlineKey(accountAddress),
				Members: assetIDs,
			})
		}
	}

	// Add contract operations with full contract addresses
	for accountAddress, contractAddresses := range contractsByAccountAddress {
		contracts := make([]string, 0, contractAddresses.Cardinality())
		for contractAddr := range contractAddresses.Iter() {
			contracts = append(contracts, contractAddr)
		}
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildContractKey(accountAddress),
			Members: contracts,
		})
	}

	// Execute operations in batches
	for i := 0; i < len(redisPipelineOps); i += redisPipelineBatchSize {
		end := min(i+redisPipelineBatchSize, len(redisPipelineOps))
		if err := s.redisStore.ExecutePipeline(ctx, redisPipelineOps[i:end]); err != nil {
			return fmt.Errorf("executing account tokens pipeline: %w", err)
		}
	}

	log.Ctx(ctx).Infof("Stored %d account trustline sets and %d account contract sets in Redis in %.2f minutes", len(trustlinesByAccountAddress), len(contractsByAccountAddress), time.Since(startTime).Minutes())
	return nil
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
