package services

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/dgraph-io/ristretto/v2"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

const (
	// Redis key prefixes for account token storage
	trustlinesKeyPrefix = "trustlines:"
	contractsKeyPrefix  = "contracts:"
	ingestLedgerKey     = "ingested_ledger"

	// redisPipelineBatchSize is the number of operations to batch
	// in a single Redis pipeline for token cache population.
	redisPipelineBatchSize = 50000

	// numTrustlineBuckets distributes accounts across Redis hashes to avoid hot-key issues.
	// With ~10M mainnet accounts, 20,000 buckets yields ~500 accounts per bucket on average.
	// This provides good distribution while keeping the number of Redis keys manageable.
	numTrustlineBuckets = 20000

	// numTrustlineAssetsInFrequencyCache is the number of assets to keep in the frequency cache.
	numTrustlineAssetsInFrequencyCache = 100000
)

// checkpointData holds all data collected from processing a checkpoint ledger.
type checkpointData struct {
	// Trustlines maps account addresses (G...) to their trustline assets formatted as "CODE:ISSUER"
	TrustlinesByAccountAddress map[string][]string
	// Contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	ContractsByHolderAddress map[string][]string
	// UniqueContractTokens tracks all unique contract tokens
	UniqueContractTokens set.Set[string]
	// ContractTypesByContractID tracks the token type for each unique contract ID
	ContractTypesByContractID map[string]types.ContractType
	// ContractIDsByWasmHash groups contract IDs by their WASM hash for batch validation
	ContractIDsByWasmHash map[xdr.Hash][]string
	// ContractTypesByWasmHash maps WASM hashes to their contract code bytes
	ContractTypesByWasmHash map[xdr.Hash]types.ContractType
	// TrustlineFrequency tracks how many accounts hold each trustline asset for frequency-based ID assignment
	TrustlineFrequency map[wbdata.TrustlineAsset]int64
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
	GetAccountTrustlines(ctx context.Context, accountAddress string) ([]*wbdata.TrustlineAsset, error)

	// GetAccountContracts retrieves all contract token IDs for an account from Redis.
	GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error)

	// ProcessTokenChanges applies trustline and contract balance changes to Redis cache
	// using pipelining for performance. This is called by the indexer for each ledger's
	// state changes during live ingestion.
	//
	// Storage semantics differ between trustlines and contracts:
	// - Trustlines: Can be added or removed. When all trustlines for an account are removed,
	//   the account's entry is deleted from Redis.
	// - Contracts: Only additions are tracked (contracts accumulate). Contract balance entries
	//   persist in the ledger even when balance is zero, so we track all contracts an account
	//   has ever held a balance in.
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, ledgerSequence uint32, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error

	// InitializeTrustlineIDByAssetCache initializes the trustline ID by asset cache.
	InitializeTrustlineIDByAssetCache(ctx context.Context) error

	// InitializeTrustlineAssetByIDCache initializes the trustline asset by ID cache.
	InitializeTrustlineAssetByIDCache(ctx context.Context) error
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
	trustlineAssetByID      *ristretto.Cache[int64, string]
	trustlineIDByAsset      *ristretto.Cache[string, int64]
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
	s.enrichContractTypes(ctx, cpData.ContractTypesByContractID, cpData.ContractIDsByWasmHash, cpData.ContractTypesByWasmHash)

	// Fetch metadata for contracts and store in database
	if err := s.contractMetadataService.FetchAndStoreMetadata(ctx, cpData.ContractTypesByContractID); err != nil {
		log.Ctx(ctx).Warnf("Failed to fetch and store contract metadata: %v", err)
		// Don't fail the entire process if metadata fetch fails
	}

	return s.storeAccountTokensInRedis(ctx, cpData.TrustlinesByAccountAddress, cpData.ContractsByHolderAddress, cpData.TrustlineFrequency)
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
// Internally stores short integer IDs in varint binary format to reduce memory usage.
func (s *accountTokenService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, ledgerSequence uint32, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// Get the last ingested ledger sequence. If the last processed ledger sequence is greater than or equal to the current ledger sequence
	// then we can skip updating the cache since it is already ahead.
	lastIngestedLedgerSequence, err := s.redisStore.Get(ctx, ingestLedgerKey)
	if err != nil {
		return fmt.Errorf("getting last ingested ledger sequence: %w", err)
	}
	lastIngestedLedgerSequenceUint64, err := strconv.ParseUint(lastIngestedLedgerSequence, 10, 32)
	if err != nil {
		return fmt.Errorf("converting last ingested ledger sequence to uint32: %w", err)
	}
	lastIngestedLedgerSequenceUint32 := uint32(lastIngestedLedgerSequenceUint64)
	if ledgerSequence <= lastIngestedLedgerSequenceUint32 {
		return nil
	}

	// Calculate capacity: each trustline = 1 op, each contract = 1 op
	operations := make([]store.RedisPipelineOperation, 0)

	// Build unique prefix buckets and group changes by account
	addressesByBucketPrefix := make(map[string][]string)
	trustlineChangesByAccount := make(map[string][]types.TrustlineChange)
	accountToPrefix := make(map[string]string) // Map account -> its prefix bucket
	uniqueTrustlineAssets := set.NewSet[wbdata.TrustlineAsset]()

	for _, change := range trustlineChanges {
		prefix := s.buildTrustlineKey(change.AccountID)
		addressesByBucketPrefix[prefix] = append(addressesByBucketPrefix[prefix], change.AccountID)
		trustlineChangesByAccount[change.AccountID] = append(trustlineChangesByAccount[change.AccountID], change)
		accountToPrefix[change.AccountID] = prefix

		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			log.Ctx(ctx).Errorf("parsing asset string from trustline change: %v", err)
			continue
		}
		uniqueTrustlineAssets.Add(wbdata.TrustlineAsset{
			Code:   code,
			Issuer: issuer,
		})
	}

	// Get or create trustline asset IDs
	assetsToID, err := s.getAssetIDs(ctx, uniqueTrustlineAssets)
	if err != nil {
		return fmt.Errorf("getting or creating trustline asset IDs: %w", err)
	}

	// Process each prefix bucket - decode varint binary format to int64 sets
	trustlinesSetByAccount := make(map[string]set.Set[int64])
	for prefix, accounts := range addressesByBucketPrefix {
		existingTrustlines, err := s.redisStore.HMGet(ctx, prefix, accounts...)
		if err != nil {
			return fmt.Errorf("getting key %s: %w", prefix, err)
		}

		// Parse existing trustlines (varint format) into sets
		for accountAddress, trustlineData := range existingTrustlines {
			if trustlineData != "" {
				ids := decodeAssetIDs([]byte(trustlineData))
				trustlinesSetByAccount[accountAddress] = set.NewSet(ids...)
			}
		}
	}

	// Process trustline changes (sorted by operation ID)
	for accountAddress, changes := range trustlineChangesByAccount {
		// Sort by operation ID to maintain order
		sort.Slice(changes, func(i, j int) bool {
			return changes[i].OperationID < changes[j].OperationID
		})

		// Initialize set if account doesn't exist yet
		if _, exists := trustlinesSetByAccount[accountAddress]; !exists {
			trustlinesSetByAccount[accountAddress] = set.NewSet[int64]()
		}

		for _, change := range changes {
			code, issuer, err := parseAssetString(change.Asset)
			if err != nil {
				log.Ctx(ctx).Warnf("Skipping trustline change with invalid asset format %s: %v", change.Asset, err)
				continue
			}

			assetID, exists := assetsToID[code+":"+issuer]
			if !exists {
				return fmt.Errorf("getting asset ID for %s: %w", change.Asset, err)
			}

			switch change.Operation {
			case types.TrustlineOpAdd:
				trustlinesSetByAccount[accountAddress].Add(assetID)
			case types.TrustlineOpRemove:
				trustlinesSetByAccount[accountAddress].Remove(assetID)
			}
		}
	}

	// Build operations - only for accounts that had changes
	for accountAddress := range trustlineChangesByAccount {
		prefix := accountToPrefix[accountAddress]
		trustlineSet := trustlinesSetByAccount[accountAddress]

		if trustlineSet.Cardinality() == 0 {
			// Delete field if no trustlines remain
			operations = append(operations, store.RedisPipelineOperation{
				Op:    store.OpHDel,
				Key:   prefix,
				Field: accountAddress,
			})
		} else {
			// Encode asset IDs to varint binary format
			operations = append(operations, store.RedisPipelineOperation{
				Op:    store.OpHSet,
				Key:   prefix,
				Field: accountAddress,
				Value: string(encodeAssetIDs(trustlineSet.ToSlice())),
			})
		}
	}

	// Convert contract changes to Redis pipeline operations.
	// Contracts use simple Redis sets with raw contract IDs (C...) rather than the
	// hash-bucketed varint encoding used for trustlines. This is because:
	// 1. Contract addresses are already fixed-length (56 chars vs variable trustline strings)
	// 2. Accounts typically hold fewer contract balances than trustlines
	// 3. The memory savings from ID encoding would be minimal for fixed-length C addresses
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

	// Set the ingested ledger
	operations = append(operations, store.RedisPipelineOperation{
		Op:    store.OpSet,
		Key:   ingestLedgerKey,
		Value: fmt.Sprintf("%d", ledgerSequence),
	})

	// Execute all operations in a single pipeline
	if err := s.redisStore.ExecutePipeline(ctx, operations); err != nil {
		return fmt.Errorf("executing token changes pipeline: %w", err)
	}

	return nil
}

// InitializeTrustlineIDByAssetCache initializes the trustline ID by asset cache. This is used for encoding asset strings to IDs during live ingestion.
func (s *accountTokenService) InitializeTrustlineIDByAssetCache(ctx context.Context) error {
	trustlineIDByAssetCache, err := ristretto.NewCache(&ristretto.Config[string, int64]{
		NumCounters: 1e7,
		MaxCost:     numTrustlineAssetsInFrequencyCache, // this is the maximum size of our cache
		BufferItems: 64,
	})
	if err != nil {
		return fmt.Errorf("initializing trustline ID by asset cache: %w", err)
	}

	topNAssets, err := s.trustlineAssetModel.GetTopN(ctx, numTrustlineAssetsInFrequencyCache)
	if err != nil {
		return fmt.Errorf("getting top N assets: %w", err)
	}
	for _, asset := range topNAssets {
		key := asset.Code + ":" + asset.Issuer
		trustlineIDByAssetCache.Set(key, asset.ID, 1)
	}

	s.trustlineIDByAsset = trustlineIDByAssetCache

	return nil
}

// InitializeTrustlineAssetByIDCache initializes the trustline asset by ID cache. This is used for decoding IDs to asset strings during API requests
// to get an account's trustline assets.
func (s *accountTokenService) InitializeTrustlineAssetByIDCache(ctx context.Context) error {
	trustlineAssetByIDCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{
		NumCounters: 1e7,
		MaxCost:     numTrustlineAssetsInFrequencyCache, // this is the maximum size of our cache
		BufferItems: 64,
	})
	if err != nil {
		return fmt.Errorf("initializing trustline asset by ID cache: %w", err)
	}

	topNAssets, err := s.trustlineAssetModel.GetTopN(ctx, numTrustlineAssetsInFrequencyCache)
	if err != nil {
		return fmt.Errorf("getting top N assets: %w", err)
	}
	for _, asset := range topNAssets {
		key := asset.Code + ":" + asset.Issuer
		trustlineAssetByIDCache.Set(asset.ID, key, 1)
	}
	s.trustlineAssetByID = trustlineAssetByIDCache
	return nil
}

// getAssetIDs returns the asset IDs for a given asset string.
func (s *accountTokenService) getAssetIDs(ctx context.Context, assets set.Set[wbdata.TrustlineAsset]) (map[string]int64, error) {
	assetIDMap := make(map[string]int64)
	cacheMisses := make([]wbdata.TrustlineAsset, 0)
	for asset := range assets.Iter() {
		key := asset.Code + ":" + asset.Issuer
		assetID, ok := s.trustlineIDByAsset.Get(key)
		if !ok {
			cacheMisses = append(cacheMisses, asset)
			continue
		}
		assetIDMap[key] = assetID
	}

	if len(cacheMisses) == 0 {
		return assetIDMap, nil
	}

	idMap, err := s.trustlineAssetModel.BatchGetOrInsert(ctx, cacheMisses)
	if err != nil {
		return nil, fmt.Errorf("getting asset IDs: %w", err)
	}
	for asset, assetID := range idMap {
		s.trustlineIDByAsset.Set(asset, assetID, 1)
		assetIDMap[asset] = assetID
	}
	return assetIDMap, nil
}

// parseAssetString parses a "CODE:ISSUER" formatted asset string into its components.
func parseAssetString(asset string) (code, issuer string, err error) {
	parts := strings.SplitN(asset, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid asset format: expected CODE:ISSUER, got %s", asset)
	}
	return parts[0], parts[1], nil
}

// encodeAssetIDs encodes a slice of asset IDs to varint binary format.
//
// Varint encoding uses 7 bits per byte for data + 1 continuation bit, allowing
// small values to use fewer bytes (e.g., IDs < 128 use 1 byte, < 16384 use 2 bytes).
// This saves 40-60% memory vs string encoding for typical asset ID distributions.
//
// Pre-allocates max possible size (10 bytes per int64) then returns only used portion.
func encodeAssetIDs(ids []int64) []byte {
	if len(ids) == 0 {
		return nil
	}
	buf := make([]byte, len(ids)*binary.MaxVarintLen64)
	offset := 0
	for _, id := range ids {
		n := binary.PutUvarint(buf[offset:], uint64(id))
		offset += n
	}
	return buf[:offset]
}

// decodeAssetIDs decodes varint binary format to slice of asset IDs.
//
// Reads consecutive varint-encoded integers from the buffer until exhausted.
// Each varint uses 7 data bits + 1 continuation bit per byte, so small IDs
// (< 128) use 1 byte, medium IDs (< 16384) use 2 bytes, etc.
//
// Logs a warning and returns partial results if buffer contains corrupted data.
func decodeAssetIDs(buf []byte) []int64 {
	if len(buf) == 0 {
		return nil
	}
	result := make([]int64, 0)
	for len(buf) > 0 {
		val, n := binary.Uvarint(buf)
		if n <= 0 {
			log.Warnf("Varint decode stopped early: %d bytes remaining, %d IDs decoded", len(buf), len(result))
			break
		}
		result = append(result, int64(val))
		buf = buf[n:]
	}
	return result
}

// buildTrustlineKey constructs the Redis key for an account's trustlines set.
//
// Uses FNV-1a hash to distribute accounts across buckets, avoiding Redis hot keys.
// FNV-1a processes each byte: hash = (hash XOR byte) * prime, producing uniform
// distribution across the 20,000 buckets (~500 accounts per bucket on mainnet).
func (s *accountTokenService) buildTrustlineKey(accountAddress string) string {
	h := fnv.New32a()
	h.Write([]byte(accountAddress))
	bucket := h.Sum32() % numTrustlineBuckets
	return fmt.Sprintf("%s%d", s.trustlinesPrefix, bucket)
}

// buildContractKey constructs the Redis key for an account's contracts set.
func (s *accountTokenService) buildContractKey(accountAddress string) string {
	return s.contractsPrefix + accountAddress
}

// GetAccountTrustlines retrieves all trustlines for an account from Redis.
// Returns asset strings in "CODE:ISSUER" format after resolving from internal IDs.
func (s *accountTokenService) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]*wbdata.TrustlineAsset, error) {
	if err := validateAccountAddress(accountAddress); err != nil {
		return nil, err
	}
	key := s.buildTrustlineKey(accountAddress)

	// Get varint-encoded IDs from Redis hash
	idData, err := s.redisStore.HGet(ctx, key, accountAddress)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	if idData == "" {
		return nil, nil
	}

	// Decode varint binary format to asset IDs
	ids := decodeAssetIDs([]byte(idData))
	if len(ids) == 0 {
		return nil, nil
	}

	// Get the trustline asset code and issuer from the IDs
	assets, err := s.getAssetsFromIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("getting assets from IDs for account %s: %w", accountAddress, err)
	}

	return assets, nil
}

// getAssetsFromIDs gets the trustline asset code and issuer from the IDs
func (s *accountTokenService) getAssetsFromIDs(ctx context.Context, ids []int64) ([]*wbdata.TrustlineAsset, error) {
	result := make([]*wbdata.TrustlineAsset, 0)
	cacheMisses := make([]int64, 0)
	for _, id := range ids {
		if asset, ok := s.trustlineAssetByID.Get(id); ok {
			code, issuer, err := parseAssetString(asset)
			if err != nil {
				log.Ctx(ctx).Warnf("parsing asset string: %s: %v", asset, err)
				continue
			}
			result = append(result, &wbdata.TrustlineAsset{
				ID:     id,
				Code:   code,
				Issuer: issuer,
			})
		} else {
			cacheMisses = append(cacheMisses, id)
		}
	}

	if len(cacheMisses) == 0 {
		return result, nil
	}

	// Batch resolve IDs to asset objects from PostgreSQL
	assetRecords, err := s.trustlineAssetModel.BatchGetByIDs(ctx, cacheMisses)
	if err != nil {
		return nil, fmt.Errorf("resolving asset IDs: %w", err)
	}

	for _, asset := range assetRecords {
		s.trustlineAssetByID.Set(asset.ID, asset.Code+":"+asset.Issuer, 1)
		result = append(result, &wbdata.TrustlineAsset{
			ID:     asset.ID,
			Code:   asset.Code,
			Issuer: asset.Issuer,
		})
	}

	return result, nil
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
		TrustlinesByAccountAddress: make(map[string][]string),
		ContractsByHolderAddress:   make(map[string][]string),
		UniqueContractTokens:       set.NewSet[string](),
		ContractTypesByContractID:  make(map[string]types.ContractType),
		ContractIDsByWasmHash:      make(map[xdr.Hash][]string),
		ContractTypesByWasmHash:    make(map[xdr.Hash]types.ContractType),
		TrustlineFrequency:         make(map[wbdata.TrustlineAsset]int64),
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
				data.TrustlinesByAccountAddress[accountAddress] = []string{}
			}
			data.TrustlinesByAccountAddress[accountAddress] = append(data.TrustlinesByAccountAddress[accountAddress], asset.Code+":"+asset.Issuer)
			data.TrustlineFrequency[asset]++

		case xdr.LedgerEntryTypeContractCode:
			contractCodeEntry := change.Post.Data.MustContractCode()
			contractType, err := s.contractValidator.ValidateFromContractCode(ctx, contractCodeEntry.Code)
			if err != nil {
				continue
			}
			data.ContractTypesByWasmHash[contractCodeEntry.Hash] = contractType
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
					data.ContractsByHolderAddress[holderAddress] = []string{}
				}
				data.ContractsByHolderAddress[holderAddress] = append(data.ContractsByHolderAddress[holderAddress], contractAddressStr)
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
	_ context.Context,
	contractTypesByContractID map[string]types.ContractType,
	contractIDsByWasmHash map[xdr.Hash][]string,
	contractTypesByWasmHash map[xdr.Hash]types.ContractType,
) {
	for wasmHash, contractType := range contractTypesByWasmHash {
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
// Assets are sorted by frequency before insertion so the most common assets get the lowest IDs,
// which results in smaller varint encoding and reduced Redis memory usage.
// Contract addresses are stored directly as full strings.
func (s *accountTokenService) storeAccountTokensInRedis(
	ctx context.Context,
	trustlinesByAccountAddress map[string][]string,
	contractsByAccountAddress map[string][]string,
	trustlineFrequency map[wbdata.TrustlineAsset]int64,
) error {
	startTime := time.Now()

	// Batch-assign IDs to all unique trustline assets using PostgreSQL
	uniqueTrustlines := s.processTrustlineAssets(trustlineFrequency)
	assetIDMap, err := s.trustlineAssetModel.BatchGetOrInsert(ctx, uniqueTrustlines)
	if err != nil {
		return fmt.Errorf("batch assigning asset IDs: %w", err)
	}
	log.Ctx(ctx).Infof("Inserted %d unique trustline assets (sorted by frequency)", len(assetIDMap))

	// Add top 100k assets to frequency cache for quick asset -> ID lookups during ingestion
	for i, asset := range uniqueTrustlines {
		if i > numTrustlineAssetsInFrequencyCache {
			break
		}
		key := asset.Code + ":" + asset.Issuer
		s.trustlineIDByAsset.Set(key, assetIDMap[key], trustlineFrequency[asset])
	}

	// Build pipeline operations
	totalOps := len(trustlinesByAccountAddress) + len(contractsByAccountAddress)
	redisPipelineOps := make([]store.RedisPipelineOperation, 0, totalOps)

	// Add trustline operations with asset IDs from PostgreSQL, encoded as varint
	for accountAddress, assets := range trustlinesByAccountAddress {
		ids := make([]int64, 0, len(assets))
		for _, asset := range assets {
			if id, ok := assetIDMap[asset]; ok {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
				Op:    store.OpHSet,
				Key:   s.buildTrustlineKey(accountAddress),
				Field: accountAddress,
				Value: string(encodeAssetIDs(ids)),
			})
		}
	}

	// Add contract operations with full contract addresses
	for accountAddress, contractAddresses := range contractsByAccountAddress {
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

	log.Ctx(ctx).Infof("Stored %d account trustline sets and %d account contract sets in Redis in %.2f minutes", len(trustlinesByAccountAddress), len(contractsByAccountAddress), time.Since(startTime).Minutes())
	return nil
}

// processTrustlineAssets sorts assets by frequency (descending) for optimal varint encoding.
// Most frequent assets get lowest IDs (1, 2, 3...) which use fewer bytes in varint format.
func (s *accountTokenService) processTrustlineAssets(trustlineFrequency map[wbdata.TrustlineAsset]int64) []wbdata.TrustlineAsset {
	type assetFreq struct {
		asset wbdata.TrustlineAsset
		count int64
	}
	sortedAssets := make([]assetFreq, 0, len(trustlineFrequency))
	for asset, count := range trustlineFrequency {
		sortedAssets = append(sortedAssets, assetFreq{asset, count})
	}
	sort.Slice(sortedAssets, func(i, j int) bool {
		return sortedAssets[i].count > sortedAssets[j].count
	})

	// Extract sorted asset slice for batch insert
	uniqueTrustlines := make([]wbdata.TrustlineAsset, len(sortedAssets))
	for i, af := range sortedAssets {
		uniqueTrustlines[i] = af.asset
	}
	return uniqueTrustlines
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
