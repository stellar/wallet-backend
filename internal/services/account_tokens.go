// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// TrustlineBatchSize is the number of trustline entries to buffer before flushing to DB.
	TrustlineBatchSize = 100_000
)

// checkpointData holds all data collected from processing a checkpoint ledger.
// Note: Trustlines are streamed directly to DB in batches, not stored here.
type checkpointData struct {
	// Contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	ContractsByHolderAddress map[string][]string
	// ContractTypesByContractID tracks the token type for each unique contract ID
	ContractTypesByContractID map[string]types.ContractType
	// ContractIDsByWasmHash groups contract IDs by their WASM hash for batch validation
	ContractIDsByWasmHash map[xdr.Hash][]string
	// ContractTypesByWasmHash maps WASM hashes to their contract code bytes
	ContractTypesByWasmHash map[xdr.Hash]types.ContractType
}

// trustlineBatch holds a batch of trustlines for streaming insertion.
type trustlineBatch struct {
	// accountTrustlines maps account address to list of computed asset IDs
	accountTrustlines map[string][]int64
	// uniqueAssets tracks unique assets with their computed IDs for batch insert
	uniqueAssets map[string]wbdata.TrustlineAsset
	// count tracks total trustline entries in this batch
	count int
}

func newTrustlineBatch() *trustlineBatch {
	return &trustlineBatch{
		accountTrustlines: make(map[string][]int64),
		uniqueAssets:      make(map[string]wbdata.TrustlineAsset),
	}
}

func (b *trustlineBatch) add(accountAddress string, asset wbdata.TrustlineAsset) {
	key := asset.Code + ":" + asset.Issuer
	assetID := wbdata.DeterministicAssetID(asset.Code, asset.Issuer)

	// Track unique asset
	if _, exists := b.uniqueAssets[key]; !exists {
		b.uniqueAssets[key] = wbdata.TrustlineAsset{
			ID:     assetID,
			Code:   asset.Code,
			Issuer: asset.Issuer,
		}
	}

	// Add to account's trustlines
	b.accountTrustlines[accountAddress] = append(b.accountTrustlines[accountAddress], assetID)
	b.count++
}

func (b *trustlineBatch) reset() {
	b.accountTrustlines = make(map[string][]int64)
	b.uniqueAssets = make(map[string]wbdata.TrustlineAsset)
	b.count = 0
}

// TokenCacheReader provides read-only access to cached account tokens.
// Used by the API server to query token holdings.
type TokenCacheReader interface {
	// GetAccountTrustlines retrieves all classic trustline assets for an account.
	// Returns a slice of assets formatted as "CODE:ISSUER", or empty slice if none exist.
	GetAccountTrustlines(ctx context.Context, accountAddress string) ([]*wbdata.TrustlineAsset, error)

	// GetAccountContracts retrieves all contract token IDs for an account from PostgreSQL.
	GetAccountContracts(ctx context.Context, accountAddress string) ([]*wbdata.Contract, error)
}

// TokenCacheWriter provides write access to the token cache during ingestion.
type TokenCacheWriter interface {
	// PopulateAccountTokens performs initial PostgreSQL cache population from Stellar
	// history archive for a specific checkpoint. It extracts all trustlines and contract
	// tokens from checkpoint ledger entries and stores them in PostgreSQL.
	// The checkpointLedger parameter specifies which checkpoint to use for population.
	// The initializeCursors callback is invoked within the same DB transaction as
	// the metadata storage to ensure atomic initialization.
	PopulateAccountTokens(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error

	// EnsureTrustlineAssetsExist inserts trustline assets into PostgreSQL with deterministic IDs.
	// Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
	// Must be called before ProcessTokenChanges to satisfy FK constraints.
	// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
	EnsureTrustlineAssetsExist(ctx context.Context, dbTx pgx.Tx, trustlineChanges []types.TrustlineChange) error

	// GetOrInsertContractTokens gets IDs for all SAC/SEP-41 contracts in changes.
	// For new contracts (not already in DB), fetches metadata via RPC.
	// Uses BatchGetOrInsert to handle both new and existing contracts.
	// Returns a map of contractID (C...) -> numeric database ID.
	GetOrInsertContractTokens(ctx context.Context, dbTx pgx.Tx, contractChanges []types.ContractChange) (map[string]int64, error)

	// ProcessTokenChanges applies trustline and contract balance changes to PostgreSQL.
	// This is called by the indexer for each ledger's state changes during live ingestion.
	//
	// Storage semantics differ between trustlines and contracts:
	// - Trustlines: Can be added or removed. When all trustlines for an account are removed,
	//   the account's entry is deleted from PostgreSQL.
	// - Contracts: Only SAC/SEP-41 contracts are tracked (contracts accumulate). Unknown contracts
	//   are skipped. Contract balance entries persist in the ledger even when balance is zero,
	//   so we track all contracts an account has ever held a balance in.
	//
	// Trustline asset IDs are computed using DeterministicAssetID (no map needed).
	// The contractIDMap parameter must be pre-populated by calling GetOrInsertContractTokens first.
	// Contracts not in contractIDMap (e.g., unknown contracts) are silently skipped.
	// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, contractIDMap map[string]int64, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
}

// Verify interface compliance at compile time
var (
	_ TokenCacheReader = (*tokenCacheService)(nil)
	_ TokenCacheWriter = (*tokenCacheService)(nil)
)

// tokenCacheService implements both TokenCacheReader and TokenCacheWriter.
type tokenCacheService struct {
	db                      db.ConnectionPool
	archive                 historyarchive.ArchiveInterface
	contractValidator       ContractValidator
	contractMetadataService ContractMetadataService
	trustlineAssetModel     wbdata.TrustlineAssetModelInterface
	accountTokensModel      wbdata.AccountTokensModelInterface
	contractModel           wbdata.ContractModelInterface
	networkPassphrase       string
}

// NewTokenCacheWriter creates a TokenCacheWriter for ingestion.
func NewTokenCacheWriter(
	dbPool db.ConnectionPool,
	networkPassphrase string,
	archive historyarchive.ArchiveInterface,
	contractValidator ContractValidator,
	contractMetadataService ContractMetadataService,
	trustlineAssetModel wbdata.TrustlineAssetModelInterface,
	accountTokensModel wbdata.AccountTokensModelInterface,
	contractModel wbdata.ContractModelInterface,
) TokenCacheWriter {
	return &tokenCacheService{
		db:                      dbPool,
		archive:                 archive,
		contractValidator:       contractValidator,
		contractMetadataService: contractMetadataService,
		trustlineAssetModel:     trustlineAssetModel,
		accountTokensModel:      accountTokensModel,
		contractModel:           contractModel,
		networkPassphrase:       networkPassphrase,
	}
}

// NewTokenCacheReader creates a TokenCacheReader for API queries.
func NewTokenCacheReader(
	dbPool db.ConnectionPool,
	trustlineAssetModel wbdata.TrustlineAssetModelInterface,
	accountTokensModel wbdata.AccountTokensModelInterface,
	contractModel wbdata.ContractModelInterface,
) TokenCacheReader {
	return &tokenCacheService{
		db:                  dbPool,
		trustlineAssetModel: trustlineAssetModel,
		accountTokensModel:  accountTokensModel,
		contractModel:       contractModel,
	}
}

// PopulateAccountTokens performs initial PostgreSQL cache population from Stellar history archive.
// This reads the specified checkpoint ledger and extracts all trustlines and contract tokens that an account has.
// Trustlines are streamed in batches of 50K to avoid memory pressure with 30M+ entries.
// The checkpoint ledger is calculated and passed in by the caller (ingestService).
// Warning: This is a long-running operation that may take several minutes.
func (s *tokenCacheService) PopulateAccountTokens(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateAccountTokens requires archive connection")
	}

	defer func() {
		if err := s.contractValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	log.Ctx(ctx).Infof("Populating account token cache from checkpoint ledger = %d", checkpointLedger)

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

	// Wrap ALL DB operations in a single transaction for atomicity
	err = db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// Stream trustlines and collect contracts from checkpoint
		cpData, txErr := s.streamCheckpointData(ctx, dbTx, reader)
		if txErr != nil {
			return fmt.Errorf("streaming checkpoint data: %w", txErr)
		}

		// Extract contract spec from WASM hash and validate SEP-41 contracts
		s.enrichContractTypes(ctx, cpData.ContractTypesByContractID, cpData.ContractIDsByWasmHash, cpData.ContractTypesByWasmHash)

		// FetchAndStoreMetadata inserts SAC/SEP-41 contracts and returns their IDs
		contractIDMap, txErr := s.contractMetadataService.FetchAndStoreMetadata(ctx, dbTx, cpData.ContractTypesByContractID)
		if txErr != nil {
			return fmt.Errorf("fetching and storing contract metadata: %w", txErr)
		}

		// Store contract relationships (contracts are fewer, still collected in memory)
		if txErr := s.storeContractsInPostgres(ctx, dbTx, cpData.ContractsByHolderAddress, contractIDMap); txErr != nil {
			return fmt.Errorf("storing contracts in postgres: %w", txErr)
		}

		if txErr := initializeCursors(dbTx); txErr != nil {
			return fmt.Errorf("initializing cursors: %w", txErr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("running db transaction for account tokens: %w", err)
	}
	return nil
}

// ProcessTokenChanges processes token changes and stores them in PostgreSQL.
// This is called by the indexer for each ledger's state changes during live ingestion.
//
// For trustlines: handles both ADD (new trustline created) and REMOVE (trustline deleted).
// For contract token balances (SAC, SEP41): only ADD operations are processed. Unknown contracts
// (not in contractIDMap) are silently skipped as we only track SAC/SEP-41 tokens.
//
// Trustline asset IDs are computed using DeterministicAssetID (no map needed).
// The contractIDMap must be pre-populated by calling GetOrInsertContractTokens before the main transaction.
// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
func (s *tokenCacheService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, contractIDMap map[string]int64, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// We sort the trustline changes in the order they happened and then apply them in that order.
	// The last operation for (account, trustline) will be applied.
	// We only store the net change for each (account, trustline) pair.
	type changeKey struct {
		accountID   string
		trustlineID int64
	}
	sort.Slice(trustlineChanges, func(i, j int) bool {
		return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
	})
	opsPerKey := make(map[changeKey]*types.TrustlineOpType)
	for _, change := range trustlineChanges {
		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			return fmt.Errorf("parsing asset string from trustline change for address %s: asset %s: %w", change.AccountID, change.Asset, err)
		}

		// Compute deterministic asset ID directly
		assetID := wbdata.DeterministicAssetID(code, issuer)

		key := changeKey{accountID: change.AccountID, trustlineID: assetID}
		// If the last operation is ADD, we remove the key from the map. This ensures we only track the net changes for an (account, trustline) pair.
		// Otherwise, we update the operation to REMOVE - this means we want to remove an existing trustline from the DB.
		if change.Operation == types.TrustlineOpRemove && opsPerKey[key] != nil && *opsPerKey[key] == types.TrustlineOpAdd {
			delete(opsPerKey, key)
		} else {
			opsPerKey[key] = &change.Operation
		}
	}

	trustlineChangesByAccount := make(map[string]*wbdata.TrustlineChanges)
	for trustlineKey, operation := range opsPerKey {
		if _, ok := trustlineChangesByAccount[trustlineKey.accountID]; !ok {
			trustlineChangesByAccount[trustlineKey.accountID] = &wbdata.TrustlineChanges{}
		}

		switch *operation {
		case types.TrustlineOpAdd:
			trustlineChangesByAccount[trustlineKey.accountID].AddIDs = append(trustlineChangesByAccount[trustlineKey.accountID].AddIDs, trustlineKey.trustlineID)
		case types.TrustlineOpRemove:
			trustlineChangesByAccount[trustlineKey.accountID].RemoveIDs = append(trustlineChangesByAccount[trustlineKey.accountID].RemoveIDs, trustlineKey.trustlineID)
		}
	}

	// Group contract changes by account using pre-computed numeric IDs from contractIDMap.
	// Unknown contracts (not in the map) are silently skipped - we only track SAC/SEP-41 tokens.
	contractsByAccount := make(map[string][]int64)
	for _, change := range contractChanges {
		if change.ContractID == "" {
			continue
		}
		// Only process contracts that exist in the pre-computed map (SAC/SEP-41)
		numericID, exists := contractIDMap[change.ContractID]
		if !exists {
			continue // Skip unknown contracts
		}
		contractsByAccount[change.AccountID] = append(contractsByAccount[change.AccountID], numericID)
	}

	// Execute all changes using the provided transaction
	// Batch upsert trustlines
	if len(trustlineChangesByAccount) > 0 {
		if err := s.accountTokensModel.BatchUpsertTrustlines(ctx, dbTx, trustlineChangesByAccount); err != nil {
			return fmt.Errorf("upserting trustlines: %w", err)
		}
	}

	// Batch add contracts
	if len(contractsByAccount) > 0 {
		if err := s.accountTokensModel.BatchAddContracts(ctx, dbTx, contractsByAccount); err != nil {
			return fmt.Errorf("adding contracts: %w", err)
		}
	}

	return nil
}

// parseAssetString parses a "CODE:ISSUER" formatted asset string into its components.
func parseAssetString(asset string) (code, issuer string, err error) {
	parts := strings.SplitN(asset, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid asset format: expected CODE:ISSUER, got %s", asset)
	}
	code, issuer = parts[0], parts[1]

	// Validate using txnbuild
	creditAsset := txnbuild.CreditAsset{Code: code, Issuer: issuer}
	if _, err := creditAsset.ToXDR(); err != nil {
		return "", "", fmt.Errorf("invalid asset %s: %w", asset, err)
	}
	return code, issuer, nil
}

// GetAccountTrustlines retrieves all trustlines for an account from PostgreSQL.
// Returns asset objects after resolving from internal IDs.
func (s *tokenCacheService) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]*wbdata.TrustlineAsset, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	// Get asset IDs from PostgreSQL
	ids, err := s.accountTokensModel.GetTrustlineAssetIDs(ctx, accountAddress)
	if err != nil {
		return nil, fmt.Errorf("getting trustline asset IDs for account %s: %w", accountAddress, err)
	}
	if len(ids) == 0 {
		return []*wbdata.TrustlineAsset{}, nil
	}

	// Resolve IDs to asset objects from PostgreSQL
	assets, err := s.trustlineAssetModel.BatchGetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("resolving asset IDs: %w", err)
	}

	return assets, nil
}

// GetAccountContracts retrieves all contract tokens for an account from PostgreSQL.
// For G-address: all non-SAC custom tokens because SAC tokens are already tracked in trustlines
// For C-address: all contract tokens (SAC, custom)
// Returns full Contract objects with metadata.
func (s *tokenCacheService) GetAccountContracts(ctx context.Context, accountAddress string) ([]*wbdata.Contract, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	// Get numeric contract IDs from PostgreSQL
	numericIDs, err := s.accountTokensModel.GetContractIDs(ctx, accountAddress)
	if err != nil {
		return nil, fmt.Errorf("getting contract IDs for account %s: %w", accountAddress, err)
	}
	if len(numericIDs) == 0 {
		return []*wbdata.Contract{}, nil
	}

	// Resolve numeric IDs to contract objects
	contracts, err := s.contractModel.BatchGetByIDs(ctx, numericIDs)
	if err != nil {
		return nil, fmt.Errorf("resolving contract IDs: %w", err)
	}

	return contracts, nil
}

// EnsureTrustlineAssetsExist inserts trustline assets into PostgreSQL with deterministic IDs.
// Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
func (s *tokenCacheService) EnsureTrustlineAssetsExist(ctx context.Context, dbTx pgx.Tx, trustlineChanges []types.TrustlineChange) error {
	if len(trustlineChanges) == 0 {
		return nil
	}

	// Extract unique assets from trustline changes and compute their IDs
	seen := set.NewSet[string]()
	var assets []wbdata.TrustlineAsset
	for _, change := range trustlineChanges {
		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			return fmt.Errorf("parsing asset %s: %w", change.Asset, err)
		}
		key := code + ":" + issuer
		if seen.Contains(key) {
			continue
		}
		seen.Add(key)
		assets = append(assets, wbdata.TrustlineAsset{
			ID:     wbdata.DeterministicAssetID(code, issuer),
			Code:   code,
			Issuer: issuer,
		})
	}

	// Insert all unique assets (ON CONFLICT DO NOTHING) using the provided transaction
	if err := s.trustlineAssetModel.BatchInsert(ctx, dbTx, assets); err != nil {
		return fmt.Errorf("batch inserting trustline assets: %w", err)
	}
	return nil
}

// GetOrInsertContractTokens gets IDs for all SAC/SEP-41 contracts in changes.
// For new contracts (not already in DB), fetches metadata via RPC. Uses BatchGetOrInsert for DB operations.
func (s *tokenCacheService) GetOrInsertContractTokens(ctx context.Context, dbTx pgx.Tx, contractChanges []types.ContractChange) (map[string]int64, error) {
	// Extract unique SAC/SEP-41 contracts from changes
	contractTypesByID := extractUniqueSACAndSEP41Contracts(contractChanges)
	if len(contractTypesByID) == 0 {
		return make(map[string]int64), nil
	}

	// Query DB for existing contract IDs (< 10k contracts, fast query)
	existingIDs, err := s.contractModel.GetAllContractIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting existing contract IDs: %w", err)
	}
	existingSet := set.NewSet(existingIDs...)

	// Separate new vs existing based on DB query
	newContractTypesByID := make(map[string]types.ContractType)
	existingContractIDs := make([]string, 0)
	for id, ctype := range contractTypesByID {
		if existingSet.Contains(id) {
			existingContractIDs = append(existingContractIDs, id)
		} else {
			newContractTypesByID[id] = ctype
		}
	}

	// Fetch metadata for NEW contracts only (no DB write yet)
	var contracts []*wbdata.Contract
	if len(newContractTypesByID) > 0 {
		newContracts, err := s.contractMetadataService.FetchMetadata(ctx, newContractTypesByID)
		if err != nil {
			return nil, fmt.Errorf("fetching metadata for new contracts: %w", err)
		}
		contracts = append(contracts, newContracts...)
	}

	// Add minimal contracts for existing ones (just need ContractID for lookup)
	for _, id := range existingContractIDs {
		contracts = append(contracts, &wbdata.Contract{ContractID: id})
	}

	if len(contracts) == 0 {
		return make(map[string]int64), nil
	}

	// BatchGetOrInsert handles everything:
	// - SELECTs existing → returns their IDs
	// - INSERTs new with metadata → returns their IDs
	idMap, err := s.contractModel.BatchGetOrInsert(ctx, dbTx, contracts)
	if err != nil {
		return nil, fmt.Errorf("batch get or insert contracts: %w", err)
	}
	return idMap, nil
}

// extractUniqueSACAndSEP41Contracts extracts unique SAC/SEP-41 contract IDs from changes.
func extractUniqueSACAndSEP41Contracts(contractChanges []types.ContractChange) map[string]types.ContractType {
	if len(contractChanges) == 0 {
		return nil
	}

	seen := set.NewSet[string]()
	result := make(map[string]types.ContractType)

	for _, change := range contractChanges {
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		if change.ContractID == "" || seen.Contains(change.ContractID) {
			continue
		}
		seen.Add(change.ContractID)
		result[change.ContractID] = change.ContractType
	}

	return result
}

// processTrustlineChange extracts trustline information from a ledger change entry.
// Returns the account address and asset string, with skip=true if the entry should be skipped.
func (s *tokenCacheService) processTrustlineChange(change ingest.Change) (string, wbdata.TrustlineAsset, bool) {
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
func (s *tokenCacheService) processContractBalanceChange(contractDataEntry xdr.ContractDataEntry) (holderAddress string, skip bool) {
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
func (s *tokenCacheService) processContractInstanceChange(
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

// streamCheckpointData reads from a ChangeReader and streams trustlines to DB in batches.
// Contract tokens are collected in memory (much fewer entries than trustlines).
// Returns checkpointData containing contract data for later processing.
func (s *tokenCacheService) streamCheckpointData(
	ctx context.Context,
	dbTx pgx.Tx,
	reader ingest.ChangeReader,
) (checkpointData, error) {
	data := checkpointData{
		ContractsByHolderAddress:  make(map[string][]string),
		ContractTypesByContractID: make(map[string]types.ContractType),
		ContractIDsByWasmHash:     make(map[xdr.Hash][]string),
		ContractTypesByWasmHash:   make(map[xdr.Hash]types.ContractType),
	}

	batch := newTrustlineBatch()
	entries := 0
	trustlineCount := 0
	batchCount := 0
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
			trustlineCount++

			batch.add(accountAddress, asset)

			// Flush batch when full
			if batch.count >= TrustlineBatchSize {
				if err := s.flushTrustlineBatch(ctx, dbTx, batch); err != nil {
					return checkpointData{}, fmt.Errorf("flushing trustline batch: %w", err)
				}
				batchCount++
				log.Ctx(ctx).Infof("Flushed trustline batch %d (%d entries so far)", batchCount, trustlineCount)
				batch.reset()
			}

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

	// Flush remaining trustlines
	if batch.count > 0 {
		if err := s.flushTrustlineBatch(ctx, dbTx, batch); err != nil {
			return checkpointData{}, fmt.Errorf("flushing final trustline batch: %w", err)
		}
		batchCount++
	}

	log.Ctx(ctx).Infof("Processed %d entries (%d trustlines in %d batches) in %.2f minutes",
		entries, trustlineCount, batchCount, time.Since(startTime).Minutes())
	return data, nil
}

// flushTrustlineBatch inserts the batch's trustline assets and account relationships.
func (s *tokenCacheService) flushTrustlineBatch(ctx context.Context, dbTx pgx.Tx, batch *trustlineBatch) error {
	// 1. Insert unique assets (ON CONFLICT DO NOTHING)
	assets := make([]wbdata.TrustlineAsset, 0, len(batch.uniqueAssets))
	for _, asset := range batch.uniqueAssets {
		assets = append(assets, asset)
	}
	if err := s.trustlineAssetModel.BatchInsert(ctx, dbTx, assets); err != nil {
		return fmt.Errorf("batch inserting assets: %w", err)
	}

	// 2. Bulk insert account trustlines (uses COPY protocol)
	if err := s.accountTokensModel.BulkInsertTrustlines(ctx, dbTx, batch.accountTrustlines); err != nil {
		return fmt.Errorf("bulk inserting account trustlines: %w", err)
	}

	return nil
}

// enrichContractTypes validates contract specs and enriches the contractTypesByContractID map with SEP-41 classifications.
func (s *tokenCacheService) enrichContractTypes(
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

// storeContractsInPostgres stores collected contract relationships into PostgreSQL.
// The contractIDMap contains pre-inserted SAC/SEP-41 contracts from FetchAndStoreMetadata;
// unknown contracts are skipped (not stored).
func (s *tokenCacheService) storeContractsInPostgres(
	ctx context.Context,
	dbTx pgx.Tx,
	contractsByAccountAddress map[string][]string,
	contractIDMap map[string]int64,
) error {
	if len(contractIDMap) == 0 {
		return nil
	}

	startTime := time.Now()

	// Convert contract addresses to numeric IDs for bulk insert using pre-populated contractIDMap.
	// Only SAC/SEP-41 contracts exist in the map; unknown contracts are skipped.
	contractIDsByAccount := make(map[string][]int64, len(contractsByAccountAddress))
	for accountAddress, contractAddrs := range contractsByAccountAddress {
		ids := make([]int64, 0, len(contractAddrs))
		for _, contractAddr := range contractAddrs {
			if id, ok := contractIDMap[contractAddr]; ok {
				ids = append(ids, id)
			}
			// Unknown contracts not in contractIDMap are silently skipped
		}
		if len(ids) > 0 {
			contractIDsByAccount[accountAddress] = ids
		}
	}

	// Bulk insert account-contract relationships
	if err := s.accountTokensModel.BulkInsertContracts(ctx, dbTx, contractIDsByAccount); err != nil {
		return fmt.Errorf("bulk inserting account contracts: %w", err)
	}
	log.Ctx(ctx).Infof("Stored account-contract relationships for %d SAC/SEP-41 contracts in %.2f minutes",
		len(contractIDMap), time.Since(startTime).Minutes())

	return nil
}

// extractHolderAddress extracts the account address from a contract balance entry key.
// Balance entries have a key that is a ScVec with 2 elements:
// - First element: ScSymbol("Balance")
// - Second element: ScAddress (the account/contract holder address)
// Returns the holder address as a Stellar-encoded string, or empty string if invalid.
func (s *tokenCacheService) extractHolderAddress(key xdr.ScVal) (string, error) {
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
