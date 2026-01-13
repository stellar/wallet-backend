// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// TrustlineBatchSize is the number of trustline entries to buffer before flushing to DB.
	trustlineBatchSize = 500_000
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
	accountTrustlines map[string][]uuid.UUID
	// uniqueAssets tracks unique assets with their computed IDs for batch insert
	uniqueAssets map[string]wbdata.TrustlineAsset
	// count tracks total trustline entries in this batch
	count int
}

func newTrustlineBatch() *trustlineBatch {
	return &trustlineBatch{
		accountTrustlines: make(map[string][]uuid.UUID),
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
	b.accountTrustlines = make(map[string][]uuid.UUID)
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
	// Both trustline and contract IDs are computed using deterministic hash functions (DeterministicAssetID, DeterministicContractID).
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
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

		// Fetch metadata for SAC/SEP-41 contracts and store in database
		contracts, txErr := s.contractMetadataService.FetchMetadata(ctx, cpData.ContractTypesByContractID)
		if txErr != nil {
			return fmt.Errorf("fetching contract metadata: %w", txErr)
		}
		if len(contracts) > 0 {
			if txErr = s.contractModel.BatchInsert(ctx, dbTx, contracts); txErr != nil {
				return fmt.Errorf("storing contract metadata: %w", txErr)
			}
		}

		// Store contract relationships using deterministic IDs
		if txErr := s.storeContractsInPostgres(ctx, dbTx, cpData.ContractsByHolderAddress, cpData.ContractTypesByContractID); txErr != nil {
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
// (not SAC/SEP-41) are silently skipped.
//
// Both trustline and contract IDs are computed using deterministic hash functions.
// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
func (s *tokenCacheService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// We sort the trustline changes in the order they happened and then apply them in that order.
	// The last operation for (account, trustline) will be applied.
	// We only store the net change for each (account, trustline) pair.
	type changeKey struct {
		accountID   string
		trustlineID uuid.UUID
	}
	sort.Slice(trustlineChanges, func(i, j int) bool {
		return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
	})
	opsPerKey := make(map[changeKey]*types.TrustlineOpType)
	for _, change := range trustlineChanges {
		code, issuer, err := indexer.ParseAssetString(change.Asset)
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

	// Group contract changes by account using deterministic IDs.
	// Only SAC/SEP-41 contracts are processed; others are silently skipped.
	contractsByAccount := make(map[string][]uuid.UUID)
	for _, change := range contractChanges {
		if change.ContractID == "" {
			continue
		}
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		contractID := wbdata.DeterministicContractID(change.ContractID)
		contractsByAccount[change.AccountID] = append(contractsByAccount[change.AccountID], contractID)
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
			if batch.count >= trustlineBatchSize {
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
// The contractTypesByContractID maps contract addresses to their types (SAC/SEP-41);
// unknown contracts (not in the map) are skipped.
func (s *tokenCacheService) storeContractsInPostgres(
	ctx context.Context,
	dbTx pgx.Tx,
	contractsByAccountAddress map[string][]string,
	contractTypesByContractID map[string]types.ContractType,
) error {
	if len(contractTypesByContractID) == 0 {
		return nil
	}

	startTime := time.Now()

	// Convert contract addresses to UUIDs for bulk insert using deterministic IDs.
	// Only SAC/SEP-41 contracts (in contractTypesByContractID) are processed.
	contractIDsByAccount := make(map[string][]uuid.UUID, len(contractsByAccountAddress))
	for accountAddress, contractAddrs := range contractsByAccountAddress {
		ids := make([]uuid.UUID, 0, len(contractAddrs))
		for _, contractAddr := range contractAddrs {
			// Only include contracts that are known SAC/SEP-41 types
			if _, ok := contractTypesByContractID[contractAddr]; ok {
				ids = append(ids, wbdata.DeterministicContractID(contractAddr))
			}
			// Unknown contracts not in contractTypesByContractID are silently skipped
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
		len(contractTypesByContractID), time.Since(startTime).Minutes())

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
