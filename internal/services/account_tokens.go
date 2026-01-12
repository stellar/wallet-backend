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

	// GetOrInsertTrustlineAssets gets IDs of trustline assets in the changes and inserts any new assets into PostgreSQL in a separate committed
	// transaction. This MUST be called BEFORE the main ingestion transaction to prevent orphan
	// asset IDs when the main transaction rolls back (PostgreSQL sequences don't roll back).
	// Returns a map of "CODE:ISSUER" -> ID for use in ProcessTokenChanges.
	GetOrInsertTrustlineAssets(ctx context.Context, trustlineChanges []types.TrustlineChange) (map[string]int64, error)

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
	// The trustlineAssetIDMap parameter must be pre-populated by calling GetOrInsertTrustlineAssets first.
	// The contractIDMap parameter must be pre-populated by calling FetchAndStoreMetadata first.
	// Contracts not in contractIDMap (e.g., unknown contracts) are silently skipped.
	ProcessTokenChanges(ctx context.Context, trustlineAssetIDMap map[string]int64, contractIDMap map[string]int64, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
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

	// Collect account tokens from checkpoint (~7 mins, no transaction needed)
	cpData, err := s.collectAccountTokensFromCheckpoint(ctx, reader)
	if err != nil {
		return err
	}

	// Extract contract spec from WASM hash and validate SEP-41 contracts
	s.enrichContractTypes(ctx, cpData.ContractTypesByContractID, cpData.ContractIDsByWasmHash, cpData.ContractTypesByWasmHash)

	// Wrap DB operations in a single transaction
	err = db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// FetchAndStoreMetadata inserts SAC/SEP-41 contracts and returns their IDs
		contractIDMap, txErr := s.contractMetadataService.FetchAndStoreMetadata(ctx, dbTx, cpData.ContractTypesByContractID)
		if txErr != nil {
			return fmt.Errorf("fetching and storing contract metadata: %w", txErr)
		}
		if txErr := s.storeAccountTokensInPostgres(ctx, dbTx, cpData.TrustlinesByAccountAddress, cpData.ContractsByHolderAddress, cpData.TrustlineFrequency, contractIDMap); txErr != nil {
			return fmt.Errorf("storing account tokens in postgres: %w", txErr)
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
// The trustlineAssetIDMap must be pre-populated by calling GetOrInsertTrustlineAssets before the main transaction.
// The contractIDMap must be pre-populated by calling FetchAndStoreMetadata before the main transaction.
func (s *tokenCacheService) ProcessTokenChanges(ctx context.Context, trustlineAssetIDMap map[string]int64, contractIDMap map[string]int64, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// Group trustline changes by account
	trustlineChangesByAccount := make(map[string]*wbdata.TrustlineChanges)
	for _, change := range trustlineChanges {
		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			return fmt.Errorf("parsing asset string from trustline change for address %s: asset %s: %w", change.AccountID, change.Asset, err)
		}

		assetID, exists := trustlineAssetIDMap[code+":"+issuer]
		if !exists {
			return fmt.Errorf("asset ID not found for asset %s", code+":"+issuer)
		}

		if _, ok := trustlineChangesByAccount[change.AccountID]; !ok {
			trustlineChangesByAccount[change.AccountID] = &wbdata.TrustlineChanges{}
		}

		switch change.Operation {
		case types.TrustlineOpAdd:
			trustlineChangesByAccount[change.AccountID].AddIDs = append(
				trustlineChangesByAccount[change.AccountID].AddIDs, assetID)
		case types.TrustlineOpRemove:
			trustlineChangesByAccount[change.AccountID].RemoveIDs = append(
				trustlineChangesByAccount[change.AccountID].RemoveIDs, assetID)
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

	// Execute all changes in a transaction
	err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// Batch upsert trustlines
		if len(trustlineChangesByAccount) > 0 {
			if txErr := s.accountTokensModel.BatchUpsertTrustlines(ctx, dbTx, trustlineChangesByAccount); txErr != nil {
				return fmt.Errorf("upserting trustlines: %w", txErr)
			}
		}

		// Batch add contracts
		if len(contractsByAccount) > 0 {
			if txErr := s.accountTokensModel.BatchAddContracts(ctx, dbTx, contractsByAccount); txErr != nil {
				return fmt.Errorf("adding contracts: %w", txErr)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("executing token changes: %w", err)
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

// GetOrInsertTrustlineAssets gets IDs of trustline assets in the changes and inserts any new assets into PostgreSQL in a separate committed transaction.
// This prevents orphan asset IDs when the main ingestion transaction rolls back.
func (s *tokenCacheService) GetOrInsertTrustlineAssets(ctx context.Context, trustlineChanges []types.TrustlineChange) (map[string]int64, error) {
	if len(trustlineChanges) == 0 {
		return make(map[string]int64), nil
	}

	// Extract unique assets from trustline changes
	uniqueAssets := set.NewSet[wbdata.TrustlineAsset]()
	for _, change := range trustlineChanges {
		code, issuer, err := parseAssetString(change.Asset)
		if err != nil {
			return nil, fmt.Errorf("parsing asset %s: %w", change.Asset, err)
		}
		uniqueAssets.Add(wbdata.TrustlineAsset{Code: code, Issuer: issuer})
	}

	// Get or insert all unique assets in PostgreSQL
	assetIDMap := make(map[string]int64)
	err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		idMap, txErr := s.trustlineAssetModel.BatchGetOrInsert(ctx, dbTx, uniqueAssets.ToSlice())
		if txErr != nil {
			return fmt.Errorf("batch get or insert trustline assets: %w", txErr)
		}
		for asset, id := range idMap {
			assetIDMap[asset] = id
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inserting trustline assets: %w", err)
	}

	return assetIDMap, nil
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

// collectAccountTokensFromCheckpoint reads from a ChangeReader and collects all trustlines and contract balances.
// Returns checkpointData containing maps of trustlines, contracts, contract types, and contract IDs grouped by WASM hash.
func (s *tokenCacheService) collectAccountTokensFromCheckpoint(
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

// storeAccountTokensInPostgres stores all collected trustlines and contracts into PostgreSQL.
// Trustline assets are converted to short integer IDs (stored in PostgreSQL) to reduce memory usage.
// Assets are sorted by frequency before insertion so the most common assets get the lowest IDs.
// The contractIDMap contains pre-inserted SAC/SEP-41 contracts from FetchAndStoreMetadata;
// unknown contracts are skipped (not stored).
func (s *tokenCacheService) storeAccountTokensInPostgres(
	ctx context.Context,
	dbTx pgx.Tx,
	trustlinesByAccountAddress map[string][]string,
	contractsByAccountAddress map[string][]string,
	trustlineFrequency map[wbdata.TrustlineAsset]int64,
	contractIDMap map[string]int64,
) error {
	startTime := time.Now()

	// Batch-assign IDs to all unique trustline assets using PostgreSQL
	uniqueTrustlines := s.processTrustlineAssets(trustlineFrequency)
	assetIDMap, err := s.trustlineAssetModel.BatchInsert(ctx, dbTx, uniqueTrustlines)
	if err != nil {
		return fmt.Errorf("batch inserting trustline assets: %w", err)
	}
	log.Ctx(ctx).Infof("Inserted %d unique trustline assets (sorted by frequency)", len(assetIDMap))

	// Convert asset strings to IDs for bulk insert
	trustlineIDsByAccount := make(map[string][]int64, len(trustlinesByAccountAddress))
	for accountAddress, assets := range trustlinesByAccountAddress {
		ids := make([]int64, 0, len(assets))
		for _, asset := range assets {
			if id, ok := assetIDMap[asset]; ok {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			trustlineIDsByAccount[accountAddress] = ids
		}
	}

	// Bulk insert trustlines
	if err := s.accountTokensModel.BulkInsertTrustlines(ctx, dbTx, trustlineIDsByAccount); err != nil {
		return fmt.Errorf("bulk inserting account trustlines: %w", err)
	}

	// Convert contract addresses to numeric IDs for bulk insert using pre-populated contractIDMap.
	// Only SAC/SEP-41 contracts exist in the map; unknown contracts are skipped.
	if len(contractIDMap) > 0 {
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
		log.Ctx(ctx).Infof("Stored account-contract relationships for %d SAC/SEP-41 contracts", len(contractIDMap))
	}

	log.Ctx(ctx).Infof("Stored %d account trustline sets in PostgreSQL in %.2f minutes", len(trustlinesByAccountAddress), time.Since(startTime).Minutes())
	return nil
}

// processTrustlineAssets sorts assets by frequency (descending) for optimal storage.
// Most frequent assets get lowest IDs (1, 2, 3...).
func (s *tokenCacheService) processTrustlineAssets(trustlineFrequency map[wbdata.TrustlineAsset]int64) []wbdata.TrustlineAsset {
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
