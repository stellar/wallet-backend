// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	// FlushBatchSize is the number of entries to buffer before flushing to DB.
	flushBatchSize = 200_000
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

// batch holds a batch of trustline balances and native balances for streaming insertion.
type batch struct {
	// trustlines holds the trustline balance entries for batch insert
	trustlines []wbdata.TrustlineBalance
	// nativeBalances holds the native balance entries for batch insert
	nativeBalances []wbdata.NativeBalance
	// uniqueAssets tracks unique assets with their computed IDs for batch insert
	uniqueAssets map[string]wbdata.TrustlineAsset
	// trustlineAssetModel is the model for inserting trustline assets
	trustlineAssetModel wbdata.TrustlineAssetModelInterface
	// trustlineBalanceModel is the model for inserting trustline balances
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface
	// nativeBalanceModel is the model for inserting native balances
	nativeBalanceModel wbdata.NativeBalanceModelInterface
}

func newBatch(
	trustlineAssetModel wbdata.TrustlineAssetModelInterface,
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface,
	nativeBalanceModel wbdata.NativeBalanceModelInterface,
) *batch {
	return &batch{
		trustlines:            make([]wbdata.TrustlineBalance, 0, flushBatchSize),
		nativeBalances:        make([]wbdata.NativeBalance, 0, flushBatchSize),
		uniqueAssets:          make(map[string]wbdata.TrustlineAsset),
		trustlineAssetModel:   trustlineAssetModel,
		trustlineBalanceModel: trustlineBalanceModel,
		nativeBalanceModel:    nativeBalanceModel,
	}
}

func (b *batch) addTrustline(accountAddress string, asset wbdata.TrustlineAsset, balance, limit, buyingLiabilities, sellingLiabilities int64, flags uint32, ledger uint32) {
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

	// Add trustline balance with all XDR fields
	b.trustlines = append(b.trustlines, wbdata.TrustlineBalance{
		AccountAddress:     accountAddress,
		AssetID:            assetID,
		Balance:            balance,
		Limit:              limit,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		Flags:              flags,
		LedgerNumber:       ledger,
	})
}

func (b *batch) addNativeBalance(accountAddress string, balance, buyingLiabilities, sellingLiabilities int64, ledger uint32) {
	b.nativeBalances = append(b.nativeBalances, wbdata.NativeBalance{
		AccountAddress:     accountAddress,
		Balance:            balance,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		LedgerNumber:       ledger,
	})
}

// flush inserts the batch's data into DB.
func (b *batch) flush(ctx context.Context, dbTx pgx.Tx) error {
	// 1. Insert unique assets (ON CONFLICT DO NOTHING)
	assets := make([]wbdata.TrustlineAsset, 0, len(b.uniqueAssets))
	for _, asset := range b.uniqueAssets {
		assets = append(assets, asset)
	}
	if err := b.trustlineAssetModel.BatchInsert(ctx, dbTx, assets); err != nil {
		return fmt.Errorf("batch inserting assets: %w", err)
	}

	// 2. Batch insert trustline balances using BatchCopy
	if err := b.trustlineBalanceModel.BatchCopy(ctx, dbTx, b.trustlines); err != nil {
		return fmt.Errorf("batch inserting trustline balances: %w", err)
	}

	// 3. Batch insert native balances using BatchCopy
	if err := b.nativeBalanceModel.BatchCopy(ctx, dbTx, b.nativeBalances); err != nil {
		return fmt.Errorf("batch inserting native balances: %w", err)
	}

	return nil
}

func (b *batch) count() int {
	return len(b.trustlines) + len(b.nativeBalances)
}

func (b *batch) reset() {
	b.trustlines = b.trustlines[:0]
	b.nativeBalances = b.nativeBalances[:0]
	b.uniqueAssets = make(map[string]wbdata.TrustlineAsset)
}

// TokenIngestionService provides write access to account token storage during ingestion.
type TokenIngestionService interface {
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
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange) error
}

// Verify interface compliance at compile time
var _ TokenIngestionService = (*tokenIngestionService)(nil)

// tokenIngestionService implements TokenIngestionService.
type tokenIngestionService struct {
	db                         db.ConnectionPool
	archive                    historyarchive.ArchiveInterface
	contractValidator          ContractValidator
	contractMetadataService    ContractMetadataService
	trustlineAssetModel        wbdata.TrustlineAssetModelInterface
	trustlineBalanceModel      wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel         wbdata.NativeBalanceModelInterface
	accountContractTokensModel wbdata.AccountContractTokensModelInterface
	contractModel              wbdata.ContractModelInterface
	networkPassphrase          string
}

// NewTokenIngestionService creates a TokenIngestionService for ingestion.
func NewTokenIngestionService(
	dbPool db.ConnectionPool,
	networkPassphrase string,
	archive historyarchive.ArchiveInterface,
	contractValidator ContractValidator,
	contractMetadataService ContractMetadataService,
	trustlineAssetModel wbdata.TrustlineAssetModelInterface,
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface,
	nativeBalanceModel wbdata.NativeBalanceModelInterface,
	accountContractTokensModel wbdata.AccountContractTokensModelInterface,
	contractModel wbdata.ContractModelInterface,
) TokenIngestionService {
	return &tokenIngestionService{
		db:                         dbPool,
		archive:                    archive,
		contractValidator:          contractValidator,
		contractMetadataService:    contractMetadataService,
		trustlineAssetModel:        trustlineAssetModel,
		trustlineBalanceModel:      trustlineBalanceModel,
		nativeBalanceModel:         nativeBalanceModel,
		accountContractTokensModel: accountContractTokensModel,
		contractModel:              contractModel,
		networkPassphrase:          networkPassphrase,
	}
}

// PopulateAccountTokens performs initial PostgreSQL cache population from Stellar history archive.
// This reads the specified checkpoint ledger and extracts all trustlines and contract tokens that an account has.
// Trustlines are streamed in batches of 50K to avoid memory pressure with 30M+ entries.
// The checkpoint ledger is calculated and passed in by the caller (ingestService).
// Warning: This is a long-running operation that may take several minutes.
func (s *tokenIngestionService) PopulateAccountTokens(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error {
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
		cpData, txErr := s.streamCheckpointData(ctx, dbTx, reader, checkpointLedger)
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
func (s *tokenIngestionService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange) error {
	if len(trustlineChangesByTrustlineKey) == 0 && len(contractChanges) == 0 && len(accountChangesByAccountID) == 0 {
		return nil
	}

	// Separate into upserts and deletes
	var upserts []wbdata.TrustlineBalance
	var deletes []wbdata.TrustlineBalance
	for key, change := range trustlineChangesByTrustlineKey {
		fullData := wbdata.TrustlineBalance{
			AccountAddress:     change.AccountID,
			AssetID:            key.TrustlineID,
			Balance:            change.Balance,
			Limit:              change.Limit,
			BuyingLiabilities:  change.BuyingLiabilities,
			SellingLiabilities: change.SellingLiabilities,
			Flags:              change.Flags,
			LedgerNumber:       change.LedgerNumber,
		}
		if change.Operation == types.TrustlineOpRemove {
			deletes = append(deletes, fullData)
		} else {
			upserts = append(upserts, fullData)
		}
	}

	// Execute all changes using the provided transaction
	// Batch upsert trustline balances with full XDR data
	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.trustlineBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting trustline balances: %w", err)
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

	// Batch insert contract tokens
	if len(contractsByAccount) > 0 {
		if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractsByAccount); err != nil {
			return fmt.Errorf("batch inserting contract tokens: %w", err)
		}
	}

	// Process account changes (native XLM balance)
	// Deduplication and no-op handling already done in IndexerBuffer
	if len(accountChangesByAccountID) > 0 {
		var nativeUpserts []wbdata.NativeBalance
		var nativeDeletes []string
		for _, change := range accountChangesByAccountID {
			if change.Operation == types.AccountOpRemove {
				nativeDeletes = append(nativeDeletes, change.AccountID)
			} else {
				nativeUpserts = append(nativeUpserts, wbdata.NativeBalance{
					AccountAddress:     change.AccountID,
					Balance:            change.Balance,
					BuyingLiabilities:  change.BuyingLiabilities,
					SellingLiabilities: change.SellingLiabilities,
					LedgerNumber:       change.LedgerNumber,
				})
			}
		}

		if len(nativeUpserts) > 0 || len(nativeDeletes) > 0 {
			if err := s.nativeBalanceModel.BatchUpsert(ctx, dbTx, nativeUpserts, nativeDeletes); err != nil {
				return fmt.Errorf("upserting native balances: %w", err)
			}
		}
	}

	return nil
}

// trustlineXDRFields holds all XDR fields extracted from a trustline entry.
type trustlineXDRFields struct {
	Balance            int64
	Limit              int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	Flags              uint32
}

// processTrustlineChange extracts trustline information from a ledger change entry.
// Returns the account address, asset, XDR fields, and skip=true if the entry should be skipped.
func (s *tokenIngestionService) processTrustlineChange(change ingest.Change) (string, wbdata.TrustlineAsset, trustlineXDRFields, bool) {
	trustlineEntry := change.Post.Data.MustTrustLine()
	accountAddress := trustlineEntry.AccountId.Address()
	asset := trustlineEntry.Asset

	// Skip liquidity pool shares as they're tracked separately via pool-specific indexing
	// and don't represent traditional trustlines.
	if asset.Type == xdr.AssetTypeAssetTypePoolShare {
		return "", wbdata.TrustlineAsset{}, trustlineXDRFields{}, true
	}

	var assetType, assetCode, assetIssuer string
	if err := trustlineEntry.Asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return "", wbdata.TrustlineAsset{}, trustlineXDRFields{}, true
	}

	liabilities := trustlineEntry.Liabilities()

	return accountAddress, wbdata.TrustlineAsset{
			Code:   assetCode,
			Issuer: assetIssuer,
		}, trustlineXDRFields{
			Balance:            int64(trustlineEntry.Balance),
			Limit:              int64(trustlineEntry.Limit),
			BuyingLiabilities:  int64(liabilities.Buying),
			SellingLiabilities: int64(liabilities.Selling),
			Flags:              uint32(trustlineEntry.Flags),
		}, false
}

// processContractBalanceChange extracts contract balance information from a contract data entry.
// Returns the holder address and contract ID, with skip=true if extraction fails.
func (s *tokenIngestionService) processContractBalanceChange(contractDataEntry xdr.ContractDataEntry) (holderAddress string, skip bool) {
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
func (s *tokenIngestionService) processContractInstanceChange(
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
func (s *tokenIngestionService) streamCheckpointData(
	ctx context.Context,
	dbTx pgx.Tx,
	reader ingest.ChangeReader,
	checkpointLedger uint32,
) (checkpointData, error) {
	data := checkpointData{
		ContractsByHolderAddress:  make(map[string][]string),
		ContractTypesByContractID: make(map[string]types.ContractType),
		ContractIDsByWasmHash:     make(map[xdr.Hash][]string),
		ContractTypesByWasmHash:   make(map[xdr.Hash]types.ContractType),
	}

	batch := newBatch(s.trustlineAssetModel, s.trustlineBalanceModel, s.nativeBalanceModel)
	entries := 0
	trustlineCount := 0
	accountCount := 0
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
		case xdr.LedgerEntryTypeAccount:
			accountEntry := change.Post.Data.MustAccount()
			liabilities := accountEntry.Liabilities()
			batch.addNativeBalance(accountEntry.AccountId.Address(), int64(accountEntry.Balance), int64(liabilities.Buying), int64(liabilities.Selling), checkpointLedger)
			entries++
			accountCount++

		case xdr.LedgerEntryTypeTrustline:
			accountAddress, asset, xdrFields, skip := s.processTrustlineChange(change)
			if skip {
				continue
			}
			entries++
			trustlineCount++
			batch.addTrustline(accountAddress, asset, xdrFields.Balance, xdrFields.Limit, xdrFields.BuyingLiabilities, xdrFields.SellingLiabilities, xdrFields.Flags, checkpointLedger)

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

		// Flush batch when full
		if batch.count() >= flushBatchSize {
			if err := batch.flush(ctx, dbTx); err != nil {
				return checkpointData{}, fmt.Errorf("flushing batch: %w", err)
			}
			batchCount++
			log.Ctx(ctx).Infof("Flushed batch %d (%d entries so far)", batchCount, entries)
			batch.reset()
		}
	}

	// Flush remaining data
	if batch.count() > 0 {
		if err := batch.flush(ctx, dbTx); err != nil {
			return checkpointData{}, fmt.Errorf("flushing final trustline batch: %w", err)
		}
		batchCount++
	}

	log.Ctx(ctx).Infof("Processed %d entries (%d trustlines, %d accounts in %d batches) in %.2f minutes",
		entries, trustlineCount, accountCount, batchCount, time.Since(startTime).Minutes())
	return data, nil
}

// enrichContractTypes validates contract specs and enriches the contractTypesByContractID map with SEP-41 classifications.
func (s *tokenIngestionService) enrichContractTypes(
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
func (s *tokenIngestionService) storeContractsInPostgres(
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

	// Batch insert account-contract relationships
	if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractIDsByAccount); err != nil {
		return fmt.Errorf("batch inserting account contracts: %w", err)
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
func (s *tokenIngestionService) extractHolderAddress(key xdr.ScVal) (string, error) {
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
