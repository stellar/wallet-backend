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
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// FlushBatchSize is the number of entries to buffer before flushing to DB.
	flushBatchSize = 200_000
)

// checkpointData holds all data collected from processing a checkpoint ledger.
// Note: Trustlines, native balances, and SAC balances are streamed directly to DB in batches.
type checkpointData struct {
	// contractTokensByHolderAddress maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	contractTokensByHolderAddress map[string][]uuid.UUID
	// contractIDsByWasmHash groups contract IDs by their WASM hash for batch validation
	contractIDsByWasmHash map[xdr.Hash][]string
	// contractTypesByWasmHash maps WASM hashes to their contract code bytes
	contractTypesByWasmHash map[xdr.Hash]types.ContractType
	// uniqueAssets stores unique asset metadata extracted from ledger (no RPC needed)
	uniqueAssets map[uuid.UUID]*wbdata.TrustlineAsset
	// uniqueContractTokens stores unique contract metadata extracted from ledger (no RPC needed)
	uniqueContractTokens map[uuid.UUID]*wbdata.Contract
}

// batch holds a batch of trustline balances, native balances, and SAC balances for streaming insertion.
type batch struct {
	// trustlineBalances holds the trustline balance entries for batch insert
	trustlineBalances []wbdata.TrustlineBalance
	// nativeBalances holds the native balance entries for batch insert
	nativeBalances []wbdata.NativeBalance
	// sacBalances holds the SAC balance entries for batch insert
	sacBalances []wbdata.SACBalance
	// trustlineBalanceModel is the model for inserting trustline balances
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface
	// nativeBalanceModel is the model for inserting native balances
	nativeBalanceModel wbdata.NativeBalanceModelInterface
	// sacBalanceModel is the model for inserting SAC balances
	sacBalanceModel wbdata.SACBalanceModelInterface
}

func newBatch(
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface,
	nativeBalanceModel wbdata.NativeBalanceModelInterface,
	sacBalanceModel wbdata.SACBalanceModelInterface,
) *batch {
	return &batch{
		trustlineBalances:     make([]wbdata.TrustlineBalance, 0, flushBatchSize),
		nativeBalances:        make([]wbdata.NativeBalance, 0, flushBatchSize),
		sacBalances:           make([]wbdata.SACBalance, 0, flushBatchSize),
		trustlineBalanceModel: trustlineBalanceModel,
		nativeBalanceModel:    nativeBalanceModel,
		sacBalanceModel:       sacBalanceModel,
	}
}

func (b *batch) addTrustline(accountAddress string, asset wbdata.TrustlineAsset, balance, limit, buyingLiabilities, sellingLiabilities int64, flags uint32, ledger uint32) {
	// Add trustline balance with all XDR fields
	b.trustlineBalances = append(b.trustlineBalances, wbdata.TrustlineBalance{
		AccountAddress:     accountAddress,
		AssetID:            wbdata.DeterministicAssetID(asset.Code, asset.Issuer),
		Balance:            balance,
		Limit:              limit,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		Flags:              flags,
		LedgerNumber:       ledger,
	})
}

func (b *batch) addNativeBalance(accountAddress string, balance, minimumBalance, buyingLiabilities, sellingLiabilities int64, ledger uint32) {
	b.nativeBalances = append(b.nativeBalances, wbdata.NativeBalance{
		AccountAddress:     accountAddress,
		Balance:            balance,
		MinimumBalance:     minimumBalance,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		LedgerNumber:       ledger,
	})
}

func (b *batch) addSACBalance(sacBalance wbdata.SACBalance) {
	b.sacBalances = append(b.sacBalances, sacBalance)
}

// flush inserts the batch's data into DB.
func (b *batch) flush(ctx context.Context, dbTx pgx.Tx) error {
	// 1. Batch insert trustline balances using BatchCopy
	if err := b.trustlineBalanceModel.BatchCopy(ctx, dbTx, b.trustlineBalances); err != nil {
		return fmt.Errorf("batch inserting trustline balances: %w", err)
	}

	// 2. Batch insert native balances using BatchCopy
	if err := b.nativeBalanceModel.BatchCopy(ctx, dbTx, b.nativeBalances); err != nil {
		return fmt.Errorf("batch inserting native balances: %w", err)
	}

	// 3. Batch insert SAC balances using BatchCopy
	if err := b.sacBalanceModel.BatchCopy(ctx, dbTx, b.sacBalances); err != nil {
		return fmt.Errorf("batch inserting SAC balances: %w", err)
	}

	return nil
}

func (b *batch) count() int {
	return len(b.trustlineBalances) + len(b.nativeBalances) + len(b.sacBalances)
}

func (b *batch) reset() {
	b.trustlineBalances = b.trustlineBalances[:0]
	b.nativeBalances = b.nativeBalances[:0]
	b.sacBalances = b.sacBalances[:0]
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

	// ProcessTokenChanges applies trustline, contract, and SAC balance changes to PostgreSQL.
	// This is called by the indexer for each ledger's state changes during live ingestion.
	//
	// Storage semantics differ between token types:
	// - Trustlines: Can be added or removed. When all trustlines for an account are removed,
	//   the account's entry is deleted from PostgreSQL.
	// - Contracts: Only SEP-41 contracts are tracked (contracts accumulate). Unknown contracts
	//   are skipped. Contract balance entries persist in the ledger even when balance is zero,
	//   so we track all contracts an account has ever held a balance in.
	// - SAC Balances: For contract addresses (C...) only. Stores absolute balance values with
	//   authorized and clawback flags. G-addresses use trustlines for SAC balances.
	//
	// Both trustline and contract IDs are computed using deterministic hash functions (DeterministicAssetID, DeterministicContractID).
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error
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
	sacBalanceModel            wbdata.SACBalanceModelInterface
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
	sacBalanceModel wbdata.SACBalanceModelInterface,
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
		sacBalanceModel:            sacBalanceModel,
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
		sep41Tokens, err := s.fetchSep41Metadata(ctx, cpData.contractIDsByWasmHash, cpData.contractTypesByWasmHash)
		if err != nil {
			return fmt.Errorf("fetching SEP-41 token metadata: %w", err)
		}
		for _, token := range sep41Tokens {
			cpData.uniqueContractTokens[token.ID] = token
		}

		// Store SEP-41 contract relationships using deterministic IDs
		if txErr := s.storeTokensInDB(ctx, dbTx, cpData.contractTokensByHolderAddress, cpData.uniqueAssets, cpData.uniqueContractTokens); txErr != nil {
			return fmt.Errorf("storing SEP-41 tokens in postgres: %w", txErr)
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
// For trustlines: handles ADD (new trustline), UPDATE (balance/limit changed), and REMOVE (deleted).
// For contract token balances (SAC, SEP41): only ADD operations are processed. Unknown contracts
// (not SAC/SEP-41) are silently skipped.
// For SAC balances (contract addresses only): handles ADD, UPDATE, and REMOVE operations
// with absolute balance values and authorization flags.
//
// Both trustline and contract IDs are computed using deterministic hash functions.
// The dbTx parameter allows this function to participate in an outer transaction for atomicity.
func (s *tokenIngestionService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(trustlineChangesByTrustlineKey) == 0 && len(contractChanges) == 0 && len(accountChangesByAccountID) == 0 && len(sacBalanceChangesByKey) == 0 {
		return nil
	}

	if err := s.processTrustlineChanges(ctx, dbTx, trustlineChangesByTrustlineKey); err != nil {
		return err
	}
	if err := s.processContractTokenChanges(ctx, dbTx, contractChanges); err != nil {
		return err
	}
	if err := s.processNativeBalanceChanges(ctx, dbTx, accountChangesByAccountID); err != nil {
		return err
	}
	if err := s.processSACBalanceChanges(ctx, dbTx, sacBalanceChangesByKey); err != nil {
		return err
	}
	return nil
}

// processTrustlineChanges handles trustline balance upserts and deletes.
func (s *tokenIngestionService) processTrustlineChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.TrustlineChangeKey]types.TrustlineChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.TrustlineBalance
	var deletes []wbdata.TrustlineBalance
	for key, change := range changesByKey {
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

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.trustlineBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}
	log.Ctx(ctx).Infof("✅ upserted %d trustlines, deleted %d trustlines", len(upserts), len(deletes))
	return nil
}

// processContractTokenChanges handles SEP-41 contract token inserts.
func (s *tokenIngestionService) processContractTokenChanges(ctx context.Context, dbTx pgx.Tx, changes []types.ContractChange) error {
	if len(changes) == 0 {
		return nil
	}

	contractTokensByAccount := make(map[string][]uuid.UUID)
	for _, change := range changes {
		if change.ContractID == "" {
			continue
		}
		// Only process SEP-41 contracts
		if change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		contractID := wbdata.DeterministicContractID(change.ContractID)
		contractTokensByAccount[change.AccountID] = append(contractTokensByAccount[change.AccountID], contractID)
	}

	if len(contractTokensByAccount) > 0 {
		if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractTokensByAccount); err != nil {
			return fmt.Errorf("batch inserting contract tokens: %w", err)
		}
	}
	log.Ctx(ctx).Infof("✅ inserted %d contract tokens", len(changes))
	return nil
}

// processNativeBalanceChanges handles native XLM balance upserts and deletes.
func (s *tokenIngestionService) processNativeBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByAccountID map[string]types.AccountChange) error {
	if len(changesByAccountID) == 0 {
		return nil
	}

	var upserts []wbdata.NativeBalance
	var deletes []string
	for _, change := range changesByAccountID {
		if change.Operation == types.AccountOpRemove {
			deletes = append(deletes, change.AccountID)
		} else {
			upserts = append(upserts, wbdata.NativeBalance{
				AccountAddress:     change.AccountID,
				Balance:            change.Balance,
				MinimumBalance:     change.MinimumBalance,
				BuyingLiabilities:  change.BuyingLiabilities,
				SellingLiabilities: change.SellingLiabilities,
				LedgerNumber:       change.LedgerNumber,
			})
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.nativeBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting native balances: %w", err)
		}
	}
	log.Ctx(ctx).Infof("✅ upserted %d native balances, deleted %d native balances", len(upserts), len(deletes))
	return nil
}

// processSACBalanceChanges handles SAC balance upserts and deletes for contract addresses.
func (s *tokenIngestionService) processSACBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.SACBalance
	var deletes []wbdata.SACBalance
	for _, change := range changesByKey {
		contractID := wbdata.DeterministicContractID(change.ContractID)
		sacBal := wbdata.SACBalance{
			AccountAddress:    change.AccountID,
			ContractID:        contractID,
			Balance:           change.Balance,
			IsAuthorized:      change.IsAuthorized,
			IsClawbackEnabled: change.IsClawbackEnabled,
			LedgerNumber:      change.LedgerNumber,
		}
		if change.Operation == types.SACBalanceOpRemove {
			deletes = append(deletes, sacBal)
		} else {
			upserts = append(upserts, sacBal)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.sacBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
	}
	log.Ctx(ctx).Infof("✅ upserted %d SAC balances, deleted %d SAC balances", len(upserts), len(deletes))
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
			ID:     wbdata.DeterministicAssetID(assetCode, assetIssuer),
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
// For SAC contracts: returns contract metadata extracted from ledger data (no RPC needed).
// For non-SAC contracts: returns WASM hash for later validation and marks as skip.
func (s *tokenIngestionService) processContractInstanceChange(
	change ingest.Change,
	contractAddress string,
	contractDataEntry xdr.ContractDataEntry,
) (sacContract *wbdata.Contract, wasmHash *xdr.Hash, skip bool) {
	ledgerEntry := change.Post
	asset, isSAC := sac.AssetFromContractData(*ledgerEntry, s.networkPassphrase)
	if isSAC {
		// Extract metadata from ledger (code:issuer format for name, code for symbol)
		var assetType, code, issuer string
		err := asset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return nil, nil, true
		}
		name := code + ":" + issuer
		decimals := uint32(7) // Stellar assets always have 7 decimals
		return &wbdata.Contract{
			ID:         wbdata.DeterministicContractID(contractAddress),
			ContractID: contractAddress,
			Type:       string(types.ContractTypeSAC),
			Code:       &code,
			Issuer:     &issuer,
			Name:       &name,
			Symbol:     &code,
			Decimals:   decimals,
		}, nil, true
	}

	// For non-SAC contracts, extract WASM hash for later validation
	contractInstance := contractDataEntry.Val.MustInstance()
	if contractInstance.Executable.Type == xdr.ContractExecutableTypeContractExecutableWasm {
		if contractInstance.Executable.WasmHash != nil {
			hash := *contractInstance.Executable.WasmHash
			return nil, &hash, false
		}
	}

	return nil, nil, true
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
		contractTokensByHolderAddress: make(map[string][]uuid.UUID),
		contractIDsByWasmHash:         make(map[xdr.Hash][]string),
		contractTypesByWasmHash:       make(map[xdr.Hash]types.ContractType),
		uniqueAssets:                  make(map[uuid.UUID]*wbdata.TrustlineAsset),
		uniqueContractTokens:          make(map[uuid.UUID]*wbdata.Contract),
	}

	batch := newBatch(s.trustlineBalanceModel, s.nativeBalanceModel, s.sacBalanceModel)
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
			// Calculate minimum balance using same formula as live ingestion (accounts.go)
			numSubEntries := accountEntry.NumSubEntries
			numSponsoring := accountEntry.NumSponsoring()
			numSponsored := accountEntry.NumSponsored()
			// Calculate the minimum balance for base reserves: https://developers.stellar.org/docs/build/guides/transactions/sponsored-reserves#effect-on-minimum-balance
			minimumBalance := int64(processors.MinimumBaseReserveCount+numSubEntries+numSponsoring-numSponsored)*processors.BaseReserveStroops + int64(liabilities.Selling)
			batch.addNativeBalance(accountEntry.AccountId.Address(), int64(accountEntry.Balance), minimumBalance, int64(liabilities.Buying), int64(liabilities.Selling), checkpointLedger)
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
			if _, exists := data.uniqueAssets[asset.ID]; !exists {
				data.uniqueAssets[asset.ID] = &asset
			}

		case xdr.LedgerEntryTypeContractCode:
			contractCodeEntry := change.Post.Data.MustContractCode()
			contractType, err := s.contractValidator.ValidateFromContractCode(ctx, contractCodeEntry.Code)
			if err != nil {
				continue
			}
			data.contractTypesByWasmHash[contractCodeEntry.Hash] = contractType
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

				// For contract addresses (C...), try to extract SAC balance and stream directly to batch
				// C-addresses with SAC balances go to sac_balances table, not ContractsByHolderAddress
				if isContractHolderAddress(holderAddress) {
					balanceStr, authorized, clawback, err := s.extractSACBalanceFromValue(contractDataEntry.Val)
					if err == nil {
						// Stream SAC balance directly to batch (like trustlines)
						batch.addSACBalance(wbdata.SACBalance{
							AccountAddress:    holderAddress,
							ContractID:        wbdata.DeterministicContractID(contractAddressStr),
							Balance:           balanceStr,
							IsAuthorized:      authorized,
							IsClawbackEnabled: clawback,
							LedgerNumber:      checkpointLedger,
						})
						entries++
						continue
					}
				}

				// Non-SAC contract token balance - add to ContractsByHolderAddress for relationship tracking
				if _, ok := data.contractTokensByHolderAddress[holderAddress]; !ok {
					data.contractTokensByHolderAddress[holderAddress] = []uuid.UUID{}
				}
				data.contractTokensByHolderAddress[holderAddress] = append(data.contractTokensByHolderAddress[holderAddress], wbdata.DeterministicContractID(contractAddressStr))
				entries++

			case xdr.ScValTypeScvLedgerKeyContractInstance:
				sacContract, wasmHash, skip := s.processContractInstanceChange(change, contractAddressStr, contractDataEntry)
				if sacContract != nil {
					// Collect SAC contract metadata for batch insert
					if _, exists := data.uniqueContractTokens[sacContract.ID]; !exists {
						data.uniqueContractTokens[sacContract.ID] = sacContract
					}
					entries++
					continue
				}
				if skip {
					continue
				}
				// For non-SAC contracts with WASM hash, track for later validation
				data.contractIDsByWasmHash[*wasmHash] = append(data.contractIDsByWasmHash[*wasmHash], contractAddressStr)
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
			return checkpointData{}, fmt.Errorf("flushing final batch: %w", err)
		}
		batchCount++
	}

	log.Ctx(ctx).Infof("Processed %d entries (%d trustlines, %d accounts in %d batches) in %.2f minutes",
		entries, trustlineCount, accountCount, batchCount, time.Since(startTime).Minutes())
	return data, nil
}

// fetchSep41Metadata validates contract specs and enriches the contractTypesByContractID map with SEP-41 classifications.
func (s *tokenIngestionService) fetchSep41Metadata(
	ctx context.Context,
	contractIDsByWasmHash map[xdr.Hash][]string,
	contractTypesByWasmHash map[xdr.Hash]types.ContractType,
) ([]*wbdata.Contract, error) {
	sep41ContractIDs := make([]string, 0)
	for wasmHash, contractType := range contractTypesByWasmHash {
		if contractType != types.ContractTypeSEP41 {
			continue
		}

		sep41ContractIDs = append(sep41ContractIDs, contractIDsByWasmHash[wasmHash]...)
	}

	// Fetch metadata for SEP-41 contracts via RPC and store in database
	sep41ContractsWithMetadata := make([]*wbdata.Contract, 0)
	var err error
	if len(sep41ContractIDs) > 0 {
		sep41ContractsWithMetadata, err = s.contractMetadataService.FetchSep41Metadata(ctx, sep41ContractIDs)
		if err != nil {
			return nil, fmt.Errorf("fetching SEP-41 contract metadata: %w", err)
		}
	}

	return sep41ContractsWithMetadata, nil
}

// storeTokensInDB stores collected contract relationships into PostgreSQL.
func (s *tokenIngestionService) storeTokensInDB(
	ctx context.Context,
	dbTx pgx.Tx,
	contractTokensByAccountAddress map[string][]uuid.UUID,
	uniqueAssets map[uuid.UUID]*wbdata.TrustlineAsset,
	uniqueContractTokens map[uuid.UUID]*wbdata.Contract,
) error {
	if len(uniqueAssets) == 0 && len(uniqueContractTokens) == 0 {
		return nil
	}

	startTime := time.Now()

	// Batch insert trustline assets
	trustlineAssets := make([]wbdata.TrustlineAsset, 0, len(uniqueAssets))
	for _, asset := range uniqueAssets {
		trustlineAssets = append(trustlineAssets, *asset)
	}
	if len(trustlineAssets) > 0 {
		if err := s.trustlineAssetModel.BatchInsert(ctx, dbTx, trustlineAssets); err != nil {
			return fmt.Errorf("batch inserting trustline assets: %w", err)
		}
	}

	// Batch insert contract tokens
	contractTokens := make([]*wbdata.Contract, 0, len(uniqueContractTokens))
	for _, contract := range uniqueContractTokens {
		contractTokens = append(contractTokens, contract)
	}
	if len(contractTokens) > 0 {
		if err := s.contractModel.BatchInsert(ctx, dbTx, contractTokens); err != nil {
			return fmt.Errorf("batch inserting contracts: %w", err)
		}
	}

	// Batch insert account-contract relationships
	if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractTokensByAccountAddress); err != nil {
		return fmt.Errorf("batch inserting account contracts: %w", err)
	}
	log.Ctx(ctx).Infof("✅ Inserted %d trustline assets, %d contract tokens, %d account-contract relationships in %.2f minutes", len(uniqueAssets), len(uniqueContractTokens), len(contractTokensByAccountAddress), time.Since(startTime).Minutes())

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

// extractSACBalanceFromValue extracts balance, authorized, and clawback from a SAC balance map.
// SAC balance format: {amount: i128, authorized: bool, clawback: bool}
func (s *tokenIngestionService) extractSACBalanceFromValue(val xdr.ScVal) (balance string, authorized bool, clawback bool, err error) {
	if val.Type != xdr.ScValTypeScvMap {
		return "", false, false, fmt.Errorf("expected ScMap, got %v", val.Type)
	}

	balanceMap, ok := val.GetMap()
	if !ok || balanceMap == nil {
		return "", false, false, fmt.Errorf("failed to get balance map")
	}

	if len(*balanceMap) != 3 {
		return "", false, false, fmt.Errorf("expected 3 entries (amount, authorized, clawback), got %d", len(*balanceMap))
	}

	var amountFound, authorizedFound, clawbackFound bool

	for _, entry := range *balanceMap {
		if entry.Key.Type != xdr.ScValTypeScvSymbol {
			continue
		}

		keySymbol, ok := entry.Key.GetSym()
		if !ok {
			continue
		}

		switch string(keySymbol) {
		case "amount":
			if entry.Val.Type != xdr.ScValTypeScvI128 {
				return "", false, false, fmt.Errorf("amount is not i128")
			}
			i128Parts := entry.Val.MustI128()
			balance = amount.String128(i128Parts)
			amountFound = true

		case "authorized":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return "", false, false, fmt.Errorf("authorized is not bool")
			}
			authorized = entry.Val.MustB()
			authorizedFound = true

		case "clawback":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return "", false, false, fmt.Errorf("clawback is not bool")
			}
			clawback = entry.Val.MustB()
			clawbackFound = true
		}
	}

	if !amountFound || !authorizedFound || !clawbackFound {
		return "", false, false, fmt.Errorf("missing required fields in balance map")
	}

	return balance, authorized, clawback, nil
}

// isContractHolderAddress checks if the holder address is a contract address (C...).
func isContractHolderAddress(address string) bool {
	_, err := strkey.Decode(strkey.VersionByteContract, address)
	return err == nil
}
