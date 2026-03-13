package services

import (
	"context"
	"encoding/hex"
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
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// flushBatchSize is the number of entries to buffer before flushing to DB.
	flushBatchSize = 250_000
)

// CheckpointService orchestrates checkpoint population by coordinating
// token and WASM ingestion.
type CheckpointService interface {
	PopulateFromCheckpoint(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error
}

var _ CheckpointService = (*checkpointService)(nil)

// readerFactory creates a ChangeReader for a given checkpoint ledger.
type readerFactory func(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) (ingest.ChangeReader, error)

// defaultReaderFactory wraps ingest.NewCheckpointChangeReader to satisfy readerFactory.
func defaultReaderFactory(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) (ingest.ChangeReader, error) {
	reader, err := ingest.NewCheckpointChangeReader(ctx, archive, checkpointLedger)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	return reader, nil
}

// CheckpointServiceConfig holds configuration for creating a CheckpointService.
type CheckpointServiceConfig struct {
	DB                         db.ConnectionPool
	Archive                    historyarchive.ArchiveInterface
	ContractValidator          ContractValidator
	ContractMetadataService    ContractMetadataService
	TrustlineAssetModel        wbdata.TrustlineAssetModelInterface
	TrustlineBalanceModel      wbdata.TrustlineBalanceModelInterface
	NativeBalanceModel         wbdata.NativeBalanceModelInterface
	SACBalanceModel            wbdata.SACBalanceModelInterface
	AccountContractTokensModel wbdata.AccountContractTokensModelInterface
	ContractModel              wbdata.ContractModelInterface
	ProtocolWasmsModel         wbdata.ProtocolWasmsModelInterface
	ProtocolContractsModel     wbdata.ProtocolContractsModelInterface
	NetworkPassphrase          string
}

type checkpointService struct {
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
	protocolWasmModel          wbdata.ProtocolWasmsModelInterface
	protocolContractsModel     wbdata.ProtocolContractsModelInterface
	networkPassphrase          string
	readerFactory              readerFactory
}

// NewCheckpointService creates a CheckpointService.
func NewCheckpointService(cfg CheckpointServiceConfig) *checkpointService {
	return &checkpointService{
		db:                         cfg.DB,
		archive:                    cfg.Archive,
		contractValidator:          cfg.ContractValidator,
		contractMetadataService:    cfg.ContractMetadataService,
		trustlineAssetModel:        cfg.TrustlineAssetModel,
		trustlineBalanceModel:      cfg.TrustlineBalanceModel,
		nativeBalanceModel:         cfg.NativeBalanceModel,
		sacBalanceModel:            cfg.SACBalanceModel,
		accountContractTokensModel: cfg.AccountContractTokensModel,
		contractModel:              cfg.ContractModel,
		protocolWasmModel:          cfg.ProtocolWasmsModel,
		protocolContractsModel:     cfg.ProtocolContractsModel,
		networkPassphrase:          cfg.NetworkPassphrase,
		readerFactory:              defaultReaderFactory,
	}
}

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

func newCheckpointData() checkpointData {
	return checkpointData{
		contractTokensByHolderAddress: make(map[string][]uuid.UUID),
		contractIDsByWasmHash:         make(map[xdr.Hash][]string),
		contractTypesByWasmHash:       make(map[xdr.Hash]types.ContractType),
		uniqueAssets:                  make(map[uuid.UUID]*wbdata.TrustlineAsset),
		uniqueContractTokens:          make(map[uuid.UUID]*wbdata.Contract),
	}
}

// batch holds a batch of trustline balances, native balances, and SAC balances for streaming insertion.
type batch struct {
	trustlineBalances     []wbdata.TrustlineBalance
	nativeBalances        []wbdata.NativeBalance
	sacBalances           []wbdata.SACBalance
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel    wbdata.NativeBalanceModelInterface
	sacBalanceModel       wbdata.SACBalanceModelInterface
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
	if err := b.trustlineBalanceModel.BatchCopy(ctx, dbTx, b.trustlineBalances); err != nil {
		return fmt.Errorf("batch inserting trustline balances: %w", err)
	}
	if err := b.nativeBalanceModel.BatchCopy(ctx, dbTx, b.nativeBalances); err != nil {
		return fmt.Errorf("batch inserting native balances: %w", err)
	}
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

// checkpointProcessor holds per-invocation state for processing a checkpoint.
type checkpointProcessor struct {
	service                                           *checkpointService
	contractValidator                                 ContractValidator
	dbTx                                              pgx.Tx
	checkpointLedger                                  uint32
	data                                              checkpointData
	batch                                             *batch
	wasmHashes                                        map[xdr.Hash]struct{}
	protocolContractIDsByWasmHash                     map[xdr.Hash][]types.HashBytea
	entries, trustlineCount, accountCount, batchCount int
	startTime                                         time.Time
}

// PopulateFromCheckpoint performs initial cache population from Stellar history archive.
// It creates a checkpoint reader, iterates all entries in a single pass, then finalizes
// all data within a single DB transaction.
func (s *checkpointService) PopulateFromCheckpoint(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateFromCheckpoint requires archive connection")
	}

	defer func() {
		if err := s.contractValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	log.Ctx(ctx).Infof("Populating from checkpoint ledger = %d", checkpointLedger)

	reader, err := s.readerFactory(ctx, s.archive, checkpointLedger)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing checkpoint reader: %v", closeErr)
		}
	}()

	err = db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
			return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
		}

		proc := &checkpointProcessor{
			service:                       s,
			contractValidator:             s.contractValidator,
			dbTx:                          dbTx,
			checkpointLedger:              checkpointLedger,
			data:                          newCheckpointData(),
			batch:                         newBatch(s.trustlineBalanceModel, s.nativeBalanceModel, s.sacBalanceModel),
			wasmHashes:                    make(map[xdr.Hash]struct{}),
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
			startTime:                     time.Now(),
		}

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("checkpoint processing cancelled: %w", ctx.Err())
			default:
			}

			change, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return fmt.Errorf("reading checkpoint changes: %w", readErr)
			}

			if change.Type == xdr.LedgerEntryTypeContractCode {
				contractCodeEntry := change.Post.Data.MustContractCode()
				proc.processContractCode(ctx, contractCodeEntry.Hash, contractCodeEntry.Code)
			} else {
				proc.processEntry(change)
			}

			if txErr := proc.flushBatchIfNeeded(ctx); txErr != nil {
				return fmt.Errorf("flushing token batch: %w", txErr)
			}
		}

		if txErr := proc.flushRemainingBatch(ctx); txErr != nil {
			return fmt.Errorf("flushing remaining token batch: %w", txErr)
		}

		if txErr := proc.finalize(ctx, dbTx); txErr != nil {
			return fmt.Errorf("finalizing checkpoint processor: %w", txErr)
		}

		if txErr := initializeCursors(dbTx); txErr != nil {
			return fmt.Errorf("initializing cursors: %w", txErr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("running db transaction for checkpoint population: %w", err)
	}
	return nil
}

// processEntry handles Account, Trustline, and ContractData entries from a checkpoint.
func (p *checkpointProcessor) processEntry(change ingest.Change) {
	//exhaustive:ignore
	switch change.Type {
	case xdr.LedgerEntryTypeAccount:
		accountEntry := change.Post.Data.MustAccount()
		liabilities := accountEntry.Liabilities()
		numSubEntries := accountEntry.NumSubEntries
		numSponsoring := accountEntry.NumSponsoring()
		numSponsored := accountEntry.NumSponsored()
		minimumBalance := int64(processors.MinimumBaseReserveCount+numSubEntries+numSponsoring-numSponsored)*processors.BaseReserveStroops + int64(liabilities.Selling)
		p.batch.addNativeBalance(accountEntry.AccountId.Address(), int64(accountEntry.Balance), minimumBalance, int64(liabilities.Buying), int64(liabilities.Selling), p.checkpointLedger)
		p.entries++
		p.accountCount++

	case xdr.LedgerEntryTypeTrustline:
		accountAddress, asset, xdrFields, skip := p.service.processTrustlineChange(change)
		if skip {
			return
		}
		p.entries++
		p.trustlineCount++
		p.batch.addTrustline(accountAddress, asset, xdrFields.Balance, xdrFields.Limit, xdrFields.BuyingLiabilities, xdrFields.SellingLiabilities, xdrFields.Flags, p.checkpointLedger)
		if _, exists := p.data.uniqueAssets[asset.ID]; !exists {
			p.data.uniqueAssets[asset.ID] = &asset
		}

	case xdr.LedgerEntryTypeContractData:
		contractDataEntry := change.Post.Data.MustContractData()

		contractAddress, ok := contractDataEntry.Contract.GetContractId()
		if !ok {
			return
		}
		contractAddressStr := strkey.MustEncode(strkey.VersionByteContract, contractAddress[:])

		//exhaustive:ignore
		if contractDataEntry.Key.Type == xdr.ScValTypeScvLedgerKeyContractInstance {
			result := p.service.processContractInstanceChange(change, contractAddressStr, contractDataEntry)
			if result.Skip {
				return
			}
			p.data.uniqueContractTokens[result.Contract.ID] = result.Contract
			p.entries++

			// Track contract-to-WASM mapping for protocol contracts
			p.processWasmContractData(contractDataEntry, xdr.Hash(contractAddress))

			if result.IsSAC {
				return
			}
			p.data.contractIDsByWasmHash[*result.WasmHash] = append(p.data.contractIDsByWasmHash[*result.WasmHash], contractAddressStr)
			p.entries++
		} else {
			holderAddress, skip := p.service.processContractBalanceChange(contractDataEntry)
			if skip {
				return
			}

			_, _, ok := sac.ContractBalanceFromContractData(*change.Post, p.service.networkPassphrase)
			if ok {
				contractUUID := wbdata.DeterministicContractID(contractAddressStr)
				if _, exists := p.data.uniqueContractTokens[contractUUID]; !exists {
					p.data.uniqueContractTokens[contractUUID] = &wbdata.Contract{
						ID:         contractUUID,
						ContractID: contractAddressStr,
						Type:       string(types.ContractTypeSAC),
					}
				}

				balanceStr, authorized, clawback := p.service.extractSACBalanceFields(contractDataEntry.Val)
				p.batch.addSACBalance(wbdata.SACBalance{
					AccountAddress:    holderAddress,
					ContractID:        contractUUID,
					Balance:           balanceStr,
					IsAuthorized:      authorized,
					IsClawbackEnabled: clawback,
					LedgerNumber:      p.checkpointLedger,
				})
				p.entries++
				return
			}

			if _, ok := p.data.contractTokensByHolderAddress[holderAddress]; !ok {
				p.data.contractTokensByHolderAddress[holderAddress] = []uuid.UUID{}
			}
			p.data.contractTokensByHolderAddress[holderAddress] = append(p.data.contractTokensByHolderAddress[holderAddress], wbdata.DeterministicContractID(contractAddressStr))
			p.entries++
		}
	}
}

// processContractCode handles both SEP-41 validation AND WASM hash tracking.
func (p *checkpointProcessor) processContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) {
	// Track hash for protocol_wasms persistence
	p.wasmHashes[wasmHash] = struct{}{}

	// Validate against SEP-41 spec
	contractType, err := p.contractValidator.ValidateFromContractCode(ctx, wasmCode)
	if err != nil {
		return // intentionally skip invalid entries
	}
	p.data.contractTypesByWasmHash[wasmHash] = contractType
	p.entries++
}

// processWasmContractData extracts contract-to-WASM-hash mappings from ContractData Instance entries.
func (p *checkpointProcessor) processWasmContractData(contractDataEntry xdr.ContractDataEntry, contractAddress xdr.Hash) {
	contractInstance := contractDataEntry.Val.MustInstance()
	if contractInstance.Executable.Type != xdr.ContractExecutableTypeContractExecutableWasm {
		return
	}
	if contractInstance.Executable.WasmHash == nil {
		return
	}

	hash := *contractInstance.Executable.WasmHash
	p.protocolContractIDsByWasmHash[hash] = append(p.protocolContractIDsByWasmHash[hash], types.HashBytea(hex.EncodeToString(contractAddress[:])))
}

// flushBatchIfNeeded flushes the batch to DB if it has reached the flush size.
func (p *checkpointProcessor) flushBatchIfNeeded(ctx context.Context) error {
	if p.batch.count() >= flushBatchSize {
		if err := p.batch.flush(ctx, p.dbTx); err != nil {
			return fmt.Errorf("flushing batch: %w", err)
		}
		p.batchCount++
		log.Ctx(ctx).Infof("Flushed batch %d (%d entries so far)", p.batchCount, p.entries)
		p.batch.reset()
	}
	return nil
}

// flushRemainingBatch flushes any remaining data in the batch.
func (p *checkpointProcessor) flushRemainingBatch(ctx context.Context) error {
	if p.batch.count() > 0 {
		if err := p.batch.flush(ctx, p.dbTx); err != nil {
			return fmt.Errorf("flushing final batch: %w", err)
		}
		p.batchCount++
	}
	log.Ctx(ctx).Infof("Processed %d entries (%d trustlines, %d accounts in %d batches) in %.2f minutes",
		p.entries, p.trustlineCount, p.accountCount, p.batchCount, time.Since(p.startTime).Minutes())
	return nil
}

// finalize identifies SEP-41 contracts, fetches metadata, stores tokens in DB,
// and persists protocol WASMs and contracts.
func (p *checkpointProcessor) finalize(ctx context.Context, dbTx pgx.Tx) error {
	// Identify SAC contracts missing code/issuer and fetch metadata via RPC
	var sacContractsNeedingMetadata []string
	for _, contract := range p.data.uniqueContractTokens {
		if contract.Type == string(types.ContractTypeSAC) && contract.Code == nil {
			sacContractsNeedingMetadata = append(sacContractsNeedingMetadata, contract.ContractID)
		}
	}
	if len(sacContractsNeedingMetadata) > 0 {
		sacContracts, err := p.service.contractMetadataService.FetchSACMetadata(ctx, sacContractsNeedingMetadata)
		if err != nil {
			return fmt.Errorf("fetching SAC metadata: %w", err)
		}
		for _, contract := range sacContracts {
			p.data.uniqueContractTokens[contract.ID] = contract
		}
	}

	// Extract contract spec from WASM hash and validate SEP-41 contracts
	sep41Tokens, err := p.service.fetchSep41Metadata(ctx, p.data.contractIDsByWasmHash, p.data.contractTypesByWasmHash)
	if err != nil {
		return fmt.Errorf("fetching SEP-41 token metadata: %w", err)
	}
	for _, token := range sep41Tokens {
		p.data.uniqueContractTokens[token.ID] = token
	}
	sep41TokensByAccountAddress := make(map[string][]uuid.UUID)
	for address, contractTokens := range p.data.contractTokensByHolderAddress {
		for _, token := range contractTokens {
			if _, exists := p.data.uniqueContractTokens[token]; exists && p.data.uniqueContractTokens[token].Type == string(types.ContractTypeSEP41) {
				if _, exists := sep41TokensByAccountAddress[address]; !exists {
					sep41TokensByAccountAddress[address] = make([]uuid.UUID, 0)
				}
				sep41TokensByAccountAddress[address] = append(sep41TokensByAccountAddress[address], token)
			}
		}
	}

	// Store SEP-41 contract relationships using deterministic IDs
	if err := p.service.storeTokensInDB(ctx, dbTx, sep41TokensByAccountAddress, p.data.uniqueAssets, p.data.uniqueContractTokens); err != nil {
		return fmt.Errorf("storing SEP-41 tokens in postgres: %w", err)
	}

	// Persist protocol WASMs
	if err := p.service.persistProtocolWasms(ctx, dbTx, p.wasmHashes); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	// Persist protocol contracts (must come after WASMs due to FK)
	if err := p.service.persistProtocolContracts(ctx, dbTx, p.wasmHashes, p.protocolContractIDsByWasmHash); err != nil {
		return fmt.Errorf("persisting protocol contracts: %w", err)
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
func (s *checkpointService) processTrustlineChange(change ingest.Change) (string, wbdata.TrustlineAsset, trustlineXDRFields, bool) {
	trustlineEntry := change.Post.Data.MustTrustLine()
	accountAddress := trustlineEntry.AccountId.Address()
	asset := trustlineEntry.Asset

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
func (s *checkpointService) processContractBalanceChange(contractDataEntry xdr.ContractDataEntry) (holderAddress string, skip bool) {
	var err error
	holderAddress, err = s.extractHolderAddress(contractDataEntry.Key)
	if err != nil {
		return "", true
	}
	return holderAddress, false
}

// contractInstanceResult holds the result of processing a contract instance entry.
type contractInstanceResult struct {
	Contract *wbdata.Contract
	WasmHash *xdr.Hash
	IsSAC    bool
	Skip     bool
}

// processContractInstanceChange extracts contract type information from a contract instance entry.
func (s *checkpointService) processContractInstanceChange(
	change ingest.Change,
	contractAddress string,
	contractDataEntry xdr.ContractDataEntry,
) contractInstanceResult {
	ledgerEntry := change.Post
	asset, isSAC := sac.AssetFromContractData(*ledgerEntry, s.networkPassphrase)
	if isSAC {
		var assetType, code, issuer string
		err := asset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return contractInstanceResult{Skip: true}
		}
		name := code + ":" + issuer
		decimals := uint32(7)
		return contractInstanceResult{
			Contract: &wbdata.Contract{
				ID:         wbdata.DeterministicContractID(contractAddress),
				ContractID: contractAddress,
				Type:       string(types.ContractTypeSAC),
				Code:       &code,
				Issuer:     &issuer,
				Name:       &name,
				Symbol:     &code,
				Decimals:   decimals,
			},
			IsSAC: true,
		}
	}

	contractInstance := contractDataEntry.Val.MustInstance()
	if contractInstance.Executable.Type == xdr.ContractExecutableTypeContractExecutableWasm {
		if contractInstance.Executable.WasmHash != nil {
			hash := *contractInstance.Executable.WasmHash
			return contractInstanceResult{
				Contract: &wbdata.Contract{
					ID:         wbdata.DeterministicContractID(contractAddress),
					ContractID: contractAddress,
					Type:       string(types.ContractTypeUnknown),
				},
				WasmHash: &hash,
			}
		}
	}

	return contractInstanceResult{Skip: true}
}

// fetchSep41Metadata validates contract specs and enriches with SEP-41 classifications.
func (s *checkpointService) fetchSep41Metadata(
	ctx context.Context,
	contractIDsByWasmHash map[xdr.Hash][]string,
	contractTypesByWasmHash map[xdr.Hash]types.ContractType,
) ([]*wbdata.Contract, error) {
	var sep41ContractIDs []string
	for wasmHash, contractType := range contractTypesByWasmHash {
		if contractType != types.ContractTypeSEP41 {
			continue
		}
		sep41ContractIDs = append(sep41ContractIDs, contractIDsByWasmHash[wasmHash]...)
	}

	var sep41ContractsWithMetadata []*wbdata.Contract
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
func (s *checkpointService) storeTokensInDB(
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

	trustlineAssets := make([]wbdata.TrustlineAsset, 0, len(uniqueAssets))
	for _, asset := range uniqueAssets {
		trustlineAssets = append(trustlineAssets, *asset)
	}
	if len(trustlineAssets) > 0 {
		if err := s.trustlineAssetModel.BatchInsert(ctx, dbTx, trustlineAssets); err != nil {
			return fmt.Errorf("batch inserting trustline assets: %w", err)
		}
	}

	contractTokens := make([]*wbdata.Contract, 0, len(uniqueContractTokens))
	for _, contract := range uniqueContractTokens {
		contractTokens = append(contractTokens, contract)
	}
	if len(contractTokens) > 0 {
		if err := s.contractModel.BatchInsert(ctx, dbTx, contractTokens); err != nil {
			return fmt.Errorf("batch inserting contracts: %w", err)
		}
	}

	if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractTokensByAccountAddress); err != nil {
		return fmt.Errorf("batch inserting account contracts: %w", err)
	}
	log.Ctx(ctx).Infof("Inserted %d trustline assets, %d contract tokens, %d account-contract relationships in %.2f minutes", len(uniqueAssets), len(uniqueContractTokens), len(contractTokensByAccountAddress), time.Since(startTime).Minutes())

	return nil
}

// extractHolderAddress extracts the account address from a contract balance entry key.
func (s *checkpointService) extractHolderAddress(key xdr.ScVal) (string, error) {
	keyVecPtr, ok := key.GetVec()
	if !ok || keyVecPtr == nil {
		return "", fmt.Errorf("key is not a vector")
	}
	keyVec := *keyVecPtr

	if len(keyVec) != 2 {
		return "", fmt.Errorf("key vector length is %d, expected 2", len(keyVec))
	}

	sym, ok := keyVec[0].GetSym()
	if !ok || sym != "Balance" {
		return "", fmt.Errorf("first element is not 'Balance' symbol")
	}

	scAddress, ok := keyVec[1].GetAddress()
	if !ok {
		return "", fmt.Errorf("second element is not a valid address")
	}

	holderAddress, err := scAddress.String()
	if err != nil {
		return "", fmt.Errorf("converting address to string: %w", err)
	}

	return holderAddress, nil
}

// extractSACBalanceFields extracts balance, authorized, and clawback from a SAC balance map.
func (s *checkpointService) extractSACBalanceFields(val xdr.ScVal) (balance string, authorized bool, clawback bool) {
	balanceMap, ok := val.GetMap()
	if !ok || balanceMap == nil {
		return "0", false, false
	}

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
			if entry.Val.Type == xdr.ScValTypeScvI128 {
				i128Parts := entry.Val.MustI128()
				balance = amount.String128(i128Parts)
			}

		case "authorized":
			if entry.Val.Type == xdr.ScValTypeScvBool {
				authorized = entry.Val.MustB()
			}

		case "clawback":
			if entry.Val.Type == xdr.ScValTypeScvBool {
				clawback = entry.Val.MustB()
			}
		}
	}

	return balance, authorized, clawback
}

// persistProtocolWasms writes all accumulated WASM hashes to the protocol_wasms table.
func (s *checkpointService) persistProtocolWasms(ctx context.Context, dbTx pgx.Tx, wasmHashes map[xdr.Hash]struct{}) error {
	if len(wasmHashes) == 0 {
		return nil
	}

	wasms := make([]wbdata.ProtocolWasms, 0, len(wasmHashes))
	for hash := range wasmHashes {
		wasms = append(wasms, wbdata.ProtocolWasms{
			WasmHash:   types.HashBytea(hex.EncodeToString(hash[:])),
			ProtocolID: nil,
		})
	}

	if err := s.protocolWasmModel.BatchInsert(ctx, dbTx, wasms); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol WASM hashes", len(wasms))
	return nil
}

// persistProtocolContracts writes all accumulated contract-to-WASM mappings to the protocol_contracts table.
// Contracts referencing WASM hashes not present in wasmHashes are skipped (e.g., expired/evicted WASMs).
func (s *checkpointService) persistProtocolContracts(ctx context.Context, dbTx pgx.Tx, wasmHashes map[xdr.Hash]struct{}, contractIDsByWasmHash map[xdr.Hash][]types.HashBytea) error {
	if len(contractIDsByWasmHash) == 0 {
		return nil
	}

	var contracts []wbdata.ProtocolContracts
	var skipped int
	for hash, contractIDs := range contractIDsByWasmHash {
		if _, exists := wasmHashes[hash]; !exists {
			skipped += len(contractIDs)
			continue
		}
		for _, contractID := range contractIDs {
			contracts = append(contracts, wbdata.ProtocolContracts{
				ContractID: contractID,
				WasmHash:   types.HashBytea(hex.EncodeToString(hash[:])),
			})
		}
	}
	if skipped > 0 {
		log.Ctx(ctx).Infof("Skipped %d protocol contracts referencing missing WASM hashes (expired/evicted)", skipped)
	}

	if err := s.protocolContractsModel.BatchInsert(ctx, dbTx, contracts); err != nil {
		return fmt.Errorf("persisting protocol contracts: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol contracts", len(contracts))
	return nil
}
