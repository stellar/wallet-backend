package services

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	"github.com/stellar/wallet-backend/internal/metrics"
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

// hotArchiveIterFactory yields a checkpoint's archived entries: persistent Soroban
// entries evicted from the live bucket list under protocol-23 state archival.
// The live checkpoint reader never sees them.
type hotArchiveIterFactory func(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) iter.Seq2[xdr.LedgerEntry, error]

// defaultHotArchiveIterFactory streams the checkpoint's hot-archive bucket list via
// ingest.NewHotArchiveIterator.
//
// Checkpoints older than protocol 23 (HAS version < 2) have no hot-archive bucket
// list and the SDK iterator fails on the absent bucket hashes, so they yield
// nothing here instead.
//
// The SDK iterator is returned unwrapped on purpose: a pass-through yield loop
// would break out of the SDK sequence whenever the consumer stops, and that
// deadlocks (see processHotArchive).
func defaultHotArchiveIterFactory(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) iter.Seq2[xdr.LedgerEntry, error] {
	has, err := archive.GetCheckpointHAS(checkpointLedger)
	if err != nil {
		return func(yield func(xdr.LedgerEntry, error) bool) {
			yield(xdr.LedgerEntry{}, fmt.Errorf("getting checkpoint HAS for hot archive at ledger %d: %w", checkpointLedger, err))
		}
	}
	if has.Version < historyarchive.HistoryArchiveStateVersionForProtocol23 {
		log.Ctx(ctx).Infof("Checkpoint ledger %d predates protocol 23 (HAS version %d); no hot archive to read", checkpointLedger, has.Version)
		return func(yield func(xdr.LedgerEntry, error) bool) {}
	}
	return ingest.NewHotArchiveIterator(ctx, archive, checkpointLedger)
}

// CheckpointServiceConfig holds configuration for creating a CheckpointService.
type CheckpointServiceConfig struct {
	DB                        *pgxpool.Pool
	Archive                   historyarchive.ArchiveInterface
	TrustlineAssetModel       wbdata.TrustlineAssetModelInterface
	TrustlineBalanceModel     wbdata.TrustlineBalanceModelInterface
	NativeBalanceModel        wbdata.NativeBalanceModelInterface
	SACBalanceModel           wbdata.SACBalanceModelInterface
	LiquidityPoolModel        wbdata.LiquidityPoolModelInterface
	LiquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface
	ContractModel             wbdata.ContractModelInterface
	ProtocolWasmsModel        wbdata.ProtocolWasmsModelInterface
	ProtocolContractsModel    wbdata.ProtocolContractsModelInterface
	NetworkPassphrase         string
	MetricsService            *metrics.IngestionMetrics
}

type checkpointService struct {
	db                        *pgxpool.Pool
	archive                   historyarchive.ArchiveInterface
	trustlineAssetModel       wbdata.TrustlineAssetModelInterface
	trustlineBalanceModel     wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel        wbdata.NativeBalanceModelInterface
	sacBalanceModel           wbdata.SACBalanceModelInterface
	liquidityPoolModel        wbdata.LiquidityPoolModelInterface
	liquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface
	contractModel             wbdata.ContractModelInterface
	protocolWasmModel         wbdata.ProtocolWasmsModelInterface
	protocolContractsModel    wbdata.ProtocolContractsModelInterface
	networkPassphrase         string
	metricsService            *metrics.IngestionMetrics
	readerFactory             readerFactory
	hotArchiveIterFactory     hotArchiveIterFactory
}

// NewCheckpointService creates a CheckpointService.
func NewCheckpointService(cfg CheckpointServiceConfig) *checkpointService {
	return &checkpointService{
		db:                        cfg.DB,
		archive:                   cfg.Archive,
		trustlineAssetModel:       cfg.TrustlineAssetModel,
		trustlineBalanceModel:     cfg.TrustlineBalanceModel,
		nativeBalanceModel:        cfg.NativeBalanceModel,
		sacBalanceModel:           cfg.SACBalanceModel,
		liquidityPoolModel:        cfg.LiquidityPoolModel,
		liquidityPoolBalanceModel: cfg.LiquidityPoolBalanceModel,
		contractModel:             cfg.ContractModel,
		protocolWasmModel:         cfg.ProtocolWasmsModel,
		protocolContractsModel:    cfg.ProtocolContractsModel,
		networkPassphrase:         cfg.NetworkPassphrase,
		metricsService:            cfg.MetricsService,
		readerFactory:             defaultReaderFactory,
		hotArchiveIterFactory:     defaultHotArchiveIterFactory,
	}
}

// checkpointData holds all data collected from processing a checkpoint ledger.
// Note: Trustlines, native balances, and SAC balances are streamed directly to DB in batches.
type checkpointData struct {
	// uniqueAssets stores unique asset metadata extracted from ledger (no RPC needed)
	uniqueAssets map[uuid.UUID]*wbdata.TrustlineAsset
	// uniqueContractTokens stores unique contract metadata extracted from ledger (no RPC needed)
	uniqueContractTokens map[uuid.UUID]*wbdata.Contract
}

func newCheckpointData() checkpointData {
	return checkpointData{
		uniqueAssets:         make(map[uuid.UUID]*wbdata.TrustlineAsset),
		uniqueContractTokens: make(map[uuid.UUID]*wbdata.Contract),
	}
}

// batch holds a batch of trustline, native, SAC, and liquidity-pool balances for streaming insertion.
type batch struct {
	trustlineBalances         []wbdata.TrustlineBalance
	nativeBalances            []wbdata.NativeBalance
	liquidityPools            []wbdata.LiquidityPool
	liquidityPoolBalances     []wbdata.LiquidityPoolBalance
	trustlineBalanceModel     wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel        wbdata.NativeBalanceModelInterface
	liquidityPoolModel        wbdata.LiquidityPoolModelInterface
	liquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface
}

func newBatch(
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface,
	nativeBalanceModel wbdata.NativeBalanceModelInterface,
	liquidityPoolModel wbdata.LiquidityPoolModelInterface,
	liquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface,
) *batch {
	return &batch{
		trustlineBalances:         make([]wbdata.TrustlineBalance, 0, flushBatchSize),
		nativeBalances:            make([]wbdata.NativeBalance, 0, flushBatchSize),
		liquidityPools:            make([]wbdata.LiquidityPool, 0, flushBatchSize),
		liquidityPoolBalances:     make([]wbdata.LiquidityPoolBalance, 0, flushBatchSize),
		trustlineBalanceModel:     trustlineBalanceModel,
		nativeBalanceModel:        nativeBalanceModel,
		liquidityPoolModel:        liquidityPoolModel,
		liquidityPoolBalanceModel: liquidityPoolBalanceModel,
	}
}

func (b *batch) addTrustline(accountAddress string, asset wbdata.TrustlineAsset, balance, limit, buyingLiabilities, sellingLiabilities int64, flags uint32, ledger uint32) {
	b.trustlineBalances = append(b.trustlineBalances, wbdata.TrustlineBalance{
		AccountID:          types.AddressBytea(accountAddress),
		AssetID:            wbdata.DeterministicAssetID(asset.Code, asset.Issuer),
		Balance:            balance,
		Limit:              limit,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		Flags:              flags,
		LedgerNumber:       ledger,
	})
}

func (b *batch) addNativeBalance(accountAddress string, balance, minimumBalance, buyingLiabilities, sellingLiabilities int64, numSubentries, ledger uint32) {
	b.nativeBalances = append(b.nativeBalances, wbdata.NativeBalance{
		AccountID:          types.AddressBytea(accountAddress),
		Balance:            balance,
		MinimumBalance:     minimumBalance,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,
		NumSubEntries:      numSubentries,
		LedgerNumber:       ledger,
	})
}

func (b *batch) addLiquidityPool(pool wbdata.LiquidityPool) {
	b.liquidityPools = append(b.liquidityPools, pool)
}

func (b *batch) addLiquidityPoolShare(accountAddress string, poolID string, shares int64, ledger uint32) {
	b.liquidityPoolBalances = append(b.liquidityPoolBalances, wbdata.LiquidityPoolBalance{
		AccountID:    types.AddressBytea(accountAddress),
		PoolID:       poolID,
		Shares:       shares,
		LedgerNumber: ledger,
	})
}

// flush inserts the batch's data into DB.
func (b *batch) flush(ctx context.Context, dbTx pgx.Tx) error {
	if err := b.trustlineBalanceModel.BatchCopy(ctx, dbTx, b.trustlineBalances); err != nil {
		return fmt.Errorf("batch inserting trustline balances: %w", err)
	}
	if err := b.nativeBalanceModel.BatchCopy(ctx, dbTx, b.nativeBalances); err != nil {
		return fmt.Errorf("batch inserting native balances: %w", err)
	}
	if err := b.liquidityPoolModel.BatchCopy(ctx, dbTx, b.liquidityPools); err != nil {
		return fmt.Errorf("batch inserting liquidity pools: %w", err)
	}
	if err := b.liquidityPoolBalanceModel.BatchCopy(ctx, dbTx, b.liquidityPoolBalances); err != nil {
		return fmt.Errorf("batch inserting liquidity pool balances: %w", err)
	}
	return nil
}

func (b *batch) count() int {
	return len(b.trustlineBalances) + len(b.nativeBalances) +
		len(b.liquidityPools) + len(b.liquidityPoolBalances)
}

func (b *batch) reset() {
	b.trustlineBalances = b.trustlineBalances[:0]
	b.nativeBalances = b.nativeBalances[:0]
	b.liquidityPools = b.liquidityPools[:0]
	b.liquidityPoolBalances = b.liquidityPoolBalances[:0]
}

// checkpointProcessor holds per-invocation state for processing a checkpoint.
type checkpointProcessor struct {
	service                                           *checkpointService
	dbTx                                              pgx.Tx
	checkpointLedger                                  uint32
	data                                              checkpointData
	batch                                             *batch
	wasmClassifications                               map[xdr.Hash]types.ContractType
	contractAddressesByWasmHash                       map[xdr.Hash][]xdr.Hash
	entries, trustlineCount, accountCount, batchCount int
	startTime                                         time.Time
	// pendingSACBalances holds SAC-shaped balance entries seen during the scan. They
	// are not written to the batch during the pass because a balance entry's shape does
	// not by itself identify its contract as a SAC. finalize keeps only those whose
	// contract was confirmed as a SAC via its instance entry (uniqueContractTokens with
	// type=SAC), then copies them in — before the deferred fk_contract_token is checked
	// at COMMIT.
	pendingSACBalances []wbdata.SACBalance
	// pendingPoolShares holds the pool-share trustlines read so far, and
	// skippedPoolIDs the pools processEntry did not capture. A share's pool
	// entry can arrive in any bucket, so whether its pool was captured is
	// unknown when the trustline is read: the shares join the batch in
	// flushRemainingBatch, once every pool entry has been seen, minus those
	// whose pool was skipped. liquidity_pool_balances references
	// liquidity_pools, so capturing a skipped pool's shares would abort the
	// load's transaction at commit. Pool-share trustlines are a small
	// fraction of a checkpoint, so holding them costs far less than a batch.
	pendingPoolShares []pendingPoolShare
	skippedPoolIDs    map[string]struct{}
}

// pendingPoolShare is one account's shares in a pool, held until every pool
// entry in the checkpoint has been read.
type pendingPoolShare struct {
	accountAddress string
	poolID         string
	shares         int64
}

// PopulateFromCheckpoint performs initial cache population from Stellar history archive.
// It streams all live-bucket-list entries, then the hot-archive (archived) entries, and
// finalizes all data within a single DB transaction.
func (s *checkpointService) PopulateFromCheckpoint(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateFromCheckpoint requires archive connection")
	}

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

	var proc *checkpointProcessor
	err = db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
			return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
		}
		// The connection sits idle-in-transaction between batch flushes while the
		// next 250k entries are decoded from the history archive stream. An
		// instance-level idle_in_transaction_session_timeout (set in production to
		// protect the vacuum horizon from abandoned transactions) would kill this
		// legitimately long-lived load mid-way and force a full redo, so exempt
		// this transaction; SET LOCAL scopes the exemption to it alone.
		if _, txErr := dbTx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout = 0"); txErr != nil {
			return fmt.Errorf("setting idle_in_transaction_session_timeout=0: %w", txErr)
		}

		proc = &checkpointProcessor{
			service:                     s,
			dbTx:                        dbTx,
			checkpointLedger:            checkpointLedger,
			data:                        newCheckpointData(),
			batch:                       newBatch(s.trustlineBalanceModel, s.nativeBalanceModel, s.liquidityPoolModel, s.liquidityPoolBalanceModel),
			wasmClassifications:         make(map[xdr.Hash]types.ContractType),
			contractAddressesByWasmHash: make(map[xdr.Hash][]xdr.Hash),
			skippedPoolIDs:              make(map[string]struct{}),
			startTime:                   time.Now(),
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

		// Must run before finalize: archived wasm hashes have to be in
		// wasmClassifications before persistProtocolWasms/persistProtocolContracts
		// write their rows. Safe after the final flush: this pass only fills
		// in-memory maps, never batch rows.
		if txErr := proc.processHotArchive(ctx); txErr != nil {
			return fmt.Errorf("processing hot archive entries: %w", txErr)
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
		minimumBalance := processors.MinimumBalance(accountEntry)
		p.batch.addNativeBalance(accountEntry.AccountId.Address(), int64(accountEntry.Balance), minimumBalance, int64(liabilities.Buying), int64(liabilities.Selling), uint32(accountEntry.NumSubEntries), p.checkpointLedger)
		p.entries++
		p.accountCount++

	case xdr.LedgerEntryTypeTrustline:
		// Pool-share trustlines carry an account's liquidity-pool shares rather than an asset
		// balance; route them to liquidity_pool_balances instead of trustline_balances.
		trustlineEntry := change.Post.Data.MustTrustLine()
		if trustlineEntry.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			p.pendingPoolShares = append(p.pendingPoolShares, pendingPoolShare{
				accountAddress: trustlineEntry.AccountId.Address(),
				poolID:         processors.PoolIDToString(*trustlineEntry.Asset.LiquidityPoolId),
				shares:         int64(trustlineEntry.Balance),
			})
			p.entries++
			return
		}

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

	case xdr.LedgerEntryTypeLiquidityPool:
		pool := change.Post.Data.MustLiquidityPool()
		cp, ok := pool.Body.GetConstantProduct()
		if !ok {
			// liquidity_pools holds constant-product reserves; a pool body it
			// cannot represent is skipped here, and flushRemainingBatch drops
			// that pool's shares with it.
			p.skippedPoolIDs[processors.PoolIDToString(pool.LiquidityPoolId)] = struct{}{}
			return
		}
		p.batch.addLiquidityPool(wbdata.LiquidityPool{
			PoolID:       processors.PoolIDToString(pool.LiquidityPoolId),
			AssetA:       cp.Params.AssetA.StringCanonical(),
			AmountA:      int64(cp.ReserveA),
			AssetB:       cp.Params.AssetB.StringCanonical(),
			AmountB:      int64(cp.ReserveB),
			LedgerNumber: p.checkpointLedger,
		})
		p.entries++

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
			if result.WasmHash != nil {
				p.contractAddressesByWasmHash[*result.WasmHash] = append(
					p.contractAddressesByWasmHash[*result.WasmHash], xdr.Hash(contractAddress))
			}

			if result.IsSAC {
				return
			}
		} else {
			holderAddress, skip := p.service.processContractBalanceChange(contractDataEntry)
			if skip {
				return
			}

			_, _, ok := sac.ContractBalanceFromContractData(*change.Post, p.service.networkPassphrase)
			if ok {
				// Shape matches a SAC balance, but that alone does not identify the
				// contract as a SAC. Defer the balance to finalize, which keeps it
				// only if the contract was confirmed via its instance entry. Do NOT
				// create a contract_tokens row from the balance shape: the instance
				// entry is the authoritative source of a contract's type and metadata.
				contractUUID := wbdata.DeterministicContractID(contractAddressStr)
				balanceStr, authorized, clawback := p.service.extractSACBalanceFields(contractDataEntry.Val)
				p.pendingSACBalances = append(p.pendingSACBalances, wbdata.SACBalance{
					AccountID:         types.AddressBytea(holderAddress),
					ContractID:        contractUUID,
					Balance:           balanceStr,
					IsAuthorized:      authorized,
					IsClawbackEnabled: clawback,
					LedgerNumber:      p.checkpointLedger,
				})
				p.entries++
			}
		}
	}
}

// processContractCode tracks WASM hashes for protocol_wasms persistence.
func (p *checkpointProcessor) processContractCode(_ context.Context, wasmHash xdr.Hash, _ []byte) {
	p.wasmClassifications[wasmHash] = types.ContractTypeUnknown
}

// processHotArchive streams the checkpoint's archived entries so evicted contract
// code and contract instances still reach protocol_wasms, protocol_contracts, and
// contract_tokens.
//
// Skipped on purpose: archived balance entries and archived SAC instances. Live
// ingestion recreates their rows when the entries are restored.
func (p *checkpointProcessor) processHotArchive(ctx context.Context) error {
	var codeCount, instanceCount int
	for entry, iterErr := range p.service.hotArchiveIterFactory(ctx, p.service.archive, p.checkpointLedger) {
		if iterErr != nil {
			// This error return must stay the loop's ONLY early exit. Any other break
			// blocks forever: the SDK iterator's producer goroutine only stops via its
			// context, and the iterator's internal WaitGroup unwinds before any
			// deferred cancel here could fire.
			return fmt.Errorf("reading hot archive entries: %w", iterErr)
		}
		//exhaustive:ignore
		switch entry.Data.Type {
		case xdr.LedgerEntryTypeContractCode:
			contractCodeEntry := entry.Data.MustContractCode()
			p.processContractCode(ctx, contractCodeEntry.Hash, contractCodeEntry.Code)
			codeCount++
		case xdr.LedgerEntryTypeContractData:
			if p.processArchivedContractData(entry) {
				instanceCount++
			}
		}
	}
	log.Ctx(ctx).Infof("Hot archive pass: %d archived contract code entries, %d archived contract instances", codeCount, instanceCount)
	return nil
}

// processArchivedContractData ingests an archived contract-instance entry so its
// wasm mapping and contract_tokens row survive eviction. Balance entries and SAC
// instances are skipped. Reports whether the entry was ingested.
func (p *checkpointProcessor) processArchivedContractData(entry xdr.LedgerEntry) bool {
	contractDataEntry := entry.Data.MustContractData()
	if contractDataEntry.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return false
	}

	contractAddress, ok := contractDataEntry.Contract.GetContractId()
	if !ok {
		return false
	}
	contractAddressStr := strkey.MustEncode(strkey.VersionByteContract, contractAddress[:])

	// Post-side wrapping mirrors the shape the live checkpoint reader produces, so
	// processContractInstanceChange works identically on archived entries.
	change := ingest.Change{
		Type:       entry.Data.Type,
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Post:       &entry,
	}
	result := p.service.processContractInstanceChange(change, contractAddressStr, contractDataEntry)
	if result.Skip || result.IsSAC {
		return false
	}

	p.data.uniqueContractTokens[result.Contract.ID] = result.Contract
	if result.WasmHash != nil {
		p.contractAddressesByWasmHash[*result.WasmHash] = append(
			p.contractAddressesByWasmHash[*result.WasmHash], xdr.Hash(contractAddress))
	}
	return true
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

// flushRemainingBatch adds the pool shares whose pool was captured — every pool
// entry has been read by now — and flushes any remaining data in the batch.
func (p *checkpointProcessor) flushRemainingBatch(ctx context.Context) error {
	for _, share := range p.pendingPoolShares {
		if _, skipped := p.skippedPoolIDs[share.poolID]; skipped {
			continue
		}
		p.batch.addLiquidityPoolShare(share.accountAddress, share.poolID, share.shares, p.checkpointLedger)
	}

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
	// Store contract tokens and trustline assets in DB
	if err := p.service.storeTokensInDB(ctx, dbTx, p.data.uniqueAssets, p.data.uniqueContractTokens); err != nil {
		return fmt.Errorf("storing tokens in postgres: %w", err)
	}

	// Persist SAC balances, but only for contracts confirmed as SAC via their
	// instance entry. The scan populated uniqueContractTokens; a balance whose
	// contract is not among them is not a SAC balance and is dropped. This also
	// guarantees every retained balance has a contract_tokens parent, so the
	// deferred fk_contract_token holds at COMMIT.
	verifiedSAC := make(map[uuid.UUID]struct{}, len(p.data.uniqueContractTokens))
	for id, contract := range p.data.uniqueContractTokens {
		if contract.Type == string(types.ContractTypeSAC) {
			verifiedSAC[id] = struct{}{}
		}
	}
	keptSACBalances := p.pendingSACBalances[:0]
	skippedSACBalances := 0
	for _, bal := range p.pendingSACBalances {
		if _, ok := verifiedSAC[bal.ContractID]; ok {
			keptSACBalances = append(keptSACBalances, bal)
		} else {
			skippedSACBalances++
		}
	}
	if skippedSACBalances > 0 {
		log.Ctx(ctx).Warnf("checkpoint: skipped %d SAC balance(s) for contracts not verified as SAC", skippedSACBalances)
	}
	if len(keptSACBalances) > 0 {
		if err := p.service.sacBalanceModel.BatchCopy(ctx, dbTx, keptSACBalances); err != nil {
			return fmt.Errorf("copying verified SAC balances: %w", err)
		}
	}

	// Persist protocol WASMs
	if err := p.service.persistProtocolWasms(ctx, dbTx, p.wasmClassifications); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	// Persist protocol contracts (must come after WASMs due to FK)
	if err := p.service.persistProtocolContracts(ctx, dbTx, p.wasmClassifications, p.contractAddressesByWasmHash); err != nil {
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

	// Key.Type and Val.Type are independent XDR unions — a malformed entry with
	// a contract-instance Key but a non-instance Val must not panic the ingest
	// goroutine. Use GetInstance + ok rather than MustInstance.
	contractInstance, ok := contractDataEntry.Val.GetInstance()
	if !ok {
		return contractInstanceResult{Skip: true}
	}

	// Switched rather than compared so that a protocol adding a fourth
	// executable kind fails the exhaustive linter instead of being skipped.
	switch contractInstance.Executable.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
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

	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		// Handled by the SAC branch above; reaching here means Extract failed.

	case xdr.ContractExecutableTypeContractExecutableExternalRef:
		// CAP-0085: an (owner, tag) pair naming an entry in another contract's
		// storage, so there is no WASM hash and no classification.
		if s.metricsService != nil {
			s.metricsService.ExternalRefContractsTotal.Inc()
		}
		log.Warnf("contract %s has an external-ref executable (%s); leaving it unclassified",
			contractAddress, processors.DescribeExternalRef(contractInstance.Executable))
	}

	return contractInstanceResult{Skip: true}
}

// storeTokensInDB stores collected contract and asset metadata into PostgreSQL.
func (s *checkpointService) storeTokensInDB(
	ctx context.Context,
	dbTx pgx.Tx,
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

	log.Ctx(ctx).Infof("Inserted %d trustline assets, %d contract tokens in %.2f minutes", len(uniqueAssets), len(uniqueContractTokens), time.Since(startTime).Minutes())

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
func (s *checkpointService) persistProtocolWasms(ctx context.Context, dbTx pgx.Tx, wasmClassifications map[xdr.Hash]types.ContractType) error {
	if len(wasmClassifications) == 0 {
		return nil
	}

	wasms := make([]wbdata.ProtocolWasms, 0, len(wasmClassifications))
	for hash := range wasmClassifications {
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
func (s *checkpointService) persistProtocolContracts(ctx context.Context, dbTx pgx.Tx, wasmClassifications map[xdr.Hash]types.ContractType, contractAddressesByWasmHash map[xdr.Hash][]xdr.Hash) error {
	if len(contractAddressesByWasmHash) == 0 {
		return nil
	}

	var contracts []wbdata.ProtocolContracts
	var skipped int
	for hash, addrs := range contractAddressesByWasmHash {
		if _, exists := wasmClassifications[hash]; !exists {
			skipped += len(addrs)
			continue
		}
		for _, addr := range addrs {
			contracts = append(contracts, wbdata.ProtocolContracts{
				ContractID: types.HashBytea(hex.EncodeToString(addr[:])),
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
