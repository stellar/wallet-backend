package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrAlreadyInSync = errors.New("ingestion is already in sync")

const (
	// BackfillBatchSize is the number of ledgers processed per parallel batch during backfill.
	BackfillBatchSize uint32 = 250
	// HistoricalBufferLedgers is the number of ledgers to keep before latestRPCLedger
	// to avoid racing with live finalization during parallel processing.
	HistoricalBufferLedgers uint32 = 5
	// maxLedgerFetchRetries is the maximum number of retry attempts when fetching a ledger fails.
	maxLedgerFetchRetries = 10
	// maxRetryBackoff is the maximum backoff duration between retry attempts.
	maxRetryBackoff = 30 * time.Second
	// IngestionModeLive represents continuous ingestion from the latest ledger onwards.
	IngestionModeLive = "live"
	// IngestionModeBackfill represents historical ledger ingestion for a specified range.
	IngestionModeBackfill = "backfill"
)

// LedgerBackendFactory creates new LedgerBackend instances for parallel batch processing.
// Each batch needs its own backend because LedgerBackend is not thread-safe.
type LedgerBackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

// IngestServiceConfig holds the configuration for creating an IngestService.
type IngestServiceConfig struct {
	IngestionMode           string
	Models                  *data.Models
	LatestLedgerCursorName  string
	OldestLedgerCursorName  string
	AccountTokensCursorName string
	AppTracker              apptracker.AppTracker
	RPCService              RPCService
	LedgerBackend           ledgerbackend.LedgerBackend
	LedgerBackendFactory    LedgerBackendFactory
	ChannelAccountStore     store.ChannelAccountStore
	AccountTokenService     AccountTokenService
	ContractMetadataService ContractMetadataService
	MetricsService          metrics.MetricsService
	GetLedgersLimit         int
	Network                 string
	NetworkPassphrase       string
	Archive                 historyarchive.ArchiveInterface
	SkipTxMeta              bool
	SkipTxEnvelope          bool
	EnableParticipantFiltering bool
}

// BackfillBatch represents a contiguous range of ledgers to process as a unit.
type BackfillBatch struct {
	StartLedger uint32
	EndLedger   uint32
}

// BackfillResult tracks the outcome of processing a single batch.
type BackfillResult struct {
	Batch        BackfillBatch
	LedgersCount int
	Duration     time.Duration
	Error        error
}

// batchAnalysis holds the aggregated results from processing multiple backfill batches.
type batchAnalysis struct {
	failedBatches []BackfillBatch
	successCount  int
	totalLedgers  int
}

// analyzeBatchResults aggregates backfill batch results and logs any failures.
func analyzeBatchResults(ctx context.Context, results []BackfillResult) batchAnalysis {
	var analysis batchAnalysis
	for _, result := range results {
		if result.Error != nil {
			analysis.failedBatches = append(analysis.failedBatches, result.Batch)
			log.Ctx(ctx).Errorf("Batch [%d-%d] failed: %v",
				result.Batch.StartLedger, result.Batch.EndLedger, result.Error)
		} else {
			analysis.successCount++
			analysis.totalLedgers += result.LedgersCount
		}
	}
	return analysis
}

// generateAdvisoryLockID creates a deterministic advisory lock ID based on the network name.
// This ensures different networks (mainnet, testnet) get separate locks while being consistent across restarts.
func generateAdvisoryLockID(network string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-ingest-" + network))
	return int(h.Sum64())
}

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	ingestionMode              string
	models                     *data.Models
	latestLedgerCursorName     string
	oldestLedgerCursorName     string
	accountTokensCursorName    string
	advisoryLockID             int
	appTracker                 apptracker.AppTracker
	rpcService                 RPCService
	ledgerBackend              ledgerbackend.LedgerBackend
	ledgerBackendFactory       LedgerBackendFactory
	chAccStore                 store.ChannelAccountStore
	accountTokenService        AccountTokenService
	contractMetadataService    ContractMetadataService
	metricsService             metrics.MetricsService
	networkPassphrase          string
	getLedgersLimit            int
	ledgerIndexer              *indexer.Indexer
	archive                    historyarchive.ArchiveInterface
	backfillMode               bool
	enableParticipantFiltering bool
	backfillPool               pond.Pool
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)
	cfg.MetricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	backfillPool := pond.NewPool(0)
	cfg.MetricsService.RegisterPoolMetrics("backfill", backfillPool)

	return &ingestService{
		ingestionMode:           cfg.IngestionMode,
		models:                  cfg.Models,
		latestLedgerCursorName:  cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:  cfg.OldestLedgerCursorName,
		accountTokensCursorName: cfg.AccountTokensCursorName,
		advisoryLockID:          generateAdvisoryLockID(cfg.Network),
		appTracker:              cfg.AppTracker,
		rpcService:              cfg.RPCService,
		ledgerBackend:           cfg.LedgerBackend,
		ledgerBackendFactory:    cfg.LedgerBackendFactory,
		chAccStore:              cfg.ChannelAccountStore,
		accountTokenService:     cfg.AccountTokenService,
		contractMetadataService: cfg.ContractMetadataService,
		metricsService:          cfg.MetricsService,
		networkPassphrase:       cfg.NetworkPassphrase,
		getLedgersLimit:         cfg.GetLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.MetricsService, cfg.SkipTxMeta, cfg.SkipTxEnvelope),
		archive:                 cfg.Archive,
		enableParticipantFiltering: cfg.EnableParticipantFiltering,
		backfillPool:            backfillPool,
	}, nil
}

// Run starts the ingestion service in the configured mode (live or backfill).
// For live mode, startLedger and endLedger are ignored and ingestion runs continuously from the last checkpoint.
// For backfill mode, processes ledgers in the range [startLedger, endLedger].
func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
	switch m.ingestionMode {
	case IngestionModeLive:
		return m.startLiveIngestion(ctx)
	case IngestionModeBackfill:
		return m.startBackfilling(ctx, startLedger, endLedger)
	default:
		return fmt.Errorf("unsupported ingestion mode %q, must be %q or %q", m.ingestionMode, IngestionModeLive, IngestionModeBackfill)
	}
}

// startLiveIngestion begins continuous ingestion from the last checkpoint ledger,
// acquiring an advisory lock to prevent concurrent ingestion instances.
func (m *ingestService) startLiveIngestion(ctx context.Context) error {
	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently
	if lockAcquired, err := db.AcquireAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	} else if !lockAcquired {
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
			err = fmt.Errorf("releasing advisory lock: %w", err)
			log.Ctx(ctx).Error(err)
		}
	}()

	// Get latest ingested ledger to determine DB state
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	startLedger := latestIngestedLedger + 1
	if latestIngestedLedger == 0 {
		startLedger, err = m.archive.GetLatestLedgerSequence()
		if err != nil {
			return fmt.Errorf("getting latest ledger sequence: %w", err)
		}

		err = m.accountTokenService.PopulateAccountTokens(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("populating account tokens cache: %w", err)
		}

		err := m.initializeCursors(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("initializing cursors: %w", err)
		}
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}
	return m.ingestLiveLedgers(ctx, startLedger)
}


func (m *ingestService) updateCursor(ctx context.Context, currentLedger uint32) error {
	cursorStart := time.Now()
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if updateErr := m.models.IngestStore.Update(ctx, dbTx, m.ledgerCursorName, currentLedger); updateErr != nil {
			return fmt.Errorf("updating latest synced ledger: %w", updateErr)
		}
		m.metricsService.SetLatestLedgerIngested(float64(currentLedger))
		return nil
	})
	if err != nil {
		return fmt.Errorf("updating cursors: %w", err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("cursor_update", time.Since(cursorStart).Seconds())
	return nil
}

// prepareBackendRange prepares the ledger backend with the appropriate range type.
// Returns the operating mode (live streaming vs backfill).
func (m *ingestService) prepareBackendRange(ctx context.Context, startLedger, endLedger uint32) error {
	var ledgerRange ledgerbackend.Range
	if endLedger == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
		log.Ctx(ctx).Infof("Prepared backend with unbounded range starting from ledger %d", startLedger)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(startLedger, endLedger)
		log.Ctx(ctx).Infof("Prepared backend with bounded range [%d, %d]", startLedger, endLedger)
	}

	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing datastore backend unbounded range from %d: %w", startLedger, err)
	}
	return nil
}

// calculateCheckpointLedger determines the appropriate checkpoint ledger for account token cache population.
// If startLedger is 0, it returns the latest checkpoint from the archive.
// If startLedger is specified, it returns startLedger if it's a checkpoint, otherwise the previous checkpoint.
func (m *ingestService) calculateCheckpointLedger(startLedger uint32) (uint32, error) {
	archiveManager := m.archive.GetCheckpointManager()

	if startLedger == 0 {
		// Get latest checkpoint from archive
		latestLedger, err := m.archive.GetLatestLedgerSequence()
		if err != nil {
			return 0, fmt.Errorf("getting latest ledger sequence: %w", err)
		}
		return latestLedger, nil
	}

	// For specified startLedger, use it if it's a checkpoint, otherwise use previous checkpoint
	if archiveManager.IsCheckpoint(startLedger) {
		return startLedger, nil
	}
	return archiveManager.PrevCheckpoint(startLedger), nil
}

// processLedger processes a single ledger through all ingestion phases.
// Phase 1: Get transactions from ledger
// Phase 2: Process transactions using Indexer (parallel within ledger)
// Phase 3: Insert all data into DB
func (m *ingestService) processLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta) error {
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Phase 1: Get transactions from ledger
	start := time.Now()
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("get_transactions", time.Since(start).Seconds())

	// Phase 2: Process transactions using Indexer (parallel within ledger)
	start = time.Now()
	buffer := indexer.NewIndexerBuffer()
	participantCount, err := m.ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionParticipantsCount(participantCount)
	m.metricsService.ObserveIngestionPhaseDuration("process_and_buffer", time.Since(start).Seconds())

	// Phase 3: Insert all data into DB
	start = time.Now()
	if err := m.ingestProcessedData(ctx, buffer); err != nil {
		return fmt.Errorf("ingesting processed data for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("db_insertion", time.Since(start).Seconds())

	// Metrics
	m.metricsService.IncIngestionTransactionsProcessed(buffer.GetNumberOfTransactions())
	m.metricsService.IncIngestionOperationsProcessed(buffer.GetNumberOfOperations())

	return nil
}

func (m *ingestService) getLedgerTransactions(ctx context.Context, xdrLedgerCloseMeta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.networkPassphrase, xdrLedgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer utils.DeferredClose(ctx, ledgerTxReader, "closing ledger transaction reader")

	transactions := make([]ingest.LedgerTransaction, 0)
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading ledger: %w", err)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// filteredIngestionData holds the filtered data for ingestion
type filteredIngestionData struct {
	txs            []types.Transaction
	txParticipants map[string]set.Set[string]
	ops            []types.Operation
	opParticipants map[int64]set.Set[string]
	stateChanges   []types.StateChange
}

// filterByRegisteredAccounts filters ingestion data to only include items
// where at least one participant is a registered account.
// If a transaction/operation has ANY registered participant, it is included with ALL its participants.
func (m *ingestService) filterByRegisteredAccounts(
	ctx context.Context,
	txs []types.Transaction,
	txParticipants map[string]set.Set[string],
	ops []types.Operation,
	opParticipants map[int64]set.Set[string],
	stateChanges []types.StateChange,
	allParticipants []string,
) (*filteredIngestionData, error) {
	// Get registered accounts from DB
	existing, err := m.models.Account.BatchGetByIDs(ctx, allParticipants)
	if err != nil {
		return nil, fmt.Errorf("getting registered accounts: %w", err)
	}
	registeredAccounts := set.NewSet(existing...)

	log.Ctx(ctx).Infof("filtering enabled: %d/%d participants are registered", len(existing), len(allParticipants))

	// Filter transactions: include if ANY participant is registered
	txHashesToInclude := set.NewSet[string]()
	for txHash, participants := range txParticipants {
		for p := range participants.Iter() {
			if registeredAccounts.Contains(p) {
				txHashesToInclude.Add(txHash)
				break
			}
		}
	}

	filteredTxs := make([]types.Transaction, 0, txHashesToInclude.Cardinality())
	filteredTxParticipants := make(map[string]set.Set[string])
	for _, tx := range txs {
		if txHashesToInclude.Contains(tx.Hash) {
			filteredTxs = append(filteredTxs, tx)
			filteredTxParticipants[tx.Hash] = txParticipants[tx.Hash]
		}
	}

	// Filter operations: include if ANY participant is registered
	opIDsToInclude := set.NewSet[int64]()
	for opID, participants := range opParticipants {
		for p := range participants.Iter() {
			if registeredAccounts.Contains(p) {
				opIDsToInclude.Add(opID)
				break
			}
		}
	}

	filteredOps := make([]types.Operation, 0, opIDsToInclude.Cardinality())
	filteredOpParticipants := make(map[int64]set.Set[string])
	for _, op := range ops {
		if opIDsToInclude.Contains(op.ID) {
			filteredOps = append(filteredOps, op)
			filteredOpParticipants[op.ID] = opParticipants[op.ID]
		}
	}

	// Filter state changes: include if account is registered
	filteredSC := make([]types.StateChange, 0)
	for _, sc := range stateChanges {
		if registeredAccounts.Contains(sc.AccountID) {
			filteredSC = append(filteredSC, sc)
		}
	}

	log.Ctx(ctx).Infof("after filtering: %d txs, %d ops, %d state_changes",
		len(filteredTxs), len(filteredOps), len(filteredSC))

	return &filteredIngestionData{
		txs:            filteredTxs,
		txParticipants: filteredTxParticipants,
		ops:            filteredOps,
		opParticipants: filteredOpParticipants,
		stateChanges:   filteredSC,
	}, nil
}

func (m *ingestService) ingestProcessedData(ctx context.Context, indexerBuffer indexer.IndexerBufferInterface) error {
	// Get data from indexer buffer
	txs := indexerBuffer.GetTransactions()
	txParticipants := indexerBuffer.GetTransactionsParticipants()
	ops := indexerBuffer.GetOperations()
	opParticipants := indexerBuffer.GetOperationsParticipants()
	stateChanges := indexerBuffer.GetStateChanges()

	// When filtering is enabled, only store data for registered accounts
	if m.enableParticipantFiltering {
		filtered, err := m.filterByRegisteredAccounts(
			ctx, txs, txParticipants, ops, opParticipants, stateChanges,
			indexerBuffer.GetAllParticipants(),
		)
		if err != nil {
			return fmt.Errorf("filtering by registered accounts: %w", err)
		}
		txs = filtered.txs
		txParticipants = filtered.txParticipants
		ops = filtered.ops
		opParticipants = filtered.opParticipants
		stateChanges = filtered.stateChanges
	}

	dbTxErr := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		// NOTE: No BatchInsert(accounts) - accounts table only has registered accounts

		// 2. Insert queries
		// 2.1. Insert transactions
		if len(txs) > 0 {
			insertedHashes, err := m.models.Transactions.BatchInsert(ctx, dbTx, txs, txParticipants)
			if err != nil {
				return fmt.Errorf("batch inserting transactions: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d transactions with hashes %v", len(insertedHashes), insertedHashes)
		}

		// 2.2. Insert operations
		if len(ops) > 0 {
			insertedOpIDs, err := m.models.Operations.BatchInsert(ctx, dbTx, ops, opParticipants)
			if err != nil {
				return fmt.Errorf("batch inserting operations: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d operations with IDs %v", len(insertedOpIDs), insertedOpIDs)
		}

		// 2.3. Insert state changes
		if len(stateChanges) > 0 {
			insertedStateChangeIDs, err := m.models.StateChanges.BatchInsert(ctx, dbTx, stateChanges)
			if err != nil {
				return fmt.Errorf("batch inserting state changes: %w", err)
			}

			// Count state changes by type and category
			typeCategoryCount := make(map[string]map[string]int)
			for _, sc := range stateChanges {
				category := string(sc.StateChangeCategory)
				scType := ""
				if sc.StateChangeReason != nil {
					scType = string(*sc.StateChangeReason)
				}

				if typeCategoryCount[scType] == nil {
					typeCategoryCount[scType] = make(map[string]int)
				}
				typeCategoryCount[scType][category]++
			}

			for scType, categories := range typeCategoryCount {
				for category, count := range categories {
					m.metricsService.IncStateChanges(scType, category, count)
				}
			}

			log.Ctx(ctx).Infof("✅ inserted %d state changes with IDs %v", len(insertedStateChangeIDs), insertedStateChangeIDs)
		}

		// 3. Unlock channel accounts.
		if !m.backfillMode {
			err := m.unlockChannelAccounts(ctx, txs)
			if err != nil {
				return fmt.Errorf("unlocking channel accounts: %w", err)
			}
		}

		return nil
	})
	if dbTxErr != nil {
		return fmt.Errorf("ingesting processed data: %w", dbTxErr)
	}

	if !m.backfillMode {
		trustlineChanges := indexerBuffer.GetTrustlineChanges()
		// Insert trustline changes in the ascending order of operation IDs using batch processing
		sort.Slice(trustlineChanges, func(i, j int) bool {
			return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
		})

		contractChanges := indexerBuffer.GetContractChanges()

		// Process all trustline and contract changes in a single batch using Redis pipelining
		if err := m.accountTokenService.ProcessTokenChanges(ctx, trustlineChanges, contractChanges); err != nil {
			log.Ctx(ctx).Errorf("processing trustline changes batch: %v", err)
			return fmt.Errorf("processing trustline changes batch: %w", err)
		}
		log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(trustlineChanges), len(contractChanges))

		// Fetch and store metadata for new SAC/SEP-41 contracts discovered during live ingestion
		if m.contractMetadataService != nil {
			newContractTypesByID := m.filterNewContractTokens(ctx, contractChanges)
			if len(newContractTypesByID) > 0 {
				log.Ctx(ctx).Infof("Fetching metadata for %d new contract tokens", len(newContractTypesByID))
				if err := m.contractMetadataService.FetchAndStoreMetadata(ctx, newContractTypesByID); err != nil {
					log.Ctx(ctx).Warnf("fetching new contract metadata: %v", err)
					// Don't return error - we don't want to block ingestion for metadata fetch failures
				}
			}
		}
	}

	return nil
}

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, txs []types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	innerTxHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		if tx.InnerTransactionHash != "" {
			innerTxHashes = append(innerTxHashes, tx.InnerTransactionHash)
			continue
		}

		// Fallback for cases where InnerTransactionHash might not be populated (though it should be)
		// Skip transactions without envelope XDR (when skip-tx-envelope is enabled)
		if tx.EnvelopeXDR == nil {
			continue
		}
		innerTxHash, err := m.extractInnerTxHash(*tx.EnvelopeXDR)
		if err != nil {
			return fmt.Errorf("extracting inner tx hash: %w", err)
		}
		innerTxHashes = append(innerTxHashes, innerTxHash)
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, nil, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("🔓 unlocked %d channel accounts", affectedRows)
	}

	return nil
}

func (m *ingestService) GetLedgerTransactions(ledger int64) ([]entities.Transaction, error) {
	var ledgerTransactions []entities.Transaction
	var cursor string
	lastLedgerSeen := ledger
	for lastLedgerSeen == ledger {
		getTxnsResp, err := m.rpcService.GetTransactions(ledger, cursor, 50)
		if err != nil {
			return []entities.Transaction{}, fmt.Errorf("getTransactions: %w", err)
		}
		cursor = getTxnsResp.Cursor
		for _, tx := range getTxnsResp.Transactions {
			if tx.Ledger == ledger {
				ledgerTransactions = append(ledgerTransactions, tx)
				lastLedgerSeen = tx.Ledger
			} else {
				lastLedgerSeen = tx.Ledger
				break
			}
		}
	}
	return ledgerTransactions, nil
}

// extractInnerTxHash takes a transaction XDR string and returns the hash of its inner transaction.
// For fee bump transactions, it returns the hash of the inner transaction.
// For regular transactions, it returns the hash of the transaction itself.
func (m *ingestService) extractInnerTxHash(txXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return "", fmt.Errorf("deserializing envelope xdr %q: %w", txXDR, err)
	}

	var innerTx *txnbuild.Transaction
	feeBumpTx, ok := genericTx.FeeBump()
	if ok {
		innerTx = feeBumpTx.InnerTransaction()
	} else {
		innerTx, ok = genericTx.Transaction()
		if !ok {
			return "", errors.New("transaction is neither fee bump nor inner transaction")
		}
	}

	innerTxHash, err := innerTx.HashHex(m.rpcService.NetworkPassphrase())
	if err != nil {
		return "", fmt.Errorf("generating hash hex: %w", err)
	}

	return innerTxHash, nil
}

// filterNewContractTokens extracts unique SAC/SEP-41 contract IDs from contract changes,
// checks which contracts already exist in the database, and returns a map of only new contracts.
func (m *ingestService) filterNewContractTokens(ctx context.Context, contractChanges []types.ContractChange) map[string]types.ContractType {
	if len(contractChanges) == 0 {
		return nil
	}

	// Extract unique SAC and SEP-41 contract IDs and build type map
	seen := set.NewSet[string]()
	contractTypeMap := make(map[string]types.ContractType)
	var contractIDs []string

	for _, change := range contractChanges {
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		if change.ContractID == "" {
			continue
		}
		if seen.Contains(change.ContractID) {
			continue
		}
		seen.Add(change.ContractID)
		contractIDs = append(contractIDs, change.ContractID)
		contractTypeMap[change.ContractID] = change.ContractType
	}

	if len(contractIDs) == 0 {
		return nil
	}

	// Check which contracts already exist in the database
	existingContracts, err := m.models.Contract.BatchGetByIDs(ctx, contractIDs)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to check existing contracts: %v", err)
		return nil
	}

	// Remove existing contracts from the map
	for _, contract := range existingContracts {
		delete(contractTypeMap, contract.ID)
	}

	return contractTypeMap
}
