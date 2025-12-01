package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
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

// generateAdvisoryLockID creates a deterministic advisory lock ID based on the network name.
// This ensures different networks (mainnet, testnet) get separate locks while being consistent across restarts.
func generateAdvisoryLockID(network string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-ingest-" + network))
	return int(h.Sum64())
}

// BackfillBatchSize is the number of ledgers processed per parallel batch during backfill.
const BackfillBatchSize uint32 = 100

// LedgerBackendFactory creates new LedgerBackend instances for parallel batch processing.
// Each batch needs its own backend because LedgerBackend is not thread-safe.
type LedgerBackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

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

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	models                  *data.Models
	latestLedgerCursorName  string
	oldestLedgerCursorName  string
	accountTokensCursorName string
	advisoryLockID          int
	appTracker              apptracker.AppTracker
	rpcService              RPCService
	ledgerBackend           ledgerbackend.LedgerBackend
	ledgerBackendFactory    LedgerBackendFactory
	chAccStore              store.ChannelAccountStore
	accountTokenService     AccountTokenService
	contractMetadataService ContractMetadataService
	metricsService          metrics.MetricsService
	networkPassphrase       string
	getLedgersLimit         int
	ledgerIndexer           *indexer.Indexer
	archive                 historyarchive.ArchiveInterface
	backfillMode            bool
	skipTxMeta              bool
	backfillPool            pond.Pool
}

func NewIngestService(
	models *data.Models,
	latestLedgerCursorName string,
	oldestLedgerCursorName string,
	accountTokensCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
	ledgerBackend ledgerbackend.LedgerBackend,
	ledgerBackendFactory LedgerBackendFactory,
	chAccStore store.ChannelAccountStore,
	accountTokenService AccountTokenService,
	contractMetadataService ContractMetadataService,
	metricsService metrics.MetricsService,
	getLedgersLimit int,
	network string,
	networkPassphrase string,
	archive historyarchive.ArchiveInterface,
	skipTxMeta bool,
) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)
	metricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	return &ingestService{
		models:                  models,
		latestLedgerCursorName:  latestLedgerCursorName,
		oldestLedgerCursorName:  oldestLedgerCursorName,
		accountTokensCursorName: accountTokensCursorName,
		advisoryLockID:          generateAdvisoryLockID(network),
		appTracker:              appTracker,
		rpcService:              rpcService,
		ledgerBackend:           ledgerBackend,
		ledgerBackendFactory:    ledgerBackendFactory,
		chAccStore:              chAccStore,
		accountTokenService:     accountTokenService,
		contractMetadataService: contractMetadataService,
		metricsService:          metricsService,
		networkPassphrase:       networkPassphrase,
		getLedgersLimit:         getLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(networkPassphrase, ledgerIndexerPool, metricsService, skipTxMeta),
		archive:                 archive,
		backfillMode:            false,
		skipTxMeta:              skipTxMeta,
	}, nil
}

func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
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

	// Check if account tokens cache is populated
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest account-tokens ledger cursor: %w", err)
	}

	// If latestIngestedLedger == 0, then its an empty db. In that case, we get the latest checkpoint ledger
	// and start from there.
	if latestIngestedLedger == 0 {
		startLedger, err = m.calculateCheckpointLedger(startLedger)
		if err != nil {
			return fmt.Errorf("calculating checkpoint ledger: %w", err)
		}

		log.Ctx(ctx).Infof("Account tokens cache not populated, using checkpoint ledger: %d", startLedger)

		if populateErr := m.accountTokenService.PopulateAccountTokens(ctx, startLedger); populateErr != nil {
			return fmt.Errorf("populating account tokens cache: %w", populateErr)
		}

		// Initialize both cursors to the starting ledger on first run
		if initErr := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
			if updateErr := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, startLedger); updateErr != nil {
				return fmt.Errorf("initializing latest cursor: %w", updateErr)
			}
			if updateErr := m.models.IngestStore.Update(ctx, dbTx, m.oldestLedgerCursorName, startLedger); updateErr != nil {
				return fmt.Errorf("initializing oldest cursor: %w", updateErr)
			}
			return nil
		}); initErr != nil {
			return fmt.Errorf("initializing cursors to ledger %d: %w", startLedger, initErr)
		}
		log.Ctx(ctx).Infof("Initialized both cursors to ledger %d", startLedger)
	} else {
		// If we already have data ingested, check if we need to backfill historical data
		if startLedger > 0 && startLedger < latestIngestedLedger {
			// Backfill only handles gaps BEFORE latestIngestedLedger
			backfillEnd := endLedger
			if endLedger == 0 || endLedger > latestIngestedLedger {
				backfillEnd = latestIngestedLedger
			}
			if backfillErr := m.backfillGaps(ctx, startLedger, backfillEnd); backfillErr != nil {
				return fmt.Errorf("backfilling gaps from ledger %d to %d: %w", startLedger, backfillEnd, backfillErr)
			}
		}

		// If bounded endLedger is already within ingested range, we're done
		if endLedger > 0 && endLedger <= latestIngestedLedger {
			log.Ctx(ctx).Infof("Successfully backfilled ledgers from [%d, %d]", startLedger, endLedger)
			return nil
		}

		// Continue from next ledger after latest ingested
		startLedger = latestIngestedLedger + 1
	}

	// Ingest from startLedger onwards (bounded or unbounded)
	err = m.prepareBackendRange(ctx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}
	return m.ingestRange(ctx, false, startLedger, endLedger)
}

func (m *ingestService) backfillGaps(ctx context.Context, startLedger, endLedger uint32) error {
	// Get oldest ledger ingested (endLedger <= latestIngestedLedger is guaranteed by caller)
	oldestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting oldest ingest ledger: %w", err)
	}

	newGaps := make([]data.LedgerRange, 0)
	currentGaps, err := m.models.IngestStore.GetLedgerGaps(ctx)
	if err != nil {
		return fmt.Errorf("calculating gaps in ledger range: %w", err)
	}

	switch {
	case endLedger == oldestIngestedLedger:
		// Case 1: Entirely before existing data but end ledger == oldest ingested ledger - backfill the whole range from [start, oldestIngestedLedger - 1]
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   oldestIngestedLedger - 1,
			})
		}
	case endLedger < oldestIngestedLedger:
		// Case 2: Entirely before existing data - backfill the whole range from [start, end]
		newGaps = append(newGaps, data.LedgerRange{
			GapStart: startLedger,
			GapEnd:   endLedger,
		})

	case startLedger < oldestIngestedLedger:
		// Case 3: Starts before existing data, overlaps with existing range
		// First, add the range before oldest (guard against underflow)
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   oldestIngestedLedger - 1,
			})
		}
		// Then add any gaps within the existing range up to endLedger
		for _, gap := range currentGaps {
			if gap.GapStart > endLedger {
				break // No more gaps in our range
			}
			// Clip gap to our requested range
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: gap.GapStart,
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}

	default:
		// Case 4: Entirely within existing range - only fill internal gaps
		for _, gap := range currentGaps {
			if gap.GapEnd < startLedger {
				continue // Skip gaps before our range
			}
			if gap.GapStart > endLedger {
				break // No more gaps in our range
			}
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: max(gap.GapStart, startLedger),
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}
	}

	// Split gaps into batches for parallel processing
	batches := splitGapsIntoBatches(newGaps, BackfillBatchSize)
	if len(batches) == 0 {
		return nil
	}
	log.Ctx(ctx).Infof("Backfilling %d batches (batch size: %d ledgers)", len(batches), BackfillBatchSize)

	// Process batches in parallel
	results := m.processBackfillBatchesParallel(ctx, batches)

	// Analyze results
	var failedBatches []BackfillBatch
	var successCount, totalLedgers int
	for _, result := range results {
		if result.Error != nil {
			failedBatches = append(failedBatches, result.Batch)
			log.Ctx(ctx).Errorf("Batch [%d-%d] failed: %v",
				result.Batch.StartLedger, result.Batch.EndLedger, result.Error)
		} else {
			successCount++
			totalLedgers += result.LedgersCount
		}
	}

	// Update oldest cursor only if ALL batches succeeded
	if len(failedBatches) == 0 && startLedger < oldestIngestedLedger {
		err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
			return m.models.IngestStore.Update(ctx, dbTx, m.oldestLedgerCursorName, startLedger)
		})
		if err != nil {
			return fmt.Errorf("updating oldest cursor: %w", err)
		}
		log.Ctx(ctx).Infof("Updated oldest ingested ledger cursor to %d", startLedger)
	}

	log.Ctx(ctx).Infof("Backfill completed: %d/%d batches succeeded, %d ledgers processed",
		successCount, len(batches), totalLedgers)

	if len(failedBatches) > 0 {
		return fmt.Errorf("backfill completed with %d failed batches", len(failedBatches))
	}

	return nil
}

// splitGapsIntoBatches divides ledger gaps into fixed-size batches for parallel processing.
func splitGapsIntoBatches(gaps []data.LedgerRange, batchSize uint32) []BackfillBatch {
	var batches []BackfillBatch

	for _, gap := range gaps {
		start := gap.GapStart
		for start <= gap.GapEnd {
			end := min(start + batchSize - 1, gap.GapEnd)
			batches = append(batches, BackfillBatch{
				StartLedger: start,
				EndLedger:   end,
			})
			start = end + 1
		}
	}

	return batches
}

// processBackfillBatchesParallel processes backfill batches in parallel using a worker pool.
func (m *ingestService) processBackfillBatchesParallel(ctx context.Context, batches []BackfillBatch) []BackfillResult {
	// Create backfill pool if not exists
	if m.backfillPool == nil {
		m.backfillPool = pond.NewPool(0)
		m.metricsService.RegisterPoolMetrics("backfill", m.backfillPool)
	}

	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)
	var mu sync.Mutex

	for i, batch := range batches {
		idx := i
		b := batch
		group.Submit(func() {
			result := m.processSingleBatch(ctx, b)
			mu.Lock()
			results[idx] = result
			mu.Unlock()
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Warnf("Backfill batch group wait returned error: %v", err)
	}
	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, batch BackfillBatch) BackfillResult {
	start := time.Now()
	result := BackfillResult{Batch: batch}

	// Create a new ledger backend for this batch
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		result.Error = fmt.Errorf("creating ledger backend: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing ledger backend for batch [%d-%d]: %v",
				batch.StartLedger, batch.EndLedger, closeErr)
		}
	}()

	// Prepare the range for this batch
	ledgerRange := ledgerbackend.BoundedRange(batch.StartLedger, batch.EndLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		result.Error = fmt.Errorf("preparing backend range: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	// Process each ledger in the batch sequentially
	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		ledgerMeta, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			result.Error = fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}

		if err := m.processLedger(ctx, ledgerMeta, true); err != nil {
			result.Error = fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}

		result.LedgersCount++
	}

	result.Duration = time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("backfill_batch", result.Duration.Seconds())
	log.Ctx(ctx).Infof("Batch [%d-%d] completed: %d ledgers in %v",
		batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

	return result
}

func (m *ingestService) ingestRange(ctx context.Context, isBackfill bool, startLedger, endLedger uint32) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger: %d", currentLedger)
	for endLedger == 0 || currentLedger <= endLedger {
		totalStart := time.Now()
		ledgerMeta, ledgerErr := m.ledgerBackend.GetLedger(ctx, currentLedger)
		if ledgerErr != nil {
			log.Ctx(ctx).Warnf("Error fetching ledger %d: %v, retrying...", currentLedger, ledgerErr)
			time.Sleep(time.Second)
			continue
		}
		m.metricsService.ObserveIngestionPhaseDuration("get_ledger", time.Since(totalStart).Seconds())

		if processErr := m.processLedger(ctx, ledgerMeta, isBackfill); processErr != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, processErr)
		}

		// Update cursor only for live ingestion
		if !isBackfill {
			err := m.updateCursor(ctx, currentLedger)
			if err != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, err)
			}
		}
		m.metricsService.ObserveIngestionDuration(time.Since(totalStart).Seconds())
		m.metricsService.IncIngestionLedgersProcessed(1)

		log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, time.Since(totalStart))
		currentLedger++
	}
	return nil
}

func (m *ingestService) updateCursor(ctx context.Context, currentLedger uint32) error {
	cursorStart := time.Now()
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if updateErr := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, currentLedger); updateErr != nil {
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
		return fmt.Errorf("preparing ledger backend range [%d, %d]: %w", startLedger, endLedger, err)
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
// isBackfill indicates whether this is a backfill operation (skips Redis cache updates and channel account unlocks)
func (m *ingestService) processLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, isBackfill bool) error {
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
	if err := m.ingestProcessedData(ctx, buffer, isBackfill); err != nil {
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

// ingestProcessedData inserts processed ledger data into the database.
// isBackfill indicates whether this is a backfill operation - when true, skips Redis cache updates and channel account unlocks.
func (m *ingestService) ingestProcessedData(ctx context.Context, indexerBuffer indexer.IndexerBufferInterface, isBackfill bool) error {
	dbTxErr := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		// 2. Insert queries
		// 2.1. Insert all participants as accounts
		participants := indexerBuffer.GetAllParticipants()
		if len(participants) > 0 {
			if err := m.models.Account.BatchInsert(ctx, dbTx, participants); err != nil {
				return fmt.Errorf("batch inserting accounts: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d participant accounts", len(participants))
		}

		// 2.2. Insert transactions
		txs := indexerBuffer.GetTransactions()
		stellarAddressesByTxHash := indexerBuffer.GetTransactionsParticipants()
		if len(txs) > 0 {
			insertedHashes, err := m.models.Transactions.BatchInsert(ctx, dbTx, txs, stellarAddressesByTxHash)
			if err != nil {
				return fmt.Errorf("batch inserting transactions: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d transactions with hashes %v", len(insertedHashes), insertedHashes)
		}

		// 2.3. Insert operations
		ops := indexerBuffer.GetOperations()
		stellarAddressesByOpID := indexerBuffer.GetOperationsParticipants()
		if len(ops) > 0 {
			insertedOpIDs, err := m.models.Operations.BatchInsert(ctx, dbTx, ops, stellarAddressesByOpID)
			if err != nil {
				return fmt.Errorf("batch inserting operations: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d operations with IDs %v", len(insertedOpIDs), insertedOpIDs)
		}

		// 2.4. Insert state changes
		stateChanges := indexerBuffer.GetStateChanges()
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

		// 3. Unlock channel accounts (skip during backfill as these are historical transactions)
		if !isBackfill {
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

	// Process trustline and contract changes only during live ingestion (not backfill)
	// These update the Redis cache which should only reflect current state
	if !isBackfill {
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
		innerTxHash, err := m.extractInnerTxHash(tx.EnvelopeXDR)
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
