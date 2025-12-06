package services

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"runtime"
	"sort"
	"strings"
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

const (
	// DefaultBackfillBatchSize is the number of ledgers processed per parallel batch during backfill.
	DefaultBackfillBatchSize uint32 = 250
	// DefaultBackfillDBInsertBatchSize is the number of ledgers in a batch to insert into the db.
	DefaultBackfillDBInsertBatchSize uint32 = 50
	// maxLedgerFetchRetries is the maximum number of retry attempts when fetching a ledger fails.
	maxLedgerFetchRetries = 10
	// maxRetryBackoff is the maximum backoff duration between retry attempts.
	maxRetryBackoff = 30 * time.Second
	// DefaultCatchupThreshold is the default number of ledgers behind network tip that triggers fast catchup.
	DefaultCatchupThreshold uint32 = 100
	// IngestionModeLive represents continuous ingestion from the latest ledger onwards.
	IngestionModeLive = "live"
	// IngestionModeBackfill represents historical ledger ingestion for a specified range.
	IngestionModeBackfill = "backfill"
)

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

// IngestService defines the interface for ledger ingestion operations.
type IngestService interface {
	// Run starts the ingestion service in the configured mode (live or backfill).
	// For live mode, startLedger and endLedger are ignored and ingestion runs continuously from the last checkpoint.
	// For backfill mode, processes ledgers in the range [startLedger, endLedger].
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

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
	// BackfillWorkers limits concurrent batch processing. Defaults to runtime.NumCPU().
	BackfillWorkers int
	// BackfillBatchSize is ledgers per batch. Defaults to 250.
	BackfillBatchSize int
	// BackfillDBInsertBatchSize is ledgers to process before flushing to DB. Defaults to 50.
	BackfillDBInsertBatchSize int
	// CatchupThreshold is the number of ledgers behind network tip that triggers fast catchup.
	// Defaults to 100.
	CatchupThreshold int
}

type ingestService struct {
	ingestionMode             string
	models                    *data.Models
	latestLedgerCursorName    string
	oldestLedgerCursorName    string
	accountTokensCursorName   string
	advisoryLockID            int
	appTracker                apptracker.AppTracker
	rpcService                RPCService
	ledgerBackend             ledgerbackend.LedgerBackend
	ledgerBackendFactory      LedgerBackendFactory
	chAccStore                store.ChannelAccountStore
	accountTokenService       AccountTokenService
	contractMetadataService   ContractMetadataService
	metricsService            metrics.MetricsService
	networkPassphrase         string
	getLedgersLimit           int
	ledgerIndexer             *indexer.Indexer
	archive                   historyarchive.ArchiveInterface
	skipTxMeta                bool
	backfillPool              pond.Pool
	backfillBatchSize         uint32
	backfillDBInsertBatchSize uint32
	backfillInstanceID        string // Format: "startLedger-endLedger" for multi-instance metrics
	catchupThreshold          uint32
}

// NewIngestService creates a new IngestService with the provided configuration.
func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create ledger indexer pool (unbounded for parallel transaction processing)
	ledgerIndexerPool := pond.NewPool(0)
	cfg.MetricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	// Create backfill pool with bounded size to control memory usage.
	// Default to NumCPU if not specified.
	backfillWorkers := cfg.BackfillWorkers
	if backfillWorkers <= 0 {
		backfillWorkers = runtime.NumCPU()
	}
	backfillPool := pond.NewPool(backfillWorkers)
	cfg.MetricsService.RegisterPoolMetrics("backfill", backfillPool)

	// Set batch size with default fallback
	backfillBatchSize := uint32(cfg.BackfillBatchSize)
	if backfillBatchSize == 0 {
		backfillBatchSize = DefaultBackfillBatchSize
	}

	// Set DB insert batch size with default fallback
	backfillDBInsertBatchSize := uint32(cfg.BackfillDBInsertBatchSize)
	if backfillDBInsertBatchSize == 0 {
		backfillDBInsertBatchSize = DefaultBackfillDBInsertBatchSize
	}

	// Set catchup threshold with default fallback
	catchupThreshold := uint32(cfg.CatchupThreshold)
	if catchupThreshold == 0 {
		catchupThreshold = DefaultCatchupThreshold
	}

	return &ingestService{
		ingestionMode:             cfg.IngestionMode,
		models:                    cfg.Models,
		latestLedgerCursorName:    cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:    cfg.OldestLedgerCursorName,
		accountTokensCursorName:   cfg.AccountTokensCursorName,
		advisoryLockID:            generateAdvisoryLockID(cfg.Network),
		appTracker:                cfg.AppTracker,
		rpcService:                cfg.RPCService,
		ledgerBackend:             cfg.LedgerBackend,
		ledgerBackendFactory:      cfg.LedgerBackendFactory,
		chAccStore:                cfg.ChannelAccountStore,
		accountTokenService:       cfg.AccountTokenService,
		contractMetadataService:   cfg.ContractMetadataService,
		metricsService:            cfg.MetricsService,
		networkPassphrase:         cfg.NetworkPassphrase,
		getLedgersLimit:           cfg.GetLedgersLimit,
		ledgerIndexer:             indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.MetricsService, cfg.SkipTxMeta),
		archive:                   cfg.Archive,
		skipTxMeta:                cfg.SkipTxMeta,
		backfillPool:              backfillPool,
		backfillBatchSize:         backfillBatchSize,
		backfillDBInsertBatchSize: backfillDBInsertBatchSize,
		catchupThreshold:          catchupThreshold,
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

		// err = m.accountTokenService.PopulateAccountTokens(ctx, startLedger)
		// if err != nil {
		// 	return fmt.Errorf("populating account tokens cache: %w", err)
		// }

		err := m.initializeCursors(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("initializing cursors: %w", err)
		}
	} else {
		// If we already have data in the DB, we will do an optimized catchup by parallely backfilling the ledgers.
		health, err := m.rpcService.GetHealth()
		if err != nil {
			return fmt.Errorf("getting health check result from RPC: %w", err)
		}

		networkLatestLedger := health.LatestLedger
		if networkLatestLedger > startLedger && (networkLatestLedger-startLedger) >= m.catchupThreshold {
			err := m.startBackfilling(ctx, startLedger, networkLatestLedger)
			if err != nil {
				return fmt.Errorf("catching up to network tip: %w", err)
			}
		}
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}
	return m.ingestLiveLedgers(ctx, startLedger)
}

// ingestLiveLedgers continuously processes ledgers starting from startLedger,
// updating cursors and metrics after each successful ledger.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", currentLedger)
	for {
		totalStart := time.Now()
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}
		m.metricsService.ObserveIngestionPhaseDuration("get_ledger", time.Since(totalStart).Seconds())

		if processErr := m.processLiveLedger(ctx, ledgerMeta); processErr != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, processErr)
		}

		// Update cursor only for live ingestion
		err := m.updateLatestLedgerCursor(ctx, currentLedger)
		if err != nil {
			return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, err)
		}
		m.metricsService.ObserveIngestionDuration(time.Since(totalStart).Seconds())
		m.metricsService.IncIngestionLedgersProcessed(1)

		log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, time.Since(totalStart))
		currentLedger++
	}
}

// startBackfilling processes historical ledgers in the specified range,
// identifying gaps and processing them in parallel batches.
func (m *ingestService) startBackfilling(ctx context.Context, startLedger, endLedger uint32) error {
	if startLedger > endLedger {
		return fmt.Errorf("start ledger cannot be greater than end ledger")
	}

	// Compute and store instance ID for metrics
	m.backfillInstanceID = fmt.Sprintf("%d-%d", startLedger, endLedger)

	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}
	if endLedger > latestIngestedLedger {
		return fmt.Errorf("end ledger %d cannot be greater than latest ingested ledger %d for backfilling", endLedger, latestIngestedLedger)
	}

	// Note that there could be some part of [start, end] that is already present in our db.
	// Since we use the COPY protocol for inserting into the db, we cannot insert duplicate data and have to calculate the
	// actual gap ranges in our db.
	gaps, err := m.calculateBackfillGaps(ctx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("calculating backfill gaps: %w", err)
	}

	if len(gaps) == 0 {
		log.Ctx(ctx).Infof("No gaps to backfill in range [%d - %d]", startLedger, endLedger)
		return nil
	}

	backfillBatches := m.splitGapsIntoBatches(gaps, m.backfillBatchSize)

	// Set batch total metric
	m.metricsService.SetBackfillBatchesTotal(m.backfillInstanceID, len(backfillBatches))

	startTime := time.Now()

	// Start elapsed time updater goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.metricsService.SetBackfillElapsed(m.backfillInstanceID, time.Since(startTime).Seconds())
			case <-done:
				return
			}
		}
	}()

	results := m.processBackfillBatchesParallel(ctx, backfillBatches)
	close(done) // Stop the updater goroutine

	// Set final elapsed time
	duration := time.Since(startTime)
	m.metricsService.SetBackfillElapsed(m.backfillInstanceID, duration.Seconds())

	analysis := analyzeBatchResults(ctx, results)

	// Record failed batches metric
	for range analysis.failedBatches {
		m.metricsService.IncBackfillBatchesFailed(m.backfillInstanceID)
	}

	if len(analysis.failedBatches) > 0 {
		return fmt.Errorf("backfilling failed: %d/%d batches failed", len(analysis.failedBatches), len(backfillBatches))
	}

	// Update oldest ledger cursor on success if configured
	if err := m.updateOldestLedgerCursor(ctx, startLedger); err != nil {
		return fmt.Errorf("updating cursor: %w", err)
	}

	log.Ctx(ctx).Infof("Backfilling completed in %v: %d batches, %d ledgers", duration, analysis.successCount, analysis.totalLedgers)
	return nil
}

// calculateBackfillGaps determines which ledger ranges need to be backfilled based on
// the requested range, oldest ingested ledger, and any existing gaps in the data.
func (m *ingestService) calculateBackfillGaps(ctx context.Context, startLedger, endLedger uint32) ([]data.LedgerRange, error) {
	// Get oldest ledger ingested (endLedger <= latestIngestedLedger is guaranteed by caller)
	oldestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName)
	if err != nil {
		return nil, fmt.Errorf("getting oldest ingest ledger: %w", err)
	}

	currentGaps, err := m.models.IngestStore.GetLedgerGaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("calculating gaps in ledger range: %w", err)
	}

	newGaps := make([]data.LedgerRange, 0)
	switch {
	case endLedger <= oldestIngestedLedger:
		// Case 1: End ledger matches/less than oldest - backfill [start, min(end, oldest-1)]
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   min(endLedger, oldestIngestedLedger-1),
			})
		}

	case startLedger < oldestIngestedLedger:
		// Case 2: Overlaps with existing range - backfill before oldest + internal gaps
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   oldestIngestedLedger - 1,
			})
		}
		for _, gap := range currentGaps {
			if gap.GapStart > endLedger {
				break
			}
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: gap.GapStart,
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}

	default:
		// Case 3: Entirely within existing range - only fill internal gaps
		for _, gap := range currentGaps {
			if gap.GapEnd < startLedger {
				continue
			}
			if gap.GapStart > endLedger {
				break
			}
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: max(gap.GapStart, startLedger),
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}
	}

	return newGaps, nil
}

// splitGapsIntoBatches divides ledger gaps into fixed-size batches for parallel processing.
func (m *ingestService) splitGapsIntoBatches(gaps []data.LedgerRange, batchSize uint32) []BackfillBatch {
	var batches []BackfillBatch

	for _, gap := range gaps {
		start := gap.GapStart
		for start <= gap.GapEnd {
			end := min(start+batchSize-1, gap.GapEnd)
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
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		group.Submit(func() {
			results[i] = m.processSingleBatch(ctx, batch)
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

	// Process each ledger in the batch using a single shared buffer.
	// Periodically flush to DB to control memory usage.
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)

	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		fetchStart := time.Now()
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			result.Error = fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "ledger_fetch", time.Since(fetchStart).Seconds())

		err = m.processBackfillLedger(ctx, ledgerMeta, batchBuffer)
		if err != nil {
			result.Error = fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}

		// Update current ledger progress and increment ledger count
		m.metricsService.IncBackfillLedgersProcessed(m.backfillInstanceID, 1)
		result.LedgersCount++
		ledgersInBuffer++

		// Flush buffer periodically to control memory usage
		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			dbInsertStart := time.Now()

			// Record metrics before clearing
			m.metricsService.IncBackfillTransactionsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfTransactions())
			m.metricsService.IncBackfillOperationsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfOperations())

			if err := m.ingestProcessedData(ctx, batchBuffer); err != nil {
				result.Error = fmt.Errorf("ingesting data for ledgers ending at %d: %w", ledgerSeq, err)
				result.Duration = time.Since(start)
				return result
			}
			m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "db_insertion", time.Since(dbInsertStart).Seconds())

			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Flush remaining data in buffer
	if ledgersInBuffer > 0 {
		dbInsertStart := time.Now()

		// Record metrics before final insert
		m.metricsService.IncBackfillTransactionsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfTransactions())
		m.metricsService.IncBackfillOperationsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfOperations())

		if err := m.ingestProcessedData(ctx, batchBuffer); err != nil {
			result.Error = fmt.Errorf("ingesting final data for batch [%d - %d]: %w", batch.StartLedger, batch.EndLedger, err)
			result.Duration = time.Since(start)
			return result
		}
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "db_insertion", time.Since(dbInsertStart).Seconds())
	}

	result.Duration = time.Since(start)

	// Record batch completion metrics
	m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "batch_processing", result.Duration.Seconds())
	m.metricsService.ObserveBackfillBatchLedgersProcessed(m.backfillInstanceID, result.LedgersCount)
	m.metricsService.IncBackfillBatchesCompleted(m.backfillInstanceID)

	log.Ctx(ctx).Infof("Batch [%d - %d] completed: %d ledgers in %v",
		batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

	return result
}

// getLedgerWithRetry fetches a ledger with exponential backoff retry logic.
// It respects context cancellation and limits retries to maxLedgerFetchRetries attempts.
func (m *ingestService) getLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	var lastErr error
	for attempt := range maxLedgerFetchRetries {
		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		ledgerMeta, err := backend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return ledgerMeta, nil
		}
		lastErr = err

		// Record retry metric for backfill mode
		if m.ingestionMode == IngestionModeBackfill && m.backfillInstanceID != "" {
			m.metricsService.IncBackfillRetries(m.backfillInstanceID)
		}

		backoff := max(time.Duration(1<<attempt)*time.Second, maxRetryBackoff)
		log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
			ledgerSeq, attempt+1, maxLedgerFetchRetries, err, backoff)

		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return xdr.LedgerCloseMeta{}, fmt.Errorf("failed after %d attempts: %w", maxLedgerFetchRetries, lastErr)
}

// updateLatestLedgerCursor updates the latest ledger cursor during live ingestion with metrics tracking.
func (m *ingestService) updateLatestLedgerCursor(ctx context.Context, currentLedger uint32) error {
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
	m.metricsService.ObserveIngestionPhaseDuration("latest_cursor_update", time.Since(cursorStart).Seconds())
	return nil
}

// updateOldestLedgerCursor updates the oldest ledger cursor during backfill with metrics tracking.
func (m *ingestService) updateOldestLedgerCursor(ctx context.Context, currentLedger uint32) error {
	cursorStart := time.Now()
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if updateErr := m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, currentLedger); updateErr != nil {
			return fmt.Errorf("updating oldest synced ledger: %w", updateErr)
		}
		m.metricsService.SetOldestLedgerIngested(float64(currentLedger))
		return nil
	})
	if err != nil {
		return fmt.Errorf("updating cursors: %w", err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("oldest_cursor_update", time.Since(cursorStart).Seconds())
	return nil
}

// initializeCursors initializes both latest and oldest cursors to the same starting ledger.
func (m *ingestService) initializeCursors(ctx context.Context, ledger uint32) error {
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if err := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, ledger); err != nil {
			return fmt.Errorf("initializing latest cursor: %w", err)
		}
		if err := m.models.IngestStore.Update(ctx, dbTx, m.oldestLedgerCursorName, ledger); err != nil {
			return fmt.Errorf("initializing oldest cursor: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("initializing cursors: %w", err)
	}
	return nil
}

// processBackfillLedger processes a ledger and populates the provided buffer.
func (m *ingestService) processBackfillLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Get transactions from ledger
	start := time.Now()
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	// Process transactions and populate buffer (combined collection + processing)
	_, err = m.ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "ledger_processing", time.Since(start).Seconds())

	return nil
}

/*
	 processLiveLedger processes a single ledger through all ingestion phases.
		Phase 1: Get transactions from ledger
		Phase 2: Process transactions and populate buffer (parallel within ledger)
		Phase 3: Insert all data into DB

Note: Live ingestion includes Redis cache updates and channel account unlocks, while backfill mode skips these operations.
*/
func (m *ingestService) processLiveLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta) error {
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

	// Record phase 2 metrics based on ingestion mode
	processingDuration := time.Since(start).Seconds()
	if m.ingestionMode == IngestionModeLive {
		m.metricsService.ObserveIngestionParticipantsCount(participantCount)
		m.metricsService.ObserveIngestionPhaseDuration("process_transactions", processingDuration)
	} else if m.ingestionMode == IngestionModeBackfill && m.backfillInstanceID != "" {
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "ledger_processing", processingDuration)
	}

	// Phase 3: Insert all data into DB
	start = time.Now()
	if err := m.ingestProcessedData(ctx, buffer); err != nil {
		return fmt.Errorf("ingesting processed data for ledger %d: %w", ledgerSeq, err)
	}

	// Record phase 3 metrics based on ingestion mode
	dbInsertDuration := time.Since(start).Seconds()
	if m.ingestionMode == IngestionModeLive {
		m.metricsService.ObserveIngestionPhaseDuration("db_insertion", dbInsertDuration)
	} else if m.ingestionMode == IngestionModeBackfill && m.backfillInstanceID != "" {
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "db_insertion", dbInsertDuration)
	}

	// Record transaction and operation counts based on ingestion mode
	if m.ingestionMode == IngestionModeLive {
		m.metricsService.IncIngestionTransactionsProcessed(buffer.GetNumberOfTransactions())
		m.metricsService.IncIngestionOperationsProcessed(buffer.GetNumberOfOperations())
	} else if m.ingestionMode == IngestionModeBackfill && m.backfillInstanceID != "" {
		m.metricsService.IncBackfillTransactionsProcessed(m.backfillInstanceID, buffer.GetNumberOfTransactions())
		m.metricsService.IncBackfillOperationsProcessed(m.backfillInstanceID, buffer.GetNumberOfOperations())
	}

	return nil
}

// getLedgerTransactions extracts all transactions from a ledger close meta.
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

// insertParticipants batch inserts participant accounts into the database.
func (m *ingestService) insertParticipants(ctx context.Context, dbTx db.Transaction, buffer indexer.IndexerBufferInterface) error {
	participants := buffer.GetAllParticipants()
	if len(participants) == 0 {
		return nil
	}
	if err := m.models.Account.BatchInsert(ctx, dbTx, participants); err != nil {
		return fmt.Errorf("batch inserting accounts: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d participant accounts", len(participants))
	return nil
}

// insertTransactions batch inserts transactions with their participants into the database.
// Uses COPY protocol with pointer slices to avoid copying large XDR strings.
func (m *ingestService) insertTransactions(ctx context.Context, dbTx db.Transaction, buffer indexer.IndexerBufferInterface) error {
	txs := buffer.GetTransactionPointers()
	if len(txs) == 0 {
		return nil
	}
	stellarAddressesByTxHash := buffer.GetTransactionsParticipants()
	insertedCount, err := m.models.Transactions.BatchInsertCopyFromPointers(ctx, dbTx, txs, stellarAddressesByTxHash)
	if err != nil {
		return fmt.Errorf("COPY inserting transactions: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d transactions", insertedCount)
	return nil
}

// insertOperations batch inserts operations with their participants into the database.
// Uses COPY protocol with pointer slices for better performance.
func (m *ingestService) insertOperations(ctx context.Context, dbTx db.Transaction, buffer indexer.IndexerBufferInterface) error {
	ops := buffer.GetOperationPointers()
	if len(ops) == 0 {
		return nil
	}
	stellarAddressesByOpID := buffer.GetOperationsParticipants()
	insertedCount, err := m.models.Operations.BatchInsertCopyFromPointers(ctx, dbTx, ops, stellarAddressesByOpID)
	if err != nil {
		return fmt.Errorf("COPY inserting operations: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d operations", insertedCount)
	return nil
}

// insertStateChanges batch inserts state changes and records metrics.
// Uses COPY protocol in backfill mode for better performance.
func (m *ingestService) insertStateChanges(ctx context.Context, dbTx db.Transaction, buffer indexer.IndexerBufferInterface) error {
	stateChanges := buffer.GetStateChanges()
	if len(stateChanges) == 0 {
		return nil
	}

	var insertedCount int
	var err error

	insertedCount, err = m.models.StateChanges.BatchInsertCopy(ctx, dbTx, stateChanges)
	if err != nil {
		return fmt.Errorf("COPY inserting state changes: %w", err)
	}

	m.recordStateChangeMetrics(stateChanges)
	log.Ctx(ctx).Infof("✅ inserted %d state changes", insertedCount)
	return nil
}

// processLiveIngestionTokenChanges processes trustline and contract changes for live ingestion.
// This updates the Redis cache and fetches metadata for new SAC/SEP-41 contracts.
func (m *ingestService) processLiveIngestionTokenChanges(ctx context.Context, buffer indexer.IndexerBufferInterface) error {
	trustlineChanges := buffer.GetTrustlineChanges()
	// Sort trustline changes by operation ID in ascending order
	sort.Slice(trustlineChanges, func(i, j int) bool {
		return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
	})

	contractChanges := buffer.GetContractChanges()

	// Process all trustline and contract changes in a single batch using Redis pipelining
	if err := m.accountTokenService.ProcessTokenChanges(ctx, trustlineChanges, contractChanges); err != nil {
		log.Ctx(ctx).Errorf("processing trustline changes batch: %v", err)
		return fmt.Errorf("processing trustline changes batch: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(trustlineChanges), len(contractChanges))

	// Fetch and store metadata for new SAC/SEP-41 contracts
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
	return nil
}

// ingestProcessedData inserts processed ledger data into the database.
// Live ingestion mode includes Redis cache updates and channel account unlocks.
// Backfill mode skips these operations to avoid affecting live data (determined by m.ingestionMode).
func (m *ingestService) ingestProcessedData(ctx context.Context, indexerBuffer indexer.IndexerBufferInterface) error {
	dbTxErr := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if err := m.insertParticipants(ctx, dbTx, indexerBuffer); err != nil {
			return err
		}
		if err := m.insertTransactions(ctx, dbTx, indexerBuffer); err != nil {
			return err
		}
		if err := m.insertOperations(ctx, dbTx, indexerBuffer); err != nil {
			return err
		}
		if err := m.insertStateChanges(ctx, dbTx, indexerBuffer); err != nil {
			return err
		}
		// Unlock channel accounts only during live ingestion (skip for historical backfill)
		if m.ingestionMode == IngestionModeLive {
			if err := m.unlockChannelAccounts(ctx, indexerBuffer.GetTransactions()); err != nil {
				return fmt.Errorf("unlocking channel accounts: %w", err)
			}
		}
		return nil
	})
	if dbTxErr != nil {
		return fmt.Errorf("ingesting processed data: %w", dbTxErr)
	}

	// Process token changes only during live ingestion (not backfill)
	if m.ingestionMode == IngestionModeLive {
		return m.processLiveIngestionTokenChanges(ctx, indexerBuffer)
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

// extractInnerTxHash takes a transaction XDR binary and returns the hash of its inner transaction.
// For fee bump transactions, it returns the hash of the inner transaction.
// For regular transactions, it returns the hash of the transaction itself.
func (m *ingestService) extractInnerTxHash(txXDR []byte) (string, error) {
	// Convert binary XDR to base64 for the SDK
	txXDRBase64 := base64.StdEncoding.EncodeToString(txXDR)
	genericTx, err := txnbuild.TransactionFromXDR(txXDRBase64)
	if err != nil {
		return "", fmt.Errorf("deserializing envelope xdr: %w", err)
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

// recordStateChangeMetrics aggregates state changes by reason and category, then records metrics.
func (m *ingestService) recordStateChangeMetrics(stateChanges []types.StateChange) {
	counts := make(map[string]int) // key: "reason|category"
	for _, sc := range stateChanges {
		reason := ""
		if sc.StateChangeReason != nil {
			reason = string(*sc.StateChangeReason)
		}
		key := reason + "|" + string(sc.StateChangeCategory)
		counts[key]++
	}
	for key, count := range counts {
		parts := strings.SplitN(key, "|", 2)
		m.metricsService.IncStateChanges(parts[0], parts[1], count)
	}
}
