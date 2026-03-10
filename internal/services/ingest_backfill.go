package services

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

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
	BatchChanges *BatchChanges // Only populated for catchup mode
	StartTime    time.Time     // First ledger close time in batch (for compression)
	EndTime      time.Time     // Last ledger close time in batch (for compression)
}

// analyzeBatchResults aggregates backfill batch results and logs any failures.
func analyzeBatchResults(ctx context.Context, results []BackfillResult) int {
	numFailed := 0
	for _, result := range results {
		if result.Error != nil {
			numFailed++
			log.Ctx(ctx).Errorf("Batch [%d-%d] failed: %v",
				result.Batch.StartLedger, result.Batch.EndLedger, result.Error)
		}
	}
	log.Ctx(ctx).Infof("Backfilling completed: %d/%d batches failed", numFailed, len(results))
	return numFailed
}

// fetchBoundaryTimestamps fetches the ClosedAt timestamps for the start and end
// ledgers of a backfill range. It creates a temporary ledger backend, fetches the
// two boundary ledgers via single-ledger PrepareRange calls, and returns their times.
func (m *ingestService) fetchBoundaryTimestamps(ctx context.Context, startLedger, endLedger uint32) (time.Time, time.Time, error) {
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("creating backend for boundary timestamps: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing boundary timestamp backend: %v", closeErr)
		}
	}()

	// Fetch start ledger timestamp
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(startLedger, startLedger))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("preparing range for start ledger %d: %w", startLedger, err)
	}
	startMeta, err := backend.GetLedger(ctx, startLedger)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("getting start ledger %d: %w", startLedger, err)
	}
	startTime := startMeta.ClosedAt()

	// Fetch end ledger timestamp
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(endLedger, endLedger))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("preparing range for end ledger %d: %w", endLedger, err)
	}
	endMeta, err := backend.GetLedger(ctx, endLedger)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("getting end ledger %d: %w", endLedger, err)
	}
	endTime := endMeta.ClosedAt()

	log.Ctx(ctx).Infof("Boundary timestamps: start ledger %d at %s, end ledger %d at %s",
		startLedger, startTime.Format(time.RFC3339), endLedger, endTime.Format(time.RFC3339))

	return startTime, endTime, nil
}

// startHistoricalBackfill processes ledgers in the specified range, identifying gaps
// and processing them in parallel batches. Fills gaps within already-ingested range.
func (m *ingestService) startHistoricalBackfill(ctx context.Context, startLedger, endLedger uint32) error {
	if startLedger > endLedger {
		return fmt.Errorf("start ledger cannot be greater than end ledger")
	}

	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	if endLedger > latestIngestedLedger {
		return fmt.Errorf("end ledger %d cannot be greater than latest ingested ledger %d for backfilling", endLedger, latestIngestedLedger)
	}

	gaps, err := m.calculateBackfillGaps(ctx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("calculating backfill gaps: %w", err)
	}
	if len(gaps) == 0 {
		log.Ctx(ctx).Infof("No gaps to backfill in range [%d - %d]", startLedger, endLedger)
		return nil
	}

	tables := []string{
		"transactions", "transactions_accounts", "operations",
		"operations_accounts", "state_changes",
	}

	// Fetch boundary timestamps for chunk pre-creation and recompression scoping.
	// boundaryStart is the chunk-boundary-aligned start timestamp used by the recompressor
	// to scope chunk queries to the backfill range.
	var boundaryStart time.Time
	if m.chunkInterval != "" {
		rangeStart := gaps[0].GapStart
		rangeEnd := gaps[len(gaps)-1].GapEnd
		var boundaryEnd time.Time
		var err2 error
		boundaryStart, boundaryEnd, err2 = m.fetchBoundaryTimestamps(ctx, rangeStart, rangeEnd)
		if err2 != nil {
			return fmt.Errorf("fetching boundary timestamps: %w", err2)
		}
		var err3 error
		boundaryStart, err3 = db.PreCreateChunks(ctx, m.models.DB, tables, boundaryStart, boundaryEnd)
		if err3 != nil {
			return fmt.Errorf("pre-creating chunks: %w", err3)
		}
		if err := db.DropIndexesOnChunksInRange(ctx, m.models.DB, tables, boundaryStart, boundaryEnd); err != nil {
			return fmt.Errorf("dropping indexes on chunks: %w", err)
		}
	}

	// Convert gaps directly to BackfillBatches (1 per gap — no batch splitting).
	// Each gap gets a single backend, avoiding redundant S3 downloads across 250-ledger batches.
	gapBatches := make([]BackfillBatch, len(gaps))
	for i, gap := range gaps {
		gapBatches[i] = BackfillBatch{StartLedger: gap.GapStart, EndLedger: gap.GapEnd}
	}

	// Create progressive recompressor.
	// Recompresses chunks as contiguous batches complete rather than waiting until the end.
	// boundaryStart scopes chunk queries to the backfill range (zero time if no pre-creation).
	recompressor := newProgressiveRecompressor(ctx, m.models.DB, tables, len(gapBatches), boundaryStart)

	pipelinedProcessor := func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
		return m.processLedgersInBatchPipelined(ctx, backend, batch, m.flushHistoricalBatch)
	}

	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, gapBatches, pipelinedProcessor, func(batchIdx int, result BackfillResult) {
		recompressor.MarkDone(batchIdx, result.StartTime, result.EndTime)
	})
	duration := time.Since(startTime)

	analyzeBatchResults(ctx, results)

	// Wait for progressive compression to finish.
	// Compression proceeds even if some batches failed — already-compressed
	// chunks contain valid data and compress_chunk is idempotent.
	recompressor.Wait()

	log.Ctx(ctx).Infof("Historical backfill completed in %v: %d gaps", duration, len(gapBatches))
	return nil
}

// calculateBackfillGaps determines which ledger ranges need to be backfilled based on
// the requested range, oldest ingested ledger, and any existing gaps in the data.
func (m *ingestService) calculateBackfillGaps(ctx context.Context, startLedger, endLedger uint32) ([]data.LedgerRange, error) {
	oldestIngestedLedger, err := m.models.IngestStore.GetOldestLedger(ctx)
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

// batchProcessor is a function that processes all ledgers in a batch and returns its result.
type batchProcessor func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult

// processBackfillBatchesParallel processes backfill batches in parallel using a worker pool.
// Each batch is processed by the provided batchProcessor function.
// The optional onBatchComplete callback is called after each successful batch.
func (m *ingestService) processBackfillBatchesParallel(
	ctx context.Context,
	batches []BackfillBatch,
	processBatch batchProcessor,
	onBatchComplete func(batchIdx int, result BackfillResult),
) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		group.Submit(func() {
			results[i] = m.processSingleBatch(ctx, batch, i, len(batches), processBatch)
			if onBatchComplete != nil && results[i].Error == nil {
				onBatchComplete(i, results[i])
			}
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Warnf("Backfill batch group wait returned error: %v", err)
	}

	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, batch BackfillBatch, batchIndex, totalBatches int, processBatch batchProcessor) BackfillResult {
	start := time.Now()

	// Setup backend
	backend, err := m.setupBatchBackend(ctx, batch)
	if err != nil {
		return BackfillResult{Batch: batch, Error: err, Duration: time.Since(start)}
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing ledger backend for batch [%d-%d]: %v", batch.StartLedger, batch.EndLedger, closeErr)
		}
	}()

	result := processBatch(ctx, backend, batch)
	result.Duration = time.Since(start)

	if result.Error == nil {
		log.Ctx(ctx).Infof("Batch %d/%d [%d - %d] completed: %d ledgers in %v",
			batchIndex+1, totalBatches, batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)
	}

	return result
}

// setupBatchBackend creates and prepares a ledger backend for a batch range.
// Caller is responsible for calling Close() on the returned backend.
func (m *ingestService) setupBatchBackend(ctx context.Context, batch BackfillBatch) (ledgerbackend.LedgerBackend, error) {
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating ledger backend: %w", err)
	}

	ledgerRange := ledgerbackend.BoundedRange(batch.StartLedger, batch.EndLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return nil, fmt.Errorf("preparing backend range: %w", err)
	}

	return backend, nil
}

// flushHistoricalBatch persists buffered data to the database within a transaction.
func (m *ingestService) flushHistoricalBatch(ctx context.Context, buffer *indexer.IndexerBuffer) error {
	var lastErr error
	for attempt := range maxIngestProcessedDataRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		_, _, err := m.insertIntoDB(ctx, buffer, insertOpts{backfillMode: true})
		if err == nil {
			return nil
		}
		lastErr = err

		backoff := min(time.Duration(1<<attempt)*time.Second, maxIngestProcessedDataRetryBackoff)
		log.Ctx(ctx).Warnf("Error flushing historical batch (attempt %d/%d): %v, retrying in %v...",
			attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return lastErr
}

// processLedgersInBatchPipelined processes ledgers using a 3-stage streaming pipeline:
//   - Stage 1 (Reader): Sequential GetLedger calls into metaCh
//   - Stage 2 (Workers): Parallel ledger processing (NumCPU workers) into flushCh
//   - Stage 3 (Consumer): Merge buffers + periodic flush to DB
//
// This eliminates the two synchronization barriers present in the sequential approach
// (all-reads-before-process, all-process-before-flush) by streaming ledgers through
// the pipeline individually.
func (m *ingestService) processLedgersInBatchPipelined(
	ctx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
	flush func(context.Context, *indexer.IndexerBuffer) error,
) BackfillResult {
	result := BackfillResult{Batch: batch}

	type metaWithTime struct {
		meta     xdr.LedgerCloseMeta
		closedAt time.Time
	}
	metaCh := make(chan metaWithTime, 2)
	flushCh := make(chan *indexer.IndexerBuffer, runtime.NumCPU()*2)

	// --- Stage 1: Reader — sequential GetLedger into metaCh ---
	readerErr := make(chan error, 1)
	var startTime, endTime time.Time
	go func() {
		defer close(metaCh)
		for seq := batch.StartLedger; seq <= batch.EndLedger; seq++ {
			meta, err := m.getLedgerWithRetry(ctx, backend, seq)
			if err != nil {
				readerErr <- fmt.Errorf("getting ledger %d: %w", seq, err)
				return
			}
			lt := meta.ClosedAt()
			if startTime.IsZero() {
				startTime = lt
			}
			endTime = lt

			select {
			case metaCh <- metaWithTime{meta: meta, closedAt: lt}:
			case <-ctx.Done():
				readerErr <- ctx.Err()
				return
			}
		}
	}()

	// --- Stage 2: Workers — parallel process, send directly to flushCh ---
	var processDuration atomic.Int64
	var workerBlockedTime atomic.Int64 // time blocked waiting to send on flushCh
	var workerLedgers atomic.Int64
	workerErr := make(chan error, 1)
	go func() {
		defer close(flushCh)
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(4)

		for mwt := range metaCh {
			g.Go(func() error {
				t := time.Now()
				buf := indexer.NewIndexerBuffer()
				if err := m.processLedgerSequential(gCtx, mwt.meta, buf); err != nil {
					return err
				}
				processDuration.Add(int64(time.Since(t)))

				t2 := time.Now()
				flushCh <- buf
				workerBlockedTime.Add(int64(time.Since(t2)))
				workerLedgers.Add(1)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			workerErr <- err
		}
	}()

	// --- Stage 3: Consumer — merge + flush ---
	batchStart := time.Now()
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	var flushDuration time.Duration
	flushCount := 0
	lastFlushEnd := time.Now()

	// Snapshot vars for per-flush deltas
	var lastProcessDuration int64
	var lastWorkerBlocked int64
	var lastWorkerLedgers int64

	for buf := range flushCh {
		batchBuffer.Merge(buf)
		ledgersInBuffer++
		result.LedgersCount++

		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			fillTime := time.Since(lastFlushEnd)
			flushCount++

			t := time.Now()
			if err := flush(ctx, batchBuffer); err != nil {
				result.Error = err
				return result
			}
			flushElapsed := time.Since(t)
			flushDuration += flushElapsed
			chProducedDuringFlush := len(flushCh) // what Stage 2 queued while we were flushing
			lastFlushEnd = time.Now()

			// Compute per-flush deltas
			currProcess := processDuration.Load()
			currBlocked := workerBlockedTime.Load()
			currLedgers := workerLedgers.Load()
			deltaProcess := currProcess - lastProcessDuration
			deltaBlocked := currBlocked - lastWorkerBlocked
			deltaLedgers := currLedgers - lastWorkerLedgers
			lastProcessDuration = currProcess
			lastWorkerBlocked = currBlocked
			lastWorkerLedgers = currLedgers

			log.Ctx(ctx).Infof("Pipeline flush #%d: fill=%v flush=%v chQueued=%d/%d ledgers=%d total=%d | workers: Δcpu=%v Δblocked=%v (%d processed)",
				flushCount, fillTime, flushElapsed, chProducedDuringFlush, cap(flushCh), ledgersInBuffer, result.LedgersCount,
				time.Duration(deltaProcess), time.Duration(deltaBlocked), deltaLedgers)

			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Final flush
	if ledgersInBuffer > 0 {
		fillTime := time.Since(lastFlushEnd)
		chLen := len(flushCh)
		flushCount++

		t := time.Now()
		if err := flush(ctx, batchBuffer); err != nil {
			result.Error = err
			return result
		}
		flushElapsed := time.Since(t)
		flushDuration += flushElapsed

		log.Ctx(ctx).Infof("Pipeline flush #%d (final): fill=%v flush=%v chBacklog=%d/%d ledgers=%d total=%d",
			flushCount, fillTime, flushElapsed, chLen, cap(flushCh), ledgersInBuffer, result.LedgersCount)
	}

	// Check for upstream errors
	select {
	case err := <-readerErr:
		result.Error = err
		return result
	default:
	}
	select {
	case err := <-workerErr:
		result.Error = err
		return result
	default:
	}

	// Cursor update
	if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
		result.Error = err
		return result
	}

	result.StartTime = startTime
	result.EndTime = endTime
	m.metricsService.SetOldestLedgerIngested(float64(batch.StartLedger))

	elapsed := time.Since(batchStart)
	throughput := float64(result.LedgersCount) / elapsed.Seconds()
	log.Ctx(ctx).Infof("Batch [%d-%d] complete — %d ledgers in %v (%.1f l/s), process: %v, flush: %v",
		batch.StartLedger, batch.EndLedger,
		result.LedgersCount, elapsed, throughput,
		time.Duration(processDuration.Load()), flushDuration)

	return result
}

// updateOldestCursor updates the oldest ledger cursor to the given ledger.
func (m *ingestService) updateOldestCursor(ctx context.Context, ledgerSeq uint32) error {
	err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		return m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, ledgerSeq)
	})
	if err != nil {
		return fmt.Errorf("updating oldest ledger cursor: %w", err)
	}
	return nil
}
