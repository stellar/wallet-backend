package services

import (
	"context"
	"fmt"
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

type CompressBatch struct {
	batch           *BackfillBatch
	ledgerCloseTime time.Time
}

// BackfillResult tracks the outcome of processing a single batch.
type BackfillResult struct {
	Batch        BackfillBatch
	LedgersCount int
	Duration     time.Duration
	Error        error
	StartTime    time.Time // First ledger close time in batch (for compression)
	EndTime      time.Time // Last ledger close time in batch (for compression)
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

// fetchLedgerCloseTime fetches the ClosedAt timestamp for a ledger
// It creates a temporary ledger backend, fetches the ledger via single-ledger PrepareRange calls, and returns the time.
func (m *ingestService) fetchLedgerCloseTime(ctx context.Context, ledger uint32) (time.Time, error) {
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("creating backend for boundary timestamps: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing boundary timestamp backend: %v", closeErr)
		}
	}()
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(ledger, ledger))
	if err != nil {
		return time.Time{}, fmt.Errorf("preparing range for start ledger %d: %w", ledger, err)
	}
	meta, err := backend.GetLedger(ctx, ledger)
	if err != nil {
		return time.Time{}, fmt.Errorf("getting start ledger %d: %w", ledger, err)
	}
	return meta.ClosedAt(), nil
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

	// Must match ingest.Hypertables — duplicated here to avoid import cycle.
	tables := []string{
		"transactions", "transactions_accounts", "operations",
		"operations_accounts", "state_changes",
	}

	// Fetch boundary timestamps for chunk pre-creation and recompression scoping.
	rangeStartTime, err := m.fetchLedgerCloseTime(ctx, gaps[0].GapStart)
	if err != nil {
		return fmt.Errorf("fetching start ledger %d timestamp: %w", gaps[0].GapStart, err)
	}
	rangeEndTime, err := m.fetchLedgerCloseTime(ctx, gaps[len(gaps)-1].GapEnd)
	if err != nil {
		return fmt.Errorf("fetching end ledger %d timestamp: %w", gaps[len(gaps)-1].GapEnd, err)
	}
	// Suppress insert-triggered autovacuum on parent hypertables during backfill
	// to avoid I/O contention with COPY inserts and compress_chunk.
	if err := db.DisableInsertAutovacuum(ctx, m.models.DB, tables); err != nil {
		return fmt.Errorf("disabling insert autovacuum: %w", err)
	}
	defer func() {
		if restoreErr := db.RestoreInsertAutovacuum(ctx, m.models.DB, tables); restoreErr != nil {
			log.Ctx(ctx).Warnf("Failed to restore insert autovacuum: %v", restoreErr)
		}
	}()

	chunks, err := db.PreCreateChunks(ctx, m.models.DB, tables, rangeStartTime, rangeEndTime)
	if err != nil {
		return fmt.Errorf("pre-creating chunks: %w", err)
	}

	gapBatches, chunksByBatch, err := m.mapBatchesToChunks(ctx, gaps, chunks)
	if err != nil {
		return err
	}
	compressorCh := make(chan *CompressBatch, 64)
	compressor := newProgressiveCompressor(ctx, m.models.DB, chunksByBatch, compressorCh)
	compressor.runCompression()

	pipelinedProcessor := func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
		return m.processLedgersInBatch(ctx, backend, batch, m.flushHistoricalBatch, compressorCh)
	}

	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, gapBatches, pipelinedProcessor)
	duration := time.Since(startTime)

	close(compressorCh)
	analyzeBatchResults(ctx, results)

	// Wait for progressive compression to finish.
	// Compression proceeds even if some batches failed — already-compressed
	// chunks contain valid data and compress_chunk is idempotent.
	compressor.Wait()

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
) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)
	for i, batch := range batches {
		group.Submit(func() {
			results[i] = m.processSingleBatch(ctx, batch, i, len(batches), processBatch)
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
func (m *ingestService) flushHistoricalBatch(ctx context.Context, buffers []*LedgerBuffer) error {
	var lastErr error
	for attempt := range maxIngestProcessedDataRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		_, _, err := m.insertBatchIntoDB(ctx, buffers, insertOpts{backfillMode: true})
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt >= maxIngestProcessedDataRetries-1 {
			log.Ctx(ctx).Errorf("Error flushing historical batch (attempt %d/%d): %v, giving up",
				attempt+1, maxIngestProcessedDataRetries, lastErr)
			break
		}

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

const (
	// pipelineWorkerLimit controls parallel ledger processors in Stage 2.
	// Tuned to ~NumCPU + margin for I/O overlap during buffer sends.
	pipelineWorkerLimit = 11

	// pipelineReaderBuffer keeps the reader ahead of workers to hide GetLedger latency.
	pipelineReaderBuffer = 200
)

// pipelineMetrics tracks worker-side timing for pipeline observability.
// Stage 2 workers update atomics; Stage 3 consumer reads deltas per flush.
type pipelineMetrics struct {
	processDuration   atomic.Int64
	workerBlockedTime atomic.Int64
	workerLedgers     atomic.Int64
	lastProcess       int64
	lastBlocked       int64
	lastLedgers       int64
}

// deltas returns the change in worker metrics since the last call.
func (pm *pipelineMetrics) deltas() (process, blocked time.Duration, ledgers int64) {
	currProcess := pm.processDuration.Load()
	currBlocked := pm.workerBlockedTime.Load()
	currLedgers := pm.workerLedgers.Load()
	process = time.Duration(currProcess - pm.lastProcess)
	blocked = time.Duration(currBlocked - pm.lastBlocked)
	ledgers = currLedgers - pm.lastLedgers
	pm.lastProcess = currProcess
	pm.lastBlocked = currBlocked
	pm.lastLedgers = currLedgers
	return
}

// metaWithTime pairs a ledger's close metadata with its parsed close time.
type metaWithTime struct {
	meta     xdr.LedgerCloseMeta
	closedAt time.Time
}

type LedgerBuffer struct {
	buffer    *indexer.IndexerBuffer
	ledgerSeq uint32
	closeTime time.Time
}

// processLedgersInBatch processes ledgers using a 3-stage streaming pipeline:
//   - Stage 1 (Reader): Sequential GetLedger calls into metaCh
//   - Stage 2 (Workers): Parallel ledger processing into flushCh
//   - Stage 3 (Consumer): Merge buffers + periodic flush to DB
//
// A pipeline-scoped context ensures all stages stop promptly when any stage fails,
// preventing partial DB writes and goroutine leaks.
func (m *ingestService) processLedgersInBatch(
	ctx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
	flush func(context.Context, []*LedgerBuffer) error,
	compressorCh chan<- *CompressBatch,
) BackfillResult {
	result := BackfillResult{Batch: batch}

	// Pipeline-scoped context: any stage failure cancels all stages.
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()

	metaCh := make(chan metaWithTime, pipelineReaderBuffer)
	flushCh := make(chan *LedgerBuffer, m.backfillDBInsertBatchSize*2)
	var metrics pipelineMetrics

	// Launch pipeline stages
	startTime, endTime, readerErr := m.startLedgerReader(pipeCtx, backend, batch, metaCh, pipeCancel)
	workerErr := m.startLedgerWorkers(pipeCtx, metaCh, flushCh, &metrics, pipeCancel)

	// Consume + flush
	batchStart := time.Now()
	ledgersCount, flushDuration, flushErr := m.consumeAndFlush(ctx, &batch, pipeCtx, flushCh, flush, &metrics, pipeCancel, compressorCh)
	result.LedgersCount = ledgersCount
	if flushErr != nil {
		result.Error = flushErr
		return result
	}

	// Check upstream errors
	if err := collectPipelineErrors(readerErr, workerErr); err != nil {
		result.Error = err
		return result
	}

	// Finalize
	if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
		result.Error = err
		return result
	}
	result.StartTime = *startTime
	result.EndTime = *endTime
	m.metricsService.SetOldestLedgerIngested(float64(batch.StartLedger))

	// Summary log
	elapsed := time.Since(batchStart)
	log.Ctx(ctx).Infof("Batch [%d-%d] complete — %d ledgers in %v (%.1f l/s), process: %v, flush: %v",
		batch.StartLedger, batch.EndLedger,
		result.LedgersCount, elapsed, float64(result.LedgersCount)/elapsed.Seconds(),
		time.Duration(metrics.processDuration.Load()), flushDuration)

	return result
}

// startLedgerReader spawns a goroutine that sequentially fetches ledgers via
// GetLedger and sends them to metaCh. It closes metaCh on completion or error.
// The caller reads boundary timestamps from the returned pointers after metaCh drains.
func (m *ingestService) startLedgerReader(
	pipeCtx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
	metaCh chan<- metaWithTime,
	cancel context.CancelFunc,
) (startTime, endTime *time.Time, errCh <-chan error) {
	readerErr := make(chan error, 1)
	var st, et time.Time
	startTime = &st
	endTime = &et

	go func() {
		defer close(metaCh)
		for seq := batch.StartLedger; seq <= batch.EndLedger; seq++ {
			meta, err := m.getLedgerWithRetry(pipeCtx, backend, seq)
			if err != nil {
				readerErr <- fmt.Errorf("getting ledger %d: %w", seq, err)
				cancel()
				return
			}
			lt := meta.ClosedAt()
			if st.IsZero() {
				st = lt
			}
			et = lt

			select {
			case metaCh <- metaWithTime{meta: meta, closedAt: lt}:
			case <-pipeCtx.Done():
				readerErr <- pipeCtx.Err()
				return
			}
		}
	}()

	return startTime, endTime, readerErr
}

// startLedgerWorkers spawns a goroutine that reads from metaCh, processes
// ledgers in parallel (up to pipelineWorkerLimit), and sends IndexerBuffers
// to flushCh. It closes flushCh on completion.
// Channel ownership: caller creates and closes metaCh; workers only read from it.
func (m *ingestService) startLedgerWorkers(
	pipeCtx context.Context,
	metaCh <-chan metaWithTime,
	flushCh chan<- *LedgerBuffer,
	metrics *pipelineMetrics,
	cancel context.CancelFunc,
) <-chan error {
	workerErr := make(chan error, 1)

	go func() {
		defer close(flushCh)
		g, gCtx := errgroup.WithContext(pipeCtx)
		g.SetLimit(pipelineWorkerLimit)

		for item := range metaCh {
			// Check if an upstream stage has failed before merging more data.
			if pipeCtx.Err() != nil {
				break
			}

			g.Go(func() error {
				t := time.Now()
				buf := indexer.NewIndexerBuffer()
				if err := m.processLedgerSequential(gCtx, item.meta, buf); err != nil {
					return err
				}
				metrics.processDuration.Add(int64(time.Since(t)))

				t2 := time.Now()
				select {
				case flushCh <- &LedgerBuffer{
					buffer:    buf,
					ledgerSeq: item.meta.LedgerSequence(),
					closeTime: item.meta.ClosedAt(),
				}:
				case <-gCtx.Done():
					return gCtx.Err()
				}
				metrics.workerBlockedTime.Add(int64(time.Since(t2)))
				metrics.workerLedgers.Add(1)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			workerErr <- err
			cancel()
		}
	}()

	return workerErr
}

// watermarkTracker tracks the highest contiguously-flushed ledger sequence
// to signal safe compression windows. Single-goroutine use only.
type watermarkTracker struct {
	gapBatch     *BackfillBatch
	flushed      []bool
	closeTimes   map[uint32]time.Time
	watermark    uint32
	compressorCh chan<- *CompressBatch
}

func newWatermarkTracker(gapBatch *BackfillBatch, compressorCh chan<- *CompressBatch) *watermarkTracker {
	return &watermarkTracker{
		gapBatch:     gapBatch,
		flushed:      make([]bool, gapBatch.EndLedger-gapBatch.StartLedger+1),
		closeTimes:   make(map[uint32]time.Time),
		watermark:    gapBatch.StartLedger,
		compressorCh: compressorCh,
	}
}

// advance marks ledgers as flushed and signals compression for any new
// contiguously-complete prefix. Prunes consumed closeTimes entries.
func (w *watermarkTracker) advance(buffers []*LedgerBuffer) {
	for _, item := range buffers {
		w.flushed[item.ledgerSeq-w.gapBatch.StartLedger] = true
		w.closeTimes[item.ledgerSeq] = item.closeTime
	}
	prev := w.watermark
	for w.watermark <= w.gapBatch.EndLedger && w.flushed[w.watermark-w.gapBatch.StartLedger] {
		w.watermark++
	}
	if w.watermark > prev {
		// watermark points to first unflushed; watermark-1 is the last flushed.
		closeTime := w.closeTimes[w.watermark-1]
		// Prune consumed entries below new watermark.
		for seq := prev; seq < w.watermark; seq++ {
			delete(w.closeTimes, seq)
		}
		w.compressorCh <- &CompressBatch{
			batch:           w.gapBatch,
			ledgerCloseTime: closeTime,
		}
	}
}

// executeFlush persists a batch to DB and advances the watermark.
func executeFlush(
	pipeCtx context.Context,
	batch []*LedgerBuffer,
	flush func(context.Context, []*LedgerBuffer) error,
	wt *watermarkTracker,
) (time.Duration, error) {
	t := time.Now()
	if err := flush(pipeCtx, batch); err != nil {
		return 0, err
	}
	elapsed := time.Since(t)
	wt.advance(batch)
	return elapsed, nil
}

// consumeAndFlush collects incoming IndexerBuffers and periodically flushes to DB.
// Runs as a single goroutine per batch — watermark mutations are safe without locks.
// Returns the number of ledgers processed, total flush duration, and any flush error.
func (m *ingestService) consumeAndFlush(
	ctx context.Context,
	gapBatch *BackfillBatch,
	pipeCtx context.Context,
	flushCh <-chan *LedgerBuffer,
	flush func(context.Context, []*LedgerBuffer) error,
	metrics *pipelineMetrics,
	cancel context.CancelFunc,
	compressorCh chan<- *CompressBatch,
) (ledgersCount int, flushDuration time.Duration, err error) {
	batch := make([]*LedgerBuffer, 0, m.backfillDBInsertBatchSize)
	flushCount := 0
	lastFlushEnd := time.Now()
	wt := newWatermarkTracker(gapBatch, compressorCh)

	for ledgerBuffer := range flushCh {
		// Check if an upstream stage has failed before collecting more data.
		if pipeCtx.Err() != nil {
			break
		}

		batch = append(batch, ledgerBuffer)
		ledgersCount++

		if len(batch) >= int(m.backfillDBInsertBatchSize) {
			fillTime := time.Since(lastFlushEnd)
			flushCount++

			flushElapsed, flushErr := executeFlush(pipeCtx, batch, flush, wt)
			if flushErr != nil {
				cancel()
				return ledgersCount, flushDuration, flushErr
			}
			flushDuration += flushElapsed
			lastFlushEnd = time.Now()

			deltaProcess, deltaBlocked, deltaLedgers := metrics.deltas()
			log.Ctx(ctx).Infof("Pipeline flush #%d: fill=%v flush=%v total=%d | workers: Δcpu=%v Δblocked=%v (%d processed)",
				flushCount, fillTime, flushElapsed, ledgersCount,
				deltaProcess, deltaBlocked, deltaLedgers)

			batch = batch[:0]
		}
	}

	// Final flush
	if len(batch) > 0 && pipeCtx.Err() == nil {
		fillTime := time.Since(lastFlushEnd)
		chLen := len(flushCh)
		flushCount++

		flushElapsed, flushErr := executeFlush(pipeCtx, batch, flush, wt)
		if flushErr != nil {
			cancel()
			return ledgersCount, flushDuration, flushErr
		}
		flushDuration += flushElapsed

		log.Ctx(ctx).Infof("Pipeline flush #%d (final): fill=%v flush=%v chBacklog=%d/%d total=%d",
			flushCount, fillTime, flushElapsed, chLen, cap(flushCh), ledgersCount)
	}

	return ledgersCount, flushDuration, nil
}

// collectPipelineErrors drains the reader and worker error channels,
// returning the first error found (if any).
func collectPipelineErrors(readerErr, workerErr <-chan error) error {
	select {
	case err := <-readerErr:
		return err
	default:
	}
	select {
	case err := <-workerErr:
		return err
	default:
	}
	return nil
}

// timeRangesOverlap returns true if [aStart, aEnd] overlaps [bStart, bEnd].
func timeRangesOverlap(aStart, aEnd, bStart, bEnd time.Time) bool {
	return !aStart.After(bEnd) && !aEnd.Before(bStart)
}

// mapBatchesToChunks converts gaps to BackfillBatches and maps each batch to the
// pre-created chunks whose time range overlaps the batch's ledger close times.
func (m *ingestService) mapBatchesToChunks(
	ctx context.Context,
	gaps []data.LedgerRange,
	chunks []*db.Chunk,
) ([]BackfillBatch, map[BackfillBatch][]*db.Chunk, error) {
	batches := make([]BackfillBatch, len(gaps))
	chunksByBatch := make(map[BackfillBatch][]*db.Chunk)
	for i, gap := range gaps {
		batches[i] = BackfillBatch{StartLedger: gap.GapStart, EndLedger: gap.GapEnd}
		startTime, err := m.fetchLedgerCloseTime(ctx, gap.GapStart)
		if err != nil {
			return nil, nil, fmt.Errorf("fetching start ledger %d timestamp: %w", gap.GapStart, err)
		}
		endTime, err := m.fetchLedgerCloseTime(ctx, gap.GapEnd)
		if err != nil {
			return nil, nil, fmt.Errorf("fetching end ledger %d timestamp: %w", gap.GapEnd, err)
		}
		for _, chunk := range chunks {
			if timeRangesOverlap(startTime, endTime, chunk.Start, chunk.End) {
				chunk.NumWriters.Add(1)
				chunksByBatch[batches[i]] = append(chunksByBatch[batches[i]], chunk)
			}
		}
	}
	return batches, chunksByBatch, nil
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
