package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

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

// startBackfilling processes ledgers in the specified range, identifying gaps
// and processing them in parallel batches for historical backfill.
func (m *ingestService) startBackfilling(ctx context.Context, startLedger, endLedger uint32) error {
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

	backfillBatches := m.splitGapsIntoBatches(gaps)

	// Create progressive recompressor that compresses chunks as contiguous batches complete.
	tables := []string{
		"transactions", "transactions_accounts", "operations",
		"operations_accounts", "state_changes",
	}
	recompressor := newProgressiveRecompressor(ctx, m.models.DB, tables, len(backfillBatches))

	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, backfillBatches, recompressor)
	duration := time.Since(startTime)

	analyzeBatchResults(ctx, results)

	// Wait for progressive compression to finish.
	// Compression proceeds even if some batches failed — already-compressed
	// chunks contain valid data and compress_chunk is idempotent.
	recompressor.Wait()

	log.Ctx(ctx).Infof("Backfilling completed in %v: %d batches", duration, len(backfillBatches))
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

// splitGapsIntoBatches divides ledger gaps into fixed-size batches for parallel processing.
func (m *ingestService) splitGapsIntoBatches(gaps []data.LedgerRange) []BackfillBatch {
	var batches []BackfillBatch

	for _, gap := range gaps {
		start := gap.GapStart
		for start <= gap.GapEnd {
			end := min(start+m.backfillBatchSize-1, gap.GapEnd)
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
// Data is inserted uncompressed; the progressive compressor compresses chunks via
// compress_chunk() as contiguous batches complete.
func (m *ingestService) processBackfillBatchesParallel(ctx context.Context, batches []BackfillBatch, recompressor *progressiveRecompressor) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		group.Submit(func() {
			results[i] = m.processSingleBatch(ctx, batch, i, len(batches))
			if results[i].Error == nil {
				recompressor.MarkDone(i, results[i].StartTime, results[i].EndTime)
			}
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Warnf("Backfill batch group wait returned error: %v", err)
	}

	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, batch BackfillBatch, batchIndex, totalBatches int) BackfillResult {
	start := time.Now()
	result := BackfillResult{Batch: batch}

	// Setup backend
	backend, err := m.setupBatchBackend(ctx, batch)
	if err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing ledger backend for batch [%d-%d]: %v", batch.StartLedger, batch.EndLedger, closeErr)
		}
	}()

	// Process all ledgers in batch (cursor is updated atomically with final flush)
	ledgersCount, batchStartTime, batchEndTime, err := m.processLedgersInBatch(ctx, backend, batch)
	result.LedgersCount = ledgersCount
	result.StartTime = batchStartTime
	result.EndTime = batchEndTime
	if err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result
	}

	m.appMetrics.Ingestion.OldestLedger.Set(float64(batch.StartLedger))

	result.Duration = time.Since(start)
	log.Ctx(ctx).Infof("Batch %d/%d [%d - %d] completed: %d ledgers in %v",
		batchIndex+1, totalBatches, batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

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

// flushBatchBufferWithRetry persists buffered data to the database within a transaction.
// If updateCursorTo is non-nil, it also updates the oldest cursor atomically.
func (m *ingestService) flushBatchBufferWithRetry(ctx context.Context, buffer *indexer.IndexerBuffer, updateCursorTo *uint32) error {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			// Disable synchronous commit for this transaction only — safe for backfill
			// since data can be re-ingested if a crash occurs before WAL flush.
			if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
				return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
			}
			txs := buffer.GetTransactions()
			if _, _, err := m.insertIntoDB(ctx, dbTx, txs, buffer); err != nil {
				return fmt.Errorf("inserting processed data into db: %w", err)
			}
			// Unlock channel accounts using all transactions (not filtered)
			if err := m.unlockChannelAccounts(ctx, dbTx, txs); err != nil {
				return fmt.Errorf("unlocking channel accounts: %w", err)
			}
			// Update cursor atomically with data insertion if requested
			if updateCursorTo != nil {
				if err := m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, *updateCursorTo); err != nil {
					return fmt.Errorf("updating oldest cursor: %w", err)
				}
			}
			return nil
		})
		if err == nil {
			return nil
		}
		lastErr = err
		m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush").Inc()

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxIngestProcessedDataRetryBackoff {
			backoff = maxIngestProcessedDataRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error flushing batch buffer (attempt %d/%d): %v, retrying in %v...",
			attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("batch_flush").Inc()
	return lastErr
}

// processLedgersInBatch processes all ledgers in a batch, flushing to DB periodically.
// The cursor is updated atomically with the final data flush.
// Returns the count of ledgers processed and the time range of the batch.
func (m *ingestService) processLedgersInBatch(
	ctx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
) (int, time.Time, time.Time, error) {
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	ledgersProcessed := 0
	var startTime, endTime time.Time

	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			return ledgersProcessed, startTime, endTime, fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
		}

		// Track time range for compression
		ledgerTime := ledgerMeta.ClosedAt()
		if startTime.IsZero() {
			startTime = ledgerTime
		}
		endTime = ledgerTime

		if err := m.processLedger(ctx, ledgerMeta, batchBuffer); err != nil {
			return ledgersProcessed, startTime, endTime, fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
		}
		ledgersProcessed++
		ledgersInBuffer++

		// Flush buffer periodically to control memory usage (intermediate flushes, no cursor update)
		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			if err := m.flushBatchBufferWithRetry(ctx, batchBuffer, nil); err != nil {
				return ledgersProcessed, startTime, endTime, err
			}
			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Final flush with cursor update
	if ledgersInBuffer > 0 {
		if err := m.flushBatchBufferWithRetry(ctx, batchBuffer, &batch.StartLedger); err != nil {
			return ledgersProcessed, startTime, endTime, err
		}
	} else {
		// All data was flushed in intermediate batches, but we still need to update the cursor
		// This happens when ledgersInBuffer == 0 (exact multiple of batch size)
		if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
			return ledgersProcessed, startTime, endTime, err
		}
	}

	return ledgersProcessed, startTime, endTime, nil
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

// progressiveRecompressor compresses uncompressed TimescaleDB chunks as they become safe during backfill.
// Tracks batch completion via a watermark to determine when chunks are fully written.
type progressiveRecompressor struct {
	pool   *pgxpool.Pool
	tables []string
	ctx    context.Context

	mu           sync.Mutex
	completed    []bool
	endTimes     []time.Time
	watermarkIdx int       // index of highest contiguous completed batch (-1 = none)
	globalStart  time.Time // lower bound for chunk queries (batch 0's StartTime)

	triggerCh chan time.Time // safeEnd for recompression window
	done      chan struct{}
}

// newProgressiveRecompressor creates a compressor that progressively compresses uncompressed chunks
// as contiguous batches complete. Starts a background goroutine for compression work.
func newProgressiveRecompressor(ctx context.Context, pool *pgxpool.Pool, tables []string, totalBatches int) *progressiveRecompressor {
	r := &progressiveRecompressor{
		pool:         pool,
		tables:       tables,
		ctx:          ctx,
		completed:    make([]bool, totalBatches),
		endTimes:     make([]time.Time, totalBatches),
		watermarkIdx: -1,
		triggerCh:    make(chan time.Time, totalBatches),
		done:         make(chan struct{}),
	}
	go r.runCompression()
	return r
}

// MarkDone records a batch as complete and advances the watermark if possible.
// If the watermark advances, triggers recompression of chunks in the safe window.
func (r *progressiveRecompressor) MarkDone(batchIdx int, startTime, endTime time.Time) {
	var safeEnd time.Time
	r.mu.Lock()
	r.completed[batchIdx] = true
	r.endTimes[batchIdx] = endTime

	// Record global start from batch 0 (earliest time boundary for queries)
	if batchIdx == 0 {
		r.globalStart = startTime
	}

	// Advance watermark past contiguous completed batches
	oldWatermark := r.watermarkIdx
	for r.watermarkIdx+1 < len(r.completed) && r.completed[r.watermarkIdx+1] {
		r.watermarkIdx++
	}

	sendToChannel := (r.watermarkIdx > oldWatermark)
	if sendToChannel {
		safeEnd = r.endTimes[r.watermarkIdx]
	}
	r.mu.Unlock()

	// If watermark advanced then we trigger recompression outside the lock
	if sendToChannel {
		r.triggerCh <- safeEnd
	}
}

// Wait closes the trigger channel and waits for background compression to finish.
func (r *progressiveRecompressor) Wait() {
	close(r.triggerCh)
	<-r.done
}

// runCompression processes compression triggers in the background.
// For each safe window, queries and compresses uncompressed chunks per table.
func (r *progressiveRecompressor) runCompression() {
	defer close(r.done)

	totalCompressed := 0
	for safeEnd := range r.triggerCh {
		for _, table := range r.tables {
			count := r.compressTableChunks(table, safeEnd)
			totalCompressed += count
		}
	}

	log.Ctx(r.ctx).Infof("Progressive compression complete: %d total chunks compressed", totalCompressed)
}

// compressTableChunks compresses uncompressed chunks for a single table within the safe window.
// Queries chunks where range_end falls within (globalStart, safeEnd) to catch the leading
// boundary chunk that overlaps globalStart.
func (r *progressiveRecompressor) compressTableChunks(table string, safeEnd time.Time) int {
	rows, err := r.pool.Query(r.ctx,
		`SELECT c.chunk_schema || '.' || c.chunk_name
		 FROM timescaledb_information.chunks c
		 WHERE c.hypertable_name = $1
		   AND NOT c.is_compressed
		   AND c.range_end < $2::timestamptz
		   AND c.range_end > $3::timestamptz`,
		table, safeEnd, r.globalStart)
	if err != nil {
		log.Ctx(r.ctx).Warnf("Failed to get chunks for %s: %v", table, err)
		return 0
	}
	defer rows.Close()

	var chunks []string
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			log.Ctx(r.ctx).Warnf("Failed to scan chunk row for table %s: %v", table, err)
			continue
		}
		chunks = append(chunks, chunk)
	}

	compressed := 0
	for _, chunk := range chunks {
		select {
		case <-r.ctx.Done():
			log.Ctx(r.ctx).Warnf("Compression cancelled for %s after %d chunks", table, compressed)
			return compressed
		default:
		}

		_, err := r.pool.Exec(r.ctx, `SELECT compress_chunk($1::regclass)`, chunk)
		if err != nil {
			log.Ctx(r.ctx).Warnf("Failed to compress chunk %s: %v", chunk, err)
			continue
		}
		compressed++
		log.Ctx(r.ctx).Debugf("Compressed chunk for %s: %s", table, chunk)
	}

	if compressed > 0 {
		log.Ctx(r.ctx).Infof("Compressed %d chunks for table %s (window end: %s)",
			compressed, table, safeEnd.Format(time.RFC3339))
	}

	return compressed
}
