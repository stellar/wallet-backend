package services

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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

	backfillBatches := m.splitGapsIntoBatches(gaps)

	// Create progressive recompressor.
	// Recompresses chunks as contiguous batches complete rather than waiting until the end.
	tables := []string{
		"transactions", "transactions_accounts", "operations",
		"operations_accounts", "state_changes",
	}
	recompressor := newProgressiveRecompressor(ctx, m.models.DB, tables, len(backfillBatches))

	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, backfillBatches, m.processLedgersInBatchHistorical, func(batchIdx int, result BackfillResult) {
		recompressor.MarkDone(batchIdx, result.StartTime, result.EndTime)
	})
	duration := time.Since(startTime)

	analyzeBatchResults(ctx, results)

	// Wait for progressive compression to finish.
	// Compression proceeds even if some batches failed — already-compressed
	// chunks contain valid data and compress_chunk is idempotent.
	recompressor.Wait()

	log.Ctx(ctx).Infof("Historical backfill completed in %v: %d batches", duration, len(backfillBatches))
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
// If updateCursorTo is non-nil, it also updates the oldest cursor atomically.
func (m *ingestService) flushHistoricalBatch(ctx context.Context, buffer *indexer.IndexerBuffer, updateCursorTo *uint32) error {
	var lastErr error
	for attempt := range maxIngestProcessedDataRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
				return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
			}
			if _, _, err := m.insertIntoDB(ctx, dbTx, buffer); err != nil {
				return fmt.Errorf("inserting processed data into db: %w", err)
			}
			if err := m.unlockChannelAccounts(ctx, dbTx, buffer.GetTransactions()); err != nil {
				return fmt.Errorf("unlocking channel accounts: %w", err)
			}
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

// processLedgersInBatchHistorical processes all ledgers in a batch for historical backfill.
// The cursor is updated atomically with the final data flush.
// Returns a BackfillResult with the count of ledgers processed and time range.
func (m *ingestService) processLedgersInBatchHistorical(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
	result := BackfillResult{Batch: batch}
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	var startTime, endTime time.Time

	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			result.Error = fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			return result
		}

		ledgerTime := ledgerMeta.ClosedAt()
		if startTime.IsZero() {
			startTime = ledgerTime
		}
		endTime = ledgerTime

		if err := m.processLedger(ctx, ledgerMeta, batchBuffer); err != nil {
			result.Error = fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
			return result
		}
		result.LedgersCount++
		ledgersInBuffer++

		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			if err := m.flushHistoricalBatch(ctx, batchBuffer, nil); err != nil {
				result.Error = err
				return result
			}
			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Final flush with cursor update
	if ledgersInBuffer > 0 {
		if err := m.flushHistoricalBatch(ctx, batchBuffer, &batch.StartLedger); err != nil {
			result.Error = err
			return result
		}
	} else {
		// All data was flushed in intermediate batches, but we still need to update the cursor
		if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
			result.Error = err
			return result
		}
	}

	result.StartTime = startTime
	result.EndTime = endTime
	m.metricsService.SetOldestLedgerIngested(float64(batch.StartLedger))
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


