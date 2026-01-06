package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// BackfillMode indicates the purpose of backfilling.
type BackfillMode int

const (
	// BackfillModeHistorical fills gaps within already-ingested ledger range.
	BackfillModeHistorical BackfillMode = iota
	// BackfillModeCatchup fills forward gaps to catch up to network tip.
	BackfillModeCatchup
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
// and processing them in parallel batches. The mode parameter determines:
// - BackfillModeHistorical: fills gaps within already-ingested range
// - BackfillModeCatchup: catches up to network tip from latest ingested ledger
func (m *ingestService) startBackfilling(ctx context.Context, startLedger, endLedger uint32, mode BackfillMode) error {
	if startLedger > endLedger {
		return fmt.Errorf("start ledger cannot be greater than end ledger")
	}

	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	// Validate based on mode
	switch mode {
	case BackfillModeHistorical:
		if endLedger > latestIngestedLedger {
			return fmt.Errorf("end ledger %d cannot be greater than latest ingested ledger %d for backfilling", endLedger, latestIngestedLedger)
		}
	case BackfillModeCatchup:
		if startLedger != latestIngestedLedger+1 {
			return fmt.Errorf("catchup must start from ledger %d (latestIngestedLedger + 1), got %d", latestIngestedLedger+1, startLedger)
		}
	}

	// Determine gaps to fill based on mode
	var gaps []data.LedgerRange
	if mode == BackfillModeCatchup {
		// For catchup, treat entire range as a single gap (no existing data in this range)
		gaps = []data.LedgerRange{{GapStart: startLedger, GapEnd: endLedger}}
	} else {
		gaps, err = m.calculateBackfillGaps(ctx, startLedger, endLedger)
		if err != nil {
			return fmt.Errorf("calculating backfill gaps: %w", err)
		}
	}
	if len(gaps) == 0 {
		log.Ctx(ctx).Infof("No gaps to backfill in range [%d - %d]", startLedger, endLedger)
		return nil
	}

	backfillBatches := m.splitGapsIntoBatches(gaps)
	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, mode, backfillBatches)
	duration := time.Since(startTime)

	numFailedBatches := analyzeBatchResults(ctx, results)

	// Update latest ledger cursor for catchup mode
	if mode == BackfillModeCatchup {
		if numFailedBatches > 0 {
			return fmt.Errorf("optimized catchup failed: %d/%d batches failed", numFailedBatches, len(backfillBatches))
		}
		if err := m.updateLatestLedgerCursorAfterCatchup(ctx, endLedger); err != nil {
			return fmt.Errorf("updating latest cursor after catchup: %w", err)
		}
	}

	log.Ctx(ctx).Infof("Backfilling completed in %v: %d batches", duration, len(backfillBatches))
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
func (m *ingestService) processBackfillBatchesParallel(ctx context.Context, mode BackfillMode, batches []BackfillBatch) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		group.Submit(func() {
			result := m.processSingleBatch(ctx, mode, batch)
			results[i] = result
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Warnf("Backfill batch group wait returned error: %v", err)
	}
	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, mode BackfillMode, batch BackfillBatch) BackfillResult {
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

	err = db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		// Process each ledger in the batch sequentially
		for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
			ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
			if err != nil {
				return fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			}

			err = m.processBackfillLedger(ctx, ledgerMeta, batchBuffer)
			if err != nil {
				return fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
			}
			result.LedgersCount++
			ledgersInBuffer++

			// Flush buffer periodically to control memory usage
			if ledgersInBuffer >= m.backfillDBInsertBatchSize {
				if err := m.ingestProcessedData(ctx, dbTx, batchBuffer); err != nil {
					return fmt.Errorf("ingesting data for ledgers ending at %d: %w", ledgerSeq, err)
				}
				batchBuffer.Clear()
				ledgersInBuffer = 0
			}
		}

		// Flush remaining data in buffer
		if ledgersInBuffer > 0 {
			if err := m.ingestProcessedData(ctx, dbTx, batchBuffer); err != nil {
				return fmt.Errorf("ingesting final data for batch [%d - %d]: %w", batch.StartLedger, batch.EndLedger, err)
			}
		}

		// We only update the cursor for historical backfill
		if mode == BackfillModeHistorical {
			return m.updateOldestLedgerCursor(ctx, dbTx, batch.StartLedger)
		}
		return nil
	})
	if err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result
	}

	result.Duration = time.Since(start)
	log.Ctx(ctx).Infof("Batch [%d - %d] completed: %d ledgers in %v",
		batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

	return result
}

// processBackfillLedger processes a ledger and populates the provided buffer.
func (m *ingestService) processBackfillLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Get transactions from ledger
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	// Process transactions and populate buffer (combined collection + processing)
	_, err = m.ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	return nil
}

// updateOldestLedgerCursor updates the oldest ledger cursor during backfill with metrics tracking.
func (m *ingestService) updateOldestLedgerCursor(ctx context.Context, dbTx pgx.Tx, currentLedger uint32) error {
	cursorStart := time.Now()
	err := m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, currentLedger)
	if err != nil {
		return fmt.Errorf("updating oldest ledger cursor: %w", err)
	}
	m.metricsService.SetOldestLedgerIngested(float64(currentLedger))
	m.metricsService.ObserveIngestionPhaseDuration("oldest_cursor_update", time.Since(cursorStart).Seconds())
	return nil
}

// updateLatestLedgerCursorAfterCatchup updates the latest ledger cursor after catchup completes.
func (m *ingestService) updateLatestLedgerCursorAfterCatchup(ctx context.Context, ledger uint32) error {
	cursorStart := time.Now()
	pgxTx, err := m.models.DB.PgxPool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning pgx transaction: %w", err)
	}
	defer func() {
		if err := pgxTx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Ctx(ctx).Errorf("error rolling back pgx transaction: %v", err)
		}
	}()
	if updateErr := m.models.IngestStore.Update(ctx, pgxTx, m.latestLedgerCursorName, ledger); updateErr != nil {
		return fmt.Errorf("updating latest synced ledger: %w", updateErr)
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("committing pgx transaction: %w", err)
	}
	m.metricsService.SetLatestLedgerIngested(float64(ledger))
	m.metricsService.ObserveIngestionPhaseDuration("catchup_cursor_update", time.Since(cursorStart).Seconds())
	return nil
}
