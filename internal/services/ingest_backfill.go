package services

import (
	"context"
	"fmt"
	"time"
	"sync"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
)

// startBackfilling processes historical ledgers in the specified range,
// identifying gaps and processing them in parallel batches.
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

	backfillBatches := m.splitGapsIntoBatches(gaps, BackfillBatchSize)
	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, backfillBatches)
	duration := time.Since(startTime)

	analysis := analyzeBatchResults(ctx, results)
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
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			result.Error = fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}

		err = m.processLedger(ctx, ledgerMeta)
		if err != nil {
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
