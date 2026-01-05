package services

import (
	"context"
	"fmt"
	"time"

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

	startTime := time.Now()
	done := make(chan struct{})

	// Start elapsed time updater goroutine (only for historical backfill)
	if mode == BackfillModeHistorical {
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
	}

	// Determine gaps to fill based on mode
	var gaps []data.LedgerRange
	if mode == BackfillModeCatchup {
		// For catchup, treat entire range as a single gap (no existing data in this range)
		gaps = []data.LedgerRange{{GapStart: startLedger, GapEnd: endLedger}}
	} else {
		m.backfillInstanceID = fmt.Sprintf("%d-%d", startLedger, endLedger)
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
	results := m.processBackfillBatchesParallel(ctx, backfillBatches, mode)
	close(done)
	duration := time.Since(startTime)
	if mode == BackfillModeHistorical {
		m.metricsService.SetBackfillElapsed(m.backfillInstanceID, duration.Seconds())
	}

	analysis := analyzeBatchResults(ctx, results)

	// Record failed batches metric (only for historical backfill)
	if mode == BackfillModeHistorical {
		for range analysis.failedBatches {
			m.metricsService.IncBackfillBatchesFailed(m.backfillInstanceID)
		}
	}
	if len(analysis.failedBatches) > 0 {
		return fmt.Errorf("backfilling failed: %d/%d batches failed", len(analysis.failedBatches), len(backfillBatches))
	}

	// Update cursors based on mode
	switch mode {
	case BackfillModeHistorical:
		if err := m.updateOldestLedgerCursor(ctx, startLedger); err != nil {
			return fmt.Errorf("updating oldest cursor: %w", err)
		}
	case BackfillModeCatchup:
		if err := m.updateLatestLedgerCursorAfterCatchup(ctx, endLedger); err != nil {
			return fmt.Errorf("updating latest cursor after catchup: %w", err)
		}
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
func (m *ingestService) processBackfillBatchesParallel(ctx context.Context, batches []BackfillBatch, mode BackfillMode) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		// Capture loop variables
		index := i
		batchCopy := batch

		group.Submit(func() {
			result := m.processSingleBatch(ctx, batchCopy, mode)
			results[index] = result
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Errorf("CRITICAL: Backfill batch group wait error: %v", err)
	}

	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, batch BackfillBatch, mode BackfillMode) BackfillResult {
	start := time.Now()
	result := BackfillResult{Batch: batch}
	recordMetrics := mode == BackfillModeHistorical

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

	// Process each ledger in the batch sequentially
	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		fetchStart := time.Now()
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			result.Error = fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}
		if recordMetrics {
			m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "get_ledger_from_backend", time.Since(fetchStart).Seconds())
		}

		err = m.processBackfillLedger(ctx, ledgerMeta, batchBuffer, recordMetrics)
		if err != nil {
			result.Error = fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
			result.Duration = time.Since(start)
			return result
		}
		result.LedgersCount++
		ledgersInBuffer++

		// Flush buffer periodically to control memory usage
		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			if recordMetrics {
				m.metricsService.IncBackfillTransactionsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfTransactions())
				m.metricsService.IncBackfillOperationsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfOperations())
			}

			insertStart := time.Now()
			if err := m.ingestProcessedData(ctx, batchBuffer); err != nil {
				log.Ctx(ctx).Errorf("FAILED database insert for batch [%d-%d] at ledger %d: %v",
					batch.StartLedger, batch.EndLedger, ledgerSeq, err)
				result.Error = fmt.Errorf("ingesting data for ledgers ending at %d: %w", ledgerSeq, err)
				result.Duration = time.Since(insertStart)
				return result
			}

			if recordMetrics {
				m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "insert_into_db", time.Since(insertStart).Seconds())
			}
			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Flush remaining data in buffer
	if ledgersInBuffer > 0 {
		if recordMetrics {
			m.metricsService.IncBackfillTransactionsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfTransactions())
			m.metricsService.IncBackfillOperationsProcessed(m.backfillInstanceID, batchBuffer.GetNumberOfOperations())
		}

		insertStart := time.Now()
		if err := m.ingestProcessedData(ctx, batchBuffer); err != nil {
			result.Error = fmt.Errorf("ingesting final data for batch [%d - %d]: %w", batch.StartLedger, batch.EndLedger, err)
			result.Duration = time.Since(insertStart)
			return result
		}

		if recordMetrics {
			m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "insert_into_db", time.Since(insertStart).Seconds())
		}
	}

	result.Duration = time.Since(start)
	if recordMetrics {
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "batch_processing", result.Duration.Seconds())
		m.metricsService.IncBackfillBatchesCompleted(m.backfillInstanceID)
		m.metricsService.IncBackfillLedgersProcessed(m.backfillInstanceID, result.LedgersCount)
	}
	log.Ctx(ctx).Infof("Batch [%d - %d] completed: %d ledgers in %v",
		batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

	return result
}

// processBackfillLedger processes a ledger and populates the provided buffer.
func (m *ingestService) processBackfillLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer, recordMetrics bool) error {
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
	if recordMetrics {
		m.metricsService.ObserveBackfillPhaseDuration(m.backfillInstanceID, "process_ledger_transactions", time.Since(start).Seconds())
	}
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

// updateLatestLedgerCursorAfterCatchup updates the latest ledger cursor after catchup completes.
func (m *ingestService) updateLatestLedgerCursorAfterCatchup(ctx context.Context, ledger uint32) error {
	cursorStart := time.Now()
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		if updateErr := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, ledger); updateErr != nil {
			return fmt.Errorf("updating latest synced ledger: %w", updateErr)
		}
		m.metricsService.SetLatestLedgerIngested(float64(ledger))
		return nil
	})
	if err != nil {
		return fmt.Errorf("updating cursors: %w", err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("catchup_cursor_update", time.Since(cursorStart).Seconds())
	return nil
}
