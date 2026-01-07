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

// BackfillMode indicates the purpose of backfilling.
type BackfillMode int

func (m BackfillMode) isHistorical() bool {
	return m == BackfillModeHistorical
}

func (m BackfillMode) isCatchup() bool {
	return m == BackfillModeCatchup
}

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
	if mode.isCatchup() {
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
	if mode.isCatchup() {
		if numFailedBatches > 0 {
			return fmt.Errorf("optimized catchup failed: %d/%d batches failed", numFailedBatches, len(backfillBatches))
		}
		err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			innerErr := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, endLedger)
			if innerErr != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", endLedger, innerErr)
			}
			return nil
		})
		if err != nil {
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

	// Process all ledgers in batch (cursor is updated atomically with final flush for historical mode)
	ledgersCount, err := m.processLedgersInBatch(ctx, backend, batch, mode)
	result.LedgersCount = ledgersCount
	if err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result
	}

	// Record metrics for historical backfill cursor updates
	if mode.isHistorical() {
		m.metricsService.SetOldestLedgerIngested(float64(batch.StartLedger))
	}

	result.Duration = time.Since(start)
	log.Ctx(ctx).Infof("Batch [%d - %d] completed: %d ledgers in %v",
		batch.StartLedger, batch.EndLedger, result.LedgersCount, result.Duration)

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

// flushBatchBuffer persists buffered data to the database within a transaction.
// If updateCursorTo is non-nil, it also updates the oldest cursor atomically.
func (m *ingestService) flushBatchBuffer(ctx context.Context, buffer *indexer.IndexerBuffer, updateCursorTo *uint32) error {
	err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		filteredData, err := m.filterParticipantData(ctx, dbTx, buffer)
		if err != nil {
			return fmt.Errorf("filtering participant data: %w", err)
		}
		if err := m.ingestProcessedData(ctx, dbTx, filteredData, false); err != nil {
			return fmt.Errorf("ingesting processed data: %w", err)
		}
		// Update cursor atomically with data insertion if requested
		if updateCursorTo != nil {
			if err := m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, *updateCursorTo); err != nil {
				return fmt.Errorf("updating oldest cursor: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("flushing batch buffer: %w", err)
	}
	return nil
}

// processLedgersInBatch processes all ledgers in a batch, flushing to DB periodically.
// For historical backfill mode, the cursor is updated atomically with the final data flush.
// Returns the count of ledgers processed.
func (m *ingestService) processLedgersInBatch(
	ctx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
	mode BackfillMode,
) (int, error) {
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	ledgersProcessed := 0

	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			return ledgersProcessed, fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
		}

		if err := m.processLedger(ctx, ledgerMeta, batchBuffer); err != nil {
			return ledgersProcessed, fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
		}
		ledgersProcessed++
		ledgersInBuffer++

		// Flush buffer periodically to control memory usage (intermediate flushes, no cursor update)
		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			if err := m.flushBatchBuffer(ctx, batchBuffer, nil); err != nil {
				return ledgersProcessed, err
			}
			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Final flush with cursor update for historical backfill
	if ledgersInBuffer > 0 {
		var cursorUpdate *uint32
		if mode.isHistorical() {
			cursorUpdate = &batch.StartLedger
		}
		if err := m.flushBatchBuffer(ctx, batchBuffer, cursorUpdate); err != nil {
			return ledgersProcessed, err
		}
	} else if mode.isHistorical() {
		// All data was flushed in intermediate batches, but we still need to update the cursor
		// This happens when ledgersInBuffer == 0 (exact multiple of batch size)
		if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
			return ledgersProcessed, err
		}
	}

	return ledgersProcessed, nil
}

// updateOldestCursor updates the oldest ledger cursor to the given ledger.
func (m *ingestService) updateOldestCursor(ctx context.Context, ledgerSeq uint32) error {
	err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		return m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, ledgerSeq)
	})
	if err != nil {
		return fmt.Errorf("updating oldest ledger cursor: %w", err)
	}
	return nil
}
