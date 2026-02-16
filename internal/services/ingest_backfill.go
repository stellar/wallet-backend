package services

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
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
	BatchChanges *BatchChanges // Only populated for catchup mode
	StartTime    time.Time     // First ledger close time in batch (for compression)
	EndTime      time.Time     // Last ledger close time in batch (for compression)
}

// BatchChanges holds data collected from a backfill batch for catchup mode.
// This data is processed after all parallel batches complete to ensure proper ordering.
type BatchChanges struct {
	TrustlineChangesByKey     map[indexer.TrustlineChangeKey]types.TrustlineChange
	ContractChanges           []types.ContractChange
	AccountChangesByAccountID map[string]types.AccountChange
	SACBalanceChangesByKey    map[indexer.SACBalanceChangeKey]types.SACBalanceChange
	UniqueTrustlineAssets     map[uuid.UUID]data.TrustlineAsset
	UniqueContractTokensByID  map[string]types.ContractType
	SACContractsByID          map[string]*data.Contract // SAC contract metadata extracted from instance entries
}

// mergeTrustlineChanges merges source trustline changes into dest, keeping highest OperationID per key.
// Handles ADD→REMOVE no-op case where a trustline is created and removed in the same batch.
func mergeTrustlineChanges(dest, source map[indexer.TrustlineChangeKey]types.TrustlineChange) {
	for key, change := range source {
		existing, exists := dest[key]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.TrustlineOpRemove && existing.Operation == types.TrustlineOpAdd {
			delete(dest, key)
			continue
		}
		dest[key] = change
	}
}

// mergeAccountChanges merges source account changes into dest, keeping highest OperationID per account.
// Handles CREATE→REMOVE no-op case where an account is created and removed in the same batch.
func mergeAccountChanges(dest, source map[string]types.AccountChange) {
	for accountID, change := range source {
		existing, exists := dest[accountID]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle CREATE→REMOVE no-op case
		if exists && change.Operation == types.AccountOpRemove && existing.Operation == types.AccountOpCreate {
			delete(dest, accountID)
			continue
		}
		dest[accountID] = change
	}
}

// mergeSACBalanceChanges merges source SAC balance changes into dest, keeping highest OperationID per key.
// Handles ADD→REMOVE no-op case where a SAC balance is created and removed in the same batch.
func mergeSACBalanceChanges(dest, source map[indexer.SACBalanceChangeKey]types.SACBalanceChange) {
	for key, change := range source {
		existing, exists := dest[key]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
			delete(dest, key)
			continue
		}
		dest[key] = change
	}
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

	// Compress backfilled chunks for historical mode (no batches failed)
	if mode.isHistorical() && numFailedBatches == 0 {
		var minTime, maxTime time.Time
		for _, result := range results {
			if result.Error == nil {
				if minTime.IsZero() || result.StartTime.Before(minTime) {
					minTime = result.StartTime
				}
				if result.EndTime.After(maxTime) {
					maxTime = result.EndTime
				}
			}
		}
		if !minTime.IsZero() {
			m.recompressBackfilledChunks(ctx, minTime, maxTime)
		}
	}

	// Update latest ledger cursor and process catchup data for catchup mode
	if mode.isCatchup() {
		if numFailedBatches > 0 {
			return fmt.Errorf("optimized catchup failed: %d/%d batches failed", numFailedBatches, len(backfillBatches))
		}

		// Merge all batch changes into single maps
		mergedTrustlineChanges := make(map[indexer.TrustlineChangeKey]types.TrustlineChange)
		mergedUniqueTrustlineAssets := make(map[uuid.UUID]data.TrustlineAsset)
		mergedUniqueContractTokens := make(map[string]types.ContractType)
		mergedSACContracts := make(map[string]*data.Contract)
		mergedAccountChanges := make(map[string]types.AccountChange)
		mergedSACBalanceChanges := make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange)
		var allContractChanges []types.ContractChange
		for _, result := range results {
			if result.BatchChanges != nil {
				mergeTrustlineChanges(mergedTrustlineChanges, result.BatchChanges.TrustlineChangesByKey)
				allContractChanges = append(allContractChanges, result.BatchChanges.ContractChanges...)
				mergeAccountChanges(mergedAccountChanges, result.BatchChanges.AccountChangesByAccountID)
				mergeSACBalanceChanges(mergedSACBalanceChanges, result.BatchChanges.SACBalanceChangesByKey)
				maps.Copy(mergedUniqueTrustlineAssets, result.BatchChanges.UniqueTrustlineAssets)
				maps.Copy(mergedUniqueContractTokens, result.BatchChanges.UniqueContractTokensByID)
				// Merge SAC contracts (first-write wins)
				for id, contract := range result.BatchChanges.SACContractsByID {
					if _, exists := mergedSACContracts[id]; !exists {
						mergedSACContracts[id] = contract
					}
				}
			}
		}

		// Update latest ledger cursor after all catchup processing succeeds
		err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			// Process aggregated changes (token cache updates)
			innerErr := m.processBatchChanges(ctx, dbTx, mergedTrustlineChanges, allContractChanges, mergedAccountChanges, mergedSACBalanceChanges, mergedUniqueTrustlineAssets, mergedUniqueContractTokens, mergedSACContracts)
			if innerErr != nil {
				return fmt.Errorf("processing batch changes: %w", innerErr)
			}
			innerErr = m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, endLedger)
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
// For historical mode, direct compress handles compression during COPY; a single recompression
// pass runs after all batches complete (in startBackfilling).
func (m *ingestService) processBackfillBatchesParallel(ctx context.Context, mode BackfillMode, batches []BackfillBatch) []BackfillResult {
	results := make([]BackfillResult, len(batches))
	group := m.backfillPool.NewGroupContext(ctx)

	for i, batch := range batches {
		group.Submit(func() {
			results[i] = m.processSingleBatch(ctx, mode, batch, i, len(batches))
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Warnf("Backfill batch group wait returned error: %v", err)
	}

	return results
}

// processSingleBatch processes a single backfill batch with its own ledger backend.
func (m *ingestService) processSingleBatch(ctx context.Context, mode BackfillMode, batch BackfillBatch, batchIndex, totalBatches int) BackfillResult {
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
	ledgersCount, batchChanges, batchStartTime, batchEndTime, err := m.processLedgersInBatch(ctx, backend, batch, mode)
	result.LedgersCount = ledgersCount
	result.BatchChanges = batchChanges
	result.StartTime = batchStartTime
	result.EndTime = batchEndTime
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
// If directCompress is true, enables TimescaleDB direct compress for COPY operations.
func (m *ingestService) flushBatchBufferWithRetry(ctx context.Context, buffer *indexer.IndexerBuffer, updateCursorTo *uint32, batchChanges *BatchChanges, directCompress bool) error {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			if directCompress {
				if _, err := dbTx.Exec(ctx, "SET LOCAL timescaledb.enable_direct_compress_copy = on"); err != nil {
					return fmt.Errorf("enabling direct compress: %w", err)
				}
				if _, err := dbTx.Exec(ctx, "SET LOCAL timescaledb.enable_direct_compress_copy_client_sorted = on"); err != nil {
					return fmt.Errorf("enabling direct compress client sorted: %w", err)
				}
			}
			filteredData, err := m.filterParticipantData(ctx, dbTx, buffer)
			if err != nil {
				return fmt.Errorf("filtering participant data: %w", err)
			}
			// Collect changes for post-catchup processing if requested
			if batchChanges != nil {
				mergeTrustlineChanges(batchChanges.TrustlineChangesByKey, buffer.GetTrustlineChanges())
				batchChanges.ContractChanges = append(batchChanges.ContractChanges, buffer.GetContractChanges()...)
				mergeAccountChanges(batchChanges.AccountChangesByAccountID, buffer.GetAccountChanges())
				mergeSACBalanceChanges(batchChanges.SACBalanceChangesByKey, buffer.GetSACBalanceChanges())
				// Collect unique assets (iterate slice into map)
				for _, asset := range buffer.GetUniqueTrustlineAssets() {
					batchChanges.UniqueTrustlineAssets[asset.ID] = asset
				}
				// Collect unique contract tokens
				maps.Copy(batchChanges.UniqueContractTokensByID, buffer.GetUniqueSEP41ContractTokensByID())
				// Collect SAC contract metadata (first-write wins for deduplication)
				for id, contract := range buffer.GetSACContracts() {
					if _, exists := batchChanges.SACContractsByID[id]; !exists {
						batchChanges.SACContractsByID[id] = contract
					}
				}
			}
			if err := m.insertIntoDB(ctx, dbTx, filteredData); err != nil {
				return fmt.Errorf("inserting processed data into db: %w", err)
			}
			// Unlock channel accounts using all transactions (not filtered)
			if err := m.unlockChannelAccounts(ctx, dbTx, buffer.GetTransactions()); err != nil {
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
	return lastErr
}

// processLedgersInBatch processes all ledgers in a batch, flushing to DB periodically.
// For historical backfill mode, the cursor is updated atomically with the final data flush.
// For catchup mode, returns collected batch changes for post-catchup processing.
// Returns the count of ledgers processed, batch changes (nil for historical mode), and time range.
func (m *ingestService) processLedgersInBatch(
	ctx context.Context,
	backend ledgerbackend.LedgerBackend,
	batch BackfillBatch,
	mode BackfillMode,
) (int, *BatchChanges, time.Time, time.Time, error) {
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	ledgersProcessed := 0
	var startTime, endTime time.Time

	// Initialize batch changes collector for catchup mode
	var batchChanges *BatchChanges
	if mode.isCatchup() {
		batchChanges = &BatchChanges{
			TrustlineChangesByKey:     make(map[indexer.TrustlineChangeKey]types.TrustlineChange),
			AccountChangesByAccountID: make(map[string]types.AccountChange),
			SACBalanceChangesByKey:    make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange),
			UniqueTrustlineAssets:     make(map[uuid.UUID]data.TrustlineAsset),
			UniqueContractTokensByID:  make(map[string]types.ContractType),
			SACContractsByID:          make(map[string]*data.Contract),
		}
	}

	for ledgerSeq := batch.StartLedger; ledgerSeq <= batch.EndLedger; ledgerSeq++ {
		ledgerMeta, err := m.getLedgerWithRetry(ctx, backend, ledgerSeq)
		if err != nil {
			return ledgersProcessed, nil, startTime, endTime, fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
		}

		// Track time range for compression
		ledgerTime := ledgerMeta.ClosedAt()
		if startTime.IsZero() {
			startTime = ledgerTime
		}
		endTime = ledgerTime

		if err := m.processLedger(ctx, ledgerMeta, batchBuffer); err != nil {
			return ledgersProcessed, nil, startTime, endTime, fmt.Errorf("processing ledger %d: %w", ledgerSeq, err)
		}
		ledgersProcessed++
		ledgersInBuffer++

		// Flush buffer periodically to control memory usage (intermediate flushes, no cursor update)
		if ledgersInBuffer >= m.backfillDBInsertBatchSize {
			if err := m.flushBatchBufferWithRetry(ctx, batchBuffer, nil, batchChanges, mode.isHistorical()); err != nil {
				return ledgersProcessed, batchChanges, startTime, endTime, err
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
		if err := m.flushBatchBufferWithRetry(ctx, batchBuffer, cursorUpdate, batchChanges, mode.isHistorical()); err != nil {
			return ledgersProcessed, batchChanges, startTime, endTime, err
		}
	} else if mode.isHistorical() {
		// All data was flushed in intermediate batches, but we still need to update the cursor
		// This happens when ledgersInBuffer == 0 (exact multiple of batch size)
		if err := m.updateOldestCursor(ctx, batch.StartLedger); err != nil {
			return ledgersProcessed, nil, startTime, endTime, err
		}
	}

	return ledgersProcessed, batchChanges, startTime, endTime, nil
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

// processBatchChanges processes aggregated batch changes after all parallel batches complete.
// Unique assets and contracts are pre-collected during batch processing.
func (m *ingestService) processBatchChanges(
	ctx context.Context,
	dbTx pgx.Tx,
	trustlineChangesByKey map[indexer.TrustlineChangeKey]types.TrustlineChange,
	contractChanges []types.ContractChange,
	accountChangesByAccountID map[string]types.AccountChange,
	sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange,
	uniqueAssets map[uuid.UUID]data.TrustlineAsset,
	uniqueContractTokens map[string]types.ContractType,
	sacContracts map[string]*data.Contract,
) error {
	// 1. Convert unique assets map to slice for BatchInsert
	assetSlice := make([]data.TrustlineAsset, 0, len(uniqueAssets))
	for _, asset := range uniqueAssets {
		assetSlice = append(assetSlice, asset)
	}

	// 2. Insert unique trustline assets
	if len(assetSlice) > 0 {
		if txErr := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, assetSlice); txErr != nil {
			return fmt.Errorf("inserting trustline assets: %w", txErr)
		}
	}

	// 3. Insert new contract tokens (filter existing, fetch metadata, insert)
	if len(uniqueContractTokens) > 0 {
		contracts, txErr := m.prepareNewContractTokens(ctx, dbTx, uniqueContractTokens, sacContracts)
		if txErr != nil {
			return fmt.Errorf("preparing contracts: %w", txErr)
		}
		if len(contracts) > 0 {
			if txErr := m.models.Contract.BatchInsert(ctx, dbTx, contracts); txErr != nil {
				return fmt.Errorf("inserting contracts: %w", txErr)
			}
		}
	}

	// 4. Apply token changes to PostgreSQL
	if txErr := m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChangesByKey, contractChanges, accountChangesByAccountID, sacBalanceChangesByKey); txErr != nil {
		return fmt.Errorf("processing token changes: %w", txErr)
	}

	log.Ctx(ctx).Infof("Processed batch changes: %d trustline, %d contract, %d account, %d SAC balance changes",
		len(trustlineChangesByKey), len(contractChanges), len(accountChangesByAccountID), len(sacBalanceChangesByKey))

	return nil
}

// recompressBackfilledChunks recompresses already-compressed chunks overlapping the backfill range.
// Direct compress produces compressed chunks during COPY; recompression optimizes compression ratios.
// Tables are compressed sequentially to avoid CPU spikes; chunks within each table also stay sequential to avoid OOM.
// Skips chunks where range_end >= NOW() to avoid compressing active live ingestion chunks.
func (m *ingestService) recompressBackfilledChunks(ctx context.Context, startTime, endTime time.Time) {
	tables := []string{"transactions", "transactions_accounts", "operations", "operations_accounts", "state_changes"}

	tableCounts := make([]int, len(tables))

	for i, table := range tables {
		tableCounts[i] = m.compressTableChunks(ctx, table, startTime, endTime)
	}

	totalCompressed := 0
	for _, count := range tableCounts {
		totalCompressed += count
	}
	log.Ctx(ctx).Infof("Recompressed %d total chunks for time range [%s - %s]",
		totalCompressed, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
}

// compressTableChunks recompresses already-compressed chunks for a single hypertable.
// Direct compress produces compressed chunks during COPY; this pass optimizes compression ratios.
// Chunks are recompressed sequentially within the table to avoid OOM errors.
func (m *ingestService) compressTableChunks(ctx context.Context, table string, startTime, endTime time.Time) int {
	rows, err := m.models.DB.PgxPool().Query(ctx,
		`SELECT chunk_schema || '.' || chunk_name FROM timescaledb_information.chunks
		 WHERE hypertable_name = $1 AND is_compressed
		   AND range_start <= $2::timestamptz AND range_end >= $3::timestamptz
		   AND range_end < NOW()`,
		table, endTime, startTime)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to get chunks for %s: %v", table, err)
		return 0
	}

	var chunks []string
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			continue
		}
		chunks = append(chunks, chunk)
	}
	rows.Close()

	compressed := 0
	for i, chunk := range chunks {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Warnf("Recompression cancelled for %s after %d chunks", table, compressed)
			return compressed
		default:
		}

		_, err := m.models.DB.PgxPool().Exec(ctx,
			`CALL _timescaledb_functions.rebuild_columnstore($1::regclass)`, chunk)
		if err != nil {
			log.Ctx(ctx).Warnf("Failed to recompress chunk %s: %v", chunk, err)
			continue
		}
		compressed++
		log.Ctx(ctx).Debugf("Recompressed chunk %d/%d for %s: %s", i+1, len(chunks), table, chunk)
	}
	log.Ctx(ctx).Infof("Recompressed %d chunks for table %s", len(chunks), table)
	return compressed
}
