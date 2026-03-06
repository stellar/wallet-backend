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
		if exists && change.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
			delete(dest, key)
			continue
		}
		dest[key] = change
	}
}

// mergeAllBatchChanges merges all batch results into single aggregated maps.
func mergeAllBatchChanges(results []BackfillResult) (
	map[indexer.TrustlineChangeKey]types.TrustlineChange,
	[]types.ContractChange,
	map[string]types.AccountChange,
	map[indexer.SACBalanceChangeKey]types.SACBalanceChange,
	map[uuid.UUID]data.TrustlineAsset,
	map[string]types.ContractType,
	map[string]*data.Contract,
) {
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
			for id, contract := range result.BatchChanges.SACContractsByID {
				if _, exists := mergedSACContracts[id]; !exists {
					mergedSACContracts[id] = contract
				}
			}
		}
	}

	return mergedTrustlineChanges, allContractChanges, mergedAccountChanges, mergedSACBalanceChanges,
		mergedUniqueTrustlineAssets, mergedUniqueContractTokens, mergedSACContracts
}

// startCatchup catches up to the network tip from the latest ingested ledger.
// Processes ledgers in parallel batches, collects state changes, then merges and applies them.
func (m *ingestService) startCatchup(ctx context.Context, startLedger, endLedger uint32) error {
	if startLedger > endLedger {
		return fmt.Errorf("start ledger cannot be greater than end ledger")
	}

	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	if startLedger != latestIngestedLedger+1 {
		return fmt.Errorf("catchup must start from ledger %d (latestIngestedLedger + 1), got %d", latestIngestedLedger+1, startLedger)
	}

	gaps := []data.LedgerRange{{GapStart: startLedger, GapEnd: endLedger}}
	backfillBatches := m.splitGapsIntoBatches(gaps)

	startTime := time.Now()
	results := m.processBackfillBatchesParallel(ctx, backfillBatches, m.processLedgersInBatchCatchup, nil)
	duration := time.Since(startTime)

	numFailedBatches := analyzeBatchResults(ctx, results)
	if numFailedBatches > 0 {
		return fmt.Errorf("optimized catchup failed: %d/%d batches failed", numFailedBatches, len(backfillBatches))
	}

	// Merge all batch changes and apply in a single transaction with cursor update
	mergedTrustline, allContract, mergedAccount, mergedSAC, mergedAssets, mergedTokens, mergedSACContracts := mergeAllBatchChanges(results)

	err = db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		innerErr := m.processBatchChanges(ctx, dbTx, mergedTrustline, allContract, mergedAccount, mergedSAC, mergedAssets, mergedTokens, mergedSACContracts)
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

	log.Ctx(ctx).Infof("Catchup completed in %v: %d batches", duration, len(backfillBatches))
	return nil
}

// processLedgersInBatchCatchup processes all ledgers in a batch for catchup mode.
// Collects BatchChanges for post-catchup processing instead of updating cursors.
func (m *ingestService) processLedgersInBatchCatchup(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
	result := BackfillResult{Batch: batch}
	batchBuffer := indexer.NewIndexerBuffer()
	ledgersInBuffer := uint32(0)
	var startTime, endTime time.Time

	batchChanges := &BatchChanges{
		TrustlineChangesByKey:     make(map[indexer.TrustlineChangeKey]types.TrustlineChange),
		AccountChangesByAccountID: make(map[string]types.AccountChange),
		SACBalanceChangesByKey:    make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange),
		UniqueTrustlineAssets:     make(map[uuid.UUID]data.TrustlineAsset),
		UniqueContractTokensByID:  make(map[string]types.ContractType),
		SACContractsByID:          make(map[string]*data.Contract),
	}

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
			if err := m.flushCatchupBatch(ctx, batchBuffer, batchChanges); err != nil {
				result.Error = err
				return result
			}
			batchBuffer.Clear()
			ledgersInBuffer = 0
		}
	}

	// Final flush
	if ledgersInBuffer > 0 {
		if err := m.flushCatchupBatch(ctx, batchBuffer, batchChanges); err != nil {
			result.Error = err
			return result
		}
	}

	result.BatchChanges = batchChanges
	result.StartTime = startTime
	result.EndTime = endTime
	return result
}

// flushCatchupBatch persists buffered data to the database and collects changes
// for post-catchup processing.
func (m *ingestService) flushCatchupBatch(ctx context.Context, buffer *indexer.IndexerBuffer, batchChanges *BatchChanges) error {
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
			// Collect changes for post-catchup processing
			mergeTrustlineChanges(batchChanges.TrustlineChangesByKey, buffer.GetTrustlineChanges())
			batchChanges.ContractChanges = append(batchChanges.ContractChanges, buffer.GetContractChanges()...)
			mergeAccountChanges(batchChanges.AccountChangesByAccountID, buffer.GetAccountChanges())
			mergeSACBalanceChanges(batchChanges.SACBalanceChangesByKey, buffer.GetSACBalanceChanges())
			for _, asset := range buffer.GetUniqueTrustlineAssets() {
				batchChanges.UniqueTrustlineAssets[asset.ID] = asset
			}
			maps.Copy(batchChanges.UniqueContractTokensByID, buffer.GetUniqueSEP41ContractTokensByID())
			for id, contract := range buffer.GetSACContracts() {
				if _, exists := batchChanges.SACContractsByID[id]; !exists {
					batchChanges.SACContractsByID[id] = contract
				}
			}
			if _, _, err := m.insertIntoDB(ctx, dbTx, buffer); err != nil {
				return fmt.Errorf("inserting processed data into db: %w", err)
			}
			if err := m.unlockChannelAccounts(ctx, dbTx, buffer.GetTransactions()); err != nil {
				return fmt.Errorf("unlocking channel accounts: %w", err)
			}
			return nil
		})
		if err == nil {
			return nil
		}
		lastErr = err

		backoff := min(time.Duration(1<<attempt)*time.Second, maxIngestProcessedDataRetryBackoff)
		log.Ctx(ctx).Warnf("Error flushing catchup batch (attempt %d/%d): %v, retrying in %v...",
			attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return lastErr
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
	assetSlice := make([]data.TrustlineAsset, 0, len(uniqueAssets))
	for _, asset := range uniqueAssets {
		assetSlice = append(assetSlice, asset)
	}

	if len(assetSlice) > 0 {
		if txErr := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, assetSlice); txErr != nil {
			return fmt.Errorf("inserting trustline assets: %w", txErr)
		}
	}

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

	if txErr := m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChangesByKey, contractChanges, accountChangesByAccountID, sacBalanceChangesByKey); txErr != nil {
		return fmt.Errorf("processing token changes: %w", txErr)
	}

	log.Ctx(ctx).Infof("Processed batch changes: %d trustline, %d contract, %d account, %d SAC balance changes",
		len(trustlineChangesByKey), len(contractChanges), len(accountChangesByAccountID), len(sacBalanceChangesByKey))

	return nil
}
