package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	maxIngestProcessedDataRetries      = 5
	maxIngestProcessedDataRetryBackoff = 10 * time.Second
)

// startLiveIngestion begins continuous ingestion from the last checkpoint ledger,
// acquiring an advisory lock to prevent concurrent ingestion instances.
func (m *ingestService) startLiveIngestion(ctx context.Context) error {
	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently
	if lockAcquired, err := db.AcquireAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	} else if !lockAcquired {
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
			err = fmt.Errorf("releasing advisory lock: %w", err)
			log.Ctx(ctx).Error(err)
		}
	}()

	// Get latest ingested ledger to determine DB state
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	startLedger := latestIngestedLedger + 1
	if latestIngestedLedger == 0 {
		startLedger, err = m.archive.GetLatestLedgerSequence()
		if err != nil {
			return fmt.Errorf("getting latest ledger sequence: %w", err)
		}
		err = m.tokenCacheWriter.PopulateAccountTokens(ctx, startLedger, func(dbTx pgx.Tx) error {
			return m.initializeCursors(ctx, dbTx, startLedger)
		})
		if err != nil {
			return fmt.Errorf("populating account tokens and initializing cursors: %w", err)
		}
		if err := m.initializeKnownContractIDs(ctx); err != nil {
			return fmt.Errorf("initializing known contract IDs: %w", err)
		}
	} else {
		// If we already have data in the DB, we will do an optimized catchup by parallely backfilling the ledgers.
		if err := m.initializeKnownContractIDs(ctx); err != nil {
			return fmt.Errorf("initializing known contract IDs: %w", err)
		}
		health, err := m.rpcService.GetHealth()
		if err != nil {
			return fmt.Errorf("getting health check result from RPC: %w", err)
		}
		networkLatestLedger := health.LatestLedger
		if networkLatestLedger > startLedger && (networkLatestLedger-startLedger) >= m.catchupThreshold {
			log.Ctx(ctx).Infof("Wallet backend has fallen behind network tip by %d ledgers. Doing optimized catchup to the tip: %d", networkLatestLedger-startLedger, networkLatestLedger)
			err := m.startBackfilling(ctx, startLedger, networkLatestLedger, BackfillModeCatchup)
			if err != nil {
				return fmt.Errorf("catching up to network tip: %w", err)
			}
			// Update startLedger to continue from where catchup ended
			startLedger = networkLatestLedger + 1
		}
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}
	return m.ingestLiveLedgers(ctx, startLedger)
}

// initializeCursors initializes both latest and oldest cursors to the same starting ledger.
func (m *ingestService) initializeCursors(ctx context.Context, dbTx pgx.Tx, ledger uint32) error {
	if err := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, ledger); err != nil {
		return fmt.Errorf("initializing latest cursor: %w", err)
	}
	if err := m.models.IngestStore.Update(ctx, dbTx, m.oldestLedgerCursorName, ledger); err != nil {
		return fmt.Errorf("initializing oldest cursor: %w", err)
	}
	return nil
}

// initializeKnownContractIDs populates the in-memory cache of known contract IDs.
func (m *ingestService) initializeKnownContractIDs(ctx context.Context) error {
	ids, err := m.models.Contract.GetAllContractIDs(ctx)
	if err != nil {
		return fmt.Errorf("loading contract IDs into cache: %w", err)
	}
	m.knownContractIDs.Append(ids...)
	log.Ctx(ctx).Infof("Loaded %d contract IDs into cache", len(ids))
	return nil
}

// ingestLiveLedgers continuously processes ledgers starting from startLedger,
// updating cursors and metrics after each successful ledger.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", currentLedger)
	for {
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}

		totalStart := time.Now()
		processStart := time.Now()
		buffer := indexer.NewIndexerBuffer()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.metricsService.ObserveIngestionPhaseDuration("process_ledger", time.Since(processStart).Seconds())

		// Pre-commit trustline assets and contract tokens to satisfy FK constraints
		dbStart := time.Now()
		contractIDMap, err := m.ensureTokensExistWithRetry(ctx, currentLedger, buffer.GetTrustlineChanges(), buffer.GetContractChanges())
		if err != nil {
			return fmt.Errorf("ensuring tokens exist for ledger %d: %w", currentLedger, err)
		}
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, buffer, contractIDMap)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.metricsService.ObserveIngestionPhaseDuration("insert_into_db", time.Since(dbStart).Seconds())
		totalIngestionDuration := time.Since(totalStart).Seconds()
		m.metricsService.ObserveIngestionDuration(totalIngestionDuration)
		m.metricsService.IncIngestionTransactionsProcessed(numTransactionProcessed)
		m.metricsService.IncIngestionOperationsProcessed(numOperationProcessed)
		m.metricsService.IncIngestionLedgersProcessed(1)
		m.metricsService.SetLatestLedgerIngested(float64(currentLedger))

		log.Ctx(ctx).Infof("Ingested ledger %d in %.4fs", currentLedger, totalIngestionDuration)
		currentLedger++
	}
}

// ensureTokensExistWithRetry ensures trustline assets and contract tokens exist in the database.
// Returns contractIDMap for use in processing token changes (trustline IDs are computed via DeterministicAssetID).
func (m *ingestService) ensureTokensExistWithRetry(ctx context.Context, currentLedger uint32, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) (map[string]int64, error) {
	var contractIDMap map[string]int64
	var innerErr error
	var lastErr error

	// Track which contracts are new (for cache update after transaction commits)
	newContractIDs := m.identifyNewContractIDs(contractChanges)

	for attempt := 0; attempt <= maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// Ensure trustline assets exist (no return value needed - IDs computed via hash)
		if innerErr = m.tokenCacheWriter.EnsureTrustlineAssetsExist(ctx, trustlineChanges); innerErr != nil {
			lastErr = fmt.Errorf("ensuring trustline assets exist: %w", innerErr)
			goto retry
		}

		// Get IDs for all SAC/SEP-41 contracts (new + existing)
		innerErr = db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			var txErr error
			contractIDMap, txErr = m.tokenCacheWriter.GetOrInsertContractTokens(ctx, dbTx, contractChanges, m.knownContractIDs)
			return txErr
		})
		if innerErr != nil {
			lastErr = fmt.Errorf("getting or inserting contract tokens: %w", innerErr)
			goto retry
		}

		// Update cache AFTER transaction commits to avoid stale cache on rollback
		for _, contractID := range newContractIDs {
			m.knownContractIDs.Add(contractID)
		}
		return contractIDMap, nil

	retry:
		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxIngestProcessedDataRetryBackoff {
			backoff = maxIngestProcessedDataRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error ensuring tokens exist for ledger %d (attempt %d/%d): %v, retrying in %v...",
			currentLedger, attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return nil, fmt.Errorf("ensuring tokens exist: %w", lastErr)
}

// ingestProcessedDataWithRetry ingests the processed data into the database with retry logic,
// unlocks channel accounts, and processes token changes.
// The contractIDMap must be pre-populated by calling GetOrInsertContractTokens before this function.
func (m *ingestService) ingestProcessedDataWithRetry(ctx context.Context, currentLedger uint32, buffer *indexer.IndexerBuffer, contractIDMap map[string]int64) (int, int, error) {
	numTransactionProcessed := 0
	numOperationProcessed := 0

	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			filteredData, innerErr := m.filterParticipantData(ctx, dbTx, buffer)
			if innerErr != nil {
				return fmt.Errorf("filtering participant data for ledger %d: %w", currentLedger, innerErr)
			}
			innerErr = m.insertIntoDB(ctx, dbTx, filteredData)
			if innerErr != nil {
				return fmt.Errorf("inserting processed data into db for ledger %d: %w", currentLedger, innerErr)
			}
			innerErr = m.unlockChannelAccounts(ctx, dbTx, buffer.GetTransactions())
			if innerErr != nil {
				return fmt.Errorf("unlocking channel accounts for ledger %d: %w", currentLedger, innerErr)
			}
			innerErr = m.tokenCacheWriter.ProcessTokenChanges(ctx, contractIDMap, filteredData.trustlineChanges, filteredData.contractTokenChanges)
			if innerErr != nil {
				return fmt.Errorf("processing token changes for ledger %d: %w", currentLedger, innerErr)
			}
			log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(filteredData.trustlineChanges), len(filteredData.contractTokenChanges))
			innerErr = m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, currentLedger)
			if innerErr != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, innerErr)
			}
			numTransactionProcessed = len(filteredData.txs)
			numOperationProcessed = len(filteredData.ops)
			return nil
		})

		if err == nil {
			return numTransactionProcessed, numOperationProcessed, nil
		}
		lastErr = err

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxIngestProcessedDataRetryBackoff {
			backoff = maxIngestProcessedDataRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error ingesting data for ledger %d (attempt %d/%d): %v, retrying in %v...",
			currentLedger, attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return 0, 0, fmt.Errorf("ingesting processed data failed after %d attempts: %w", maxIngestProcessedDataRetries, lastErr)
}

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, dbTx pgx.Tx, txs []*types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	innerTxHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		innerTxHashes = append(innerTxHashes, tx.InnerTransactionHash)
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, dbTx, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("🔓 unlocked %d channel accounts", affectedRows)
	}

	return nil
}

// identifyNewContractIDs returns unique SAC/SEP-41 contract IDs that are not in the cache.
// Used to update the cache after transaction commits.
func (m *ingestService) identifyNewContractIDs(contractChanges []types.ContractChange) []string {
	if len(contractChanges) == 0 {
		return nil
	}

	seen := set.NewSet[string]()
	var newIDs []string

	for _, change := range contractChanges {
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		if change.ContractID == "" || seen.Contains(change.ContractID) {
			continue
		}
		seen.Add(change.ContractID)

		// Check cache - if not known, it's a new contract
		if !m.knownContractIDs.Contains(change.ContractID) {
			newIDs = append(newIDs, change.ContractID)
		}
	}

	return newIDs
}
