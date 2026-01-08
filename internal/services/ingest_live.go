package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
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

		err = db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			if txErr := m.accountTokenService.PopulateAccountTokens(ctx, dbTx, startLedger); txErr != nil {
				return fmt.Errorf("populating account tokens cache: %w", txErr)
			}
			if txErr := m.initializeCursors(ctx, dbTx, startLedger); txErr != nil {
				return fmt.Errorf("initializing cursors: %w", txErr)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("initializing live ingestion: %w", err)
		}
	} else {
		// If we already have data in the DB, we will do an optimized catchup by parallely backfilling the ledgers.
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

// ingestLiveLedgers continuously processes ledgers starting from startLedger,
// updating cursors and metrics after each successful ledger.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", currentLedger)
	for {
		totalStart := time.Now()
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}
		m.metricsService.ObserveIngestionPhaseDuration("get_ledger", time.Since(totalStart).Seconds())

		start := time.Now()
		buffer := indexer.NewIndexerBuffer()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.metricsService.ObserveIngestionPhaseDuration("process_ledger", time.Since(start).Seconds())

		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedData(ctx, currentLedger, buffer)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}

		// Record processing metrics
		m.metricsService.ObserveIngestionPhaseDuration("db_insertion", time.Since(dbStart).Seconds())
		totalIngestionDuration := time.Since(totalStart).Seconds()
		m.metricsService.ObserveIngestionDuration(totalIngestionDuration)
		m.metricsService.IncIngestionTransactionsProcessed(numTransactionProcessed)
		m.metricsService.IncIngestionOperationsProcessed(numOperationProcessed)
		m.metricsService.IncIngestionLedgersProcessed(1)
		m.metricsService.SetLatestLedgerIngested(float64(currentLedger))

		log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, totalIngestionDuration)
		currentLedger++
	}
}

// ingestProcessedData ingests the processed data into the database, unlocks channel accounts, and processes token changes.
func (m *ingestService) ingestProcessedData(ctx context.Context, currentLedger uint32, buffer *indexer.IndexerBuffer) (int, int, error) {
	numTransactionProcessed := 0
	numOperationProcessed := 0
	err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		filteredData, innerErr := m.filterParticipantData(ctx, dbTx, buffer)
		if innerErr != nil {
			return fmt.Errorf("filtering participant data for ledger %d: %w", currentLedger, innerErr)
		}
		innerErr = m.insertIntoDB(ctx, dbTx, filteredData)
		if innerErr != nil {
			return fmt.Errorf("inserting processed data into db for ledger %d: %w", currentLedger, innerErr)
		}
		if innerErr := m.unlockChannelAccounts(ctx, dbTx, filteredData.txs); innerErr != nil {
			return fmt.Errorf("unlocking channel accounts for ledger %d: %w", currentLedger, innerErr)
		}
		innerErr = m.processLiveIngestionTokenChanges(ctx, dbTx, filteredData.trustlineChanges, filteredData.contractTokenChanges)
		if innerErr != nil {
			return fmt.Errorf("processing token changes for ledger %d: %w", currentLedger, innerErr)
		}
		innerErr = m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, currentLedger)
		if innerErr != nil {
			return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, innerErr)
		}
		numTransactionProcessed = len(filteredData.txs)
		numOperationProcessed = len(filteredData.ops)
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("ingesting processed data: %w", err)
	}
	return numTransactionProcessed, numOperationProcessed, nil
}

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	innerTxHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		innerTxHashes = append(innerTxHashes, tx.InnerTransactionHash)
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, pgxTx, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("🔓 unlocked %d channel accounts", affectedRows)
	}

	return nil
}

// processLiveIngestionTokenChanges processes trustline and contract changes for live ingestion.
// This updates the Redis cache and fetches metadata for new SAC/SEP-41 contracts.
func (m *ingestService) processLiveIngestionTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	// Sort trustline changes by operation ID in ascending order
	sort.Slice(trustlineChanges, func(i, j int) bool {
		return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
	})

	// Process all trustline and contract changes in a single batch using Redis pipelining
	if err := m.accountTokenService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges); err != nil {
		log.Ctx(ctx).Errorf("processing trustline changes batch: %v", err)
		return fmt.Errorf("processing trustline changes batch: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(trustlineChanges), len(contractChanges))

	// Fetch and store metadata for new SAC/SEP-41 contracts
	if m.contractMetadataService != nil {
		newContractTypesByID := m.filterNewContractTokens(ctx, contractChanges)
		if len(newContractTypesByID) > 0 {
			log.Ctx(ctx).Infof("Fetching metadata for %d new contract tokens", len(newContractTypesByID))
			if err := m.contractMetadataService.FetchAndStoreMetadata(ctx, dbTx, newContractTypesByID); err != nil {
				log.Ctx(ctx).Warnf("fetching new contract metadata: %v", err)
				// Don't return error - we don't want to block ingestion for metadata fetch failures
			}
		}
	}
	return nil
}

// filterNewContractTokens extracts unique SAC/SEP-41 contract IDs from contract changes,
// checks which contracts already exist in the database, and returns a map of only new contracts.
func (m *ingestService) filterNewContractTokens(ctx context.Context, contractChanges []types.ContractChange) map[string]types.ContractType {
	if len(contractChanges) == 0 {
		return nil
	}

	// Extract unique SAC and SEP-41 contract IDs and build type map
	seen := set.NewSet[string]()
	contractTypeMap := make(map[string]types.ContractType)
	var contractIDs []string

	for _, change := range contractChanges {
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		if change.ContractID == "" {
			continue
		}
		if seen.Contains(change.ContractID) {
			continue
		}
		seen.Add(change.ContractID)
		contractIDs = append(contractIDs, change.ContractID)
		contractTypeMap[change.ContractID] = change.ContractType
	}

	if len(contractIDs) == 0 {
		return nil
	}

	// Check which contracts already exist in the database
	existingContracts, err := m.models.Contract.BatchGetByIDs(ctx, contractIDs)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to check existing contracts: %v", err)
		return nil
	}

	// Remove existing contracts from the map
	for _, contract := range existingContracts {
		delete(contractTypeMap, contract.ID)
	}

	return contractTypeMap
}
