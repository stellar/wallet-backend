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

	"github.com/stellar/wallet-backend/internal/data"
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
		err = m.tokenIngestionService.PopulateAccountTokens(ctx, startLedger, func(dbTx pgx.Tx) error {
			return m.initializeCursors(ctx, dbTx, startLedger)
		})
		if err != nil {
			return fmt.Errorf("populating account tokens and initializing cursors: %w", err)
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

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, buffer)
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

// ingestProcessedDataWithRetry ingests all ledger data into the database in a single atomic transaction.
// This includes trustline assets, contract tokens, transactions, operations, token changes, and cursor update.
// All operations succeed or fail together, ensuring data consistency.
func (m *ingestService) ingestProcessedDataWithRetry(ctx context.Context, currentLedger uint32, buffer *indexer.IndexerBuffer) (int, int, error) {
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
			// 1. Insert unique trustline assets (pre-tracked in buffer)
			if txErr := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, buffer.GetUniqueTrustlineAssets()); txErr != nil {
				return fmt.Errorf("inserting trustline assets for ledger %d: %w", currentLedger, txErr)
			}

			// 2. Insert new contract tokens (filter existing, fetch metadata, insert)
			contracts, txErr := m.prepareNewContractTokens(ctx, dbTx, buffer.GetUniqueContractsByID())
			if txErr != nil {
				return fmt.Errorf("preparing contract tokens for ledger %d: %w", currentLedger, txErr)
			}
			if len(contracts) > 0 {
				if txErr = m.models.Contract.BatchInsert(ctx, dbTx, contracts); txErr != nil {
					return fmt.Errorf("inserting contracts for ledger %d: %w", currentLedger, txErr)
				}
			}

			// 3. Filter participant data
			filteredData, txErr := m.filterParticipantData(ctx, dbTx, buffer)
			if txErr != nil {
				return fmt.Errorf("filtering participant data for ledger %d: %w", currentLedger, txErr)
			}

			// 4. Insert transactions and operations
			if txErr = m.insertIntoDB(ctx, dbTx, filteredData); txErr != nil {
				return fmt.Errorf("inserting processed data into db for ledger %d: %w", currentLedger, txErr)
			}

			// 5. Unlock channel accounts
			if txErr = m.unlockChannelAccounts(ctx, dbTx, buffer.GetTransactions()); txErr != nil {
				return fmt.Errorf("unlocking channel accounts for ledger %d: %w", currentLedger, txErr)
			}

			// 6. Process token changes (trustline add/remove/update with full XDR fields, contract token add, native balance, SAC balance)
			trustlineChanges := buffer.GetTrustlineChanges()
			contractChanges := buffer.GetContractChanges()
			accountChanges := buffer.GetAccountChanges()
			sacBalanceChanges := buffer.GetSACBalanceChanges()
			if txErr = m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges, accountChanges, sacBalanceChanges); txErr != nil {
				return fmt.Errorf("processing token changes for ledger %d: %w", currentLedger, txErr)
			}

			// 7. Update cursor (all operations atomic with this)
			if txErr = m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, currentLedger); txErr != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, txErr)
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
	if len(txs) == 0 || m.chAccStore == nil {
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

// prepareNewContractTokens filters out existing contracts and prepares metadata for new contracts.
// SAC contracts get their metadata from ledger data (sacContracts parameter).
// SEP-41 contracts need RPC metadata fetch.
func (m *ingestService) prepareNewContractTokens(ctx context.Context, dbTx pgx.Tx, contractsByID map[string]types.ContractType, sacContracts map[string]*data.Contract) ([]*data.Contract, error) {
	if len(contractsByID) == 0 {
		return nil, nil
	}

	// Build list of contract IDs to check
	contractIDsList := make([]string, 0, len(contractsByID))
	for id := range contractsByID {
		contractIDsList = append(contractIDsList, id)
	}

	// Get existing contract IDs from DB (only checking the ones we need)
	existingIDs, err := m.models.Contract.GetExisting(ctx, dbTx, contractIDsList)
	if err != nil {
		return nil, fmt.Errorf("getting existing contract IDs: %w", err)
	}
	existingSet := set.NewSet(existingIDs...)

	// Collect new contracts
	var contracts []*data.Contract
	var newSep41ContractIDs []string

	for id, ctype := range contractsByID {
		if existingSet.Contains(id) {
			continue
		}
		switch ctype {
		case types.ContractTypeSAC:
			// SAC metadata already extracted from ledger
			if c, ok := sacContracts[id]; ok {
				contracts = append(contracts, c)
			}
		case types.ContractTypeSEP41:
			newSep41ContractIDs = append(newSep41ContractIDs, id)
		}
	}

	// Fetch metadata for new SEP-41 contracts via RPC
	if len(newSep41ContractIDs) > 0 {
		sep41Contracts, fetchErr := m.contractMetadataService.FetchSep41Metadata(ctx, newSep41ContractIDs)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetching metadata for new SEP-41 contracts: %w", fetchErr)
		}
		contracts = append(contracts, sep41Contracts...)
	}

	return contracts, nil
}
