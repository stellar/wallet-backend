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
	oldestLedgerSyncInterval           = 100
)

// PersistLedgerData persists processed ledger data to the database in three phases:
//
// Phase 1: Insert FK prerequisites (trustline assets, contract tokens) and commit,
// making parent rows visible for phase 2's balance upserts.
//
// Phase 2: Parallel COPYs (5 tables) + parallel upserts (4 balance tables) via errgroup,
// each on its own pool connection. UniqueViolation errors are treated as success for
// idempotent crash recovery.
//
// Phase 3: Finalize — unlock channel accounts + advance cursor. The cursor update is the
// idempotency marker; if we crash before it, everything replays safely.
func (m *ingestService) PersistLedgerData(ctx context.Context, ledgerSeq uint32, buffer *indexer.IndexerBuffer, cursorName string) (int, int, error) {
	// Phase 1: FK prerequisites — commit so parent rows are visible to phase 2.
	if err := m.persistFKPrerequisites(ctx, ledgerSeq, buffer); err != nil {
		return 0, 0, fmt.Errorf("persisting FK prerequisites for ledger %d: %w", ledgerSeq, err)
	}

	// Phase 2: Parallel COPYs + upserts on separate pool connections.
	txs := buffer.GetTransactions()
	numTxs, numOps, err := m.insertAndUpsertParallel(ctx, txs, buffer)
	if err != nil {
		return 0, 0, fmt.Errorf("parallel insert/upsert for ledger %d: %w", ledgerSeq, err)
	}

	// Phase 3: Finalize — unlock channel accounts + advance cursor.
	if err := m.persistFinalize(ctx, ledgerSeq, txs, cursorName); err != nil {
		return 0, 0, fmt.Errorf("finalizing ledger %d: %w", ledgerSeq, err)
	}

	return numTxs, numOps, nil
}

// persistFKPrerequisites inserts trustline assets and contract tokens in a committed
// transaction so that FK parent rows are visible to parallel balance upserts.
func (m *ingestService) persistFKPrerequisites(ctx context.Context, ledgerSeq uint32, buffer *indexer.IndexerBuffer) error {
	if err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		uniqueAssets := buffer.GetUniqueTrustlineAssets()
		if len(uniqueAssets) > 0 {
			if err := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, uniqueAssets); err != nil {
				return fmt.Errorf("inserting trustline assets for ledger %d: %w", ledgerSeq, err)
			}
		}

		contracts, err := m.prepareNewContractTokens(ctx, dbTx, buffer.GetUniqueSEP41ContractTokensByID(), buffer.GetSACContracts())
		if err != nil {
			return fmt.Errorf("preparing contract tokens for ledger %d: %w", ledgerSeq, err)
		}
		if len(contracts) > 0 {
			if err = m.models.Contract.BatchInsert(ctx, dbTx, contracts); err != nil {
				return fmt.Errorf("inserting contracts for ledger %d: %w", ledgerSeq, err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d contract tokens", len(contracts))
		}

		return nil
	}); err != nil {
		return fmt.Errorf("FK prerequisites transaction: %w", err)
	}
	return nil
}

// persistFinalize unlocks channel accounts and advances the cursor in a committed transaction.
// The cursor update is the idempotency marker for crash recovery.
func (m *ingestService) persistFinalize(ctx context.Context, ledgerSeq uint32, txs []*types.Transaction, cursorName string) error {
	if err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		if err := m.unlockChannelAccounts(ctx, dbTx, txs); err != nil {
			return fmt.Errorf("unlocking channel accounts for ledger %d: %w", ledgerSeq, err)
		}
		if err := m.models.IngestStore.Update(ctx, dbTx, cursorName, ledgerSeq); err != nil {
			return fmt.Errorf("updating cursor for ledger %d: %w", ledgerSeq, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("finalize transaction: %w", err)
	}
	return nil
}

// startLiveIngestion begins continuous ingestion from the last checkpoint ledger,
// acquiring an advisory lock to prevent concurrent ingestion instances.
func (m *ingestService) startLiveIngestion(ctx context.Context) error {
	conn, err := m.models.DB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring a connection from the pool: %w", err)
	}
	defer conn.Release()

	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently
	if lockAcquired, err := db.AcquireAdvisoryLock(ctx, conn, m.advisoryLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	} else if !lockAcquired {
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(ctx, conn, m.advisoryLockID); err != nil {
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
		m.appMetrics.Ingestion.LatestLedger.Set(float64(startLedger))
		m.appMetrics.Ingestion.OldestLedger.Set(float64(startLedger))
	} else {
		// Initialize metrics from DB state so Prometheus reflects backfill progress after restart
		oldestIngestedLedger, oldestErr := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName)
		if oldestErr != nil {
			return fmt.Errorf("getting oldest ledger cursor: %w", oldestErr)
		}
		m.appMetrics.Ingestion.OldestLedger.Set(float64(oldestIngestedLedger))
		m.appMetrics.Ingestion.LatestLedger.Set(float64(latestIngestedLedger))
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}

	// Set initial lag now that the backend buffer is populated
	if backendTip, lagErr := m.ledgerBackend.GetLatestLedgerSequence(ctx); lagErr == nil {
		m.appMetrics.Ingestion.LagLedgers.Set(float64(backendTip - startLedger))
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
	buffer := indexer.NewIndexerBuffer()
	for {
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}

		totalStart := time.Now()
		processStart := time.Now()
		buffer.Clear()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("process_ledger").Observe(time.Since(processStart).Seconds())

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, buffer)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("insert_into_db").Observe(time.Since(dbStart).Seconds())
		totalIngestionDuration := time.Since(totalStart).Seconds()
		m.appMetrics.Ingestion.Duration.Observe(totalIngestionDuration)
		m.appMetrics.Ingestion.TransactionsTotal.Add(float64(numTransactionProcessed))
		m.appMetrics.Ingestion.OperationsTotal.Add(float64(numOperationProcessed))
		m.appMetrics.Ingestion.LedgersProcessed.Add(float64(1))
		m.appMetrics.Ingestion.LatestLedger.Set(float64(currentLedger))

		// Update lag metric (non-blocking atomic read)
		if backendTip, lagErr := m.ledgerBackend.GetLatestLedgerSequence(ctx); lagErr == nil {
			m.appMetrics.Ingestion.LagLedgers.Set(float64(backendTip - currentLedger))
		}

		// Periodically sync oldest ledger metric from DB (picks up changes from backfill jobs)
		if currentLedger%oldestLedgerSyncInterval == 0 {
			if oldest, syncErr := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName); syncErr == nil {
				m.appMetrics.Ingestion.OldestLedger.Set(float64(oldest))
			}
		}

		log.Ctx(ctx).Infof("Ingested ledger %d in %.4fs", currentLedger, totalIngestionDuration)
		currentLedger++
	}
}

// ingestProcessedDataWithRetry wraps PersistLedgerData with retry logic.
func (m *ingestService) ingestProcessedDataWithRetry(ctx context.Context, currentLedger uint32, buffer *indexer.IndexerBuffer) (int, int, error) {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		numTxs, numOps, err := m.PersistLedgerData(ctx, currentLedger, buffer, m.latestLedgerCursorName)
		if err == nil {
			return numTxs, numOps, nil
		}
		lastErr = err
		m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("db_persist").Inc()

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
	m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("db_persist").Inc()
	m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("db_persist").Inc()
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
func (m *ingestService) prepareNewContractTokens(ctx context.Context, dbTx pgx.Tx, sep41ContractTokensByID map[string]types.ContractType, sacContracts map[string]*data.Contract) ([]*data.Contract, error) {
	if len(sep41ContractTokensByID) == 0 && len(sacContracts) == 0 {
		return nil, nil
	}

	// Build list of contract IDs to check
	contractAddresses := make([]string, 0, len(sep41ContractTokensByID))
	for address := range sep41ContractTokensByID {
		contractAddresses = append(contractAddresses, address)
	}
	for address := range sacContracts {
		contractAddresses = append(contractAddresses, address)
	}

	// Get existing contract IDs from DB (only checking the ones we need)
	existingAddresses, err := m.models.Contract.GetExisting(ctx, dbTx, contractAddresses)
	if err != nil {
		return nil, fmt.Errorf("getting existing contract IDs: %w", err)
	}
	existingSet := set.NewSet(existingAddresses...)

	// Collect new contract tokens
	var contracts []*data.Contract
	for address := range sacContracts {
		if existingSet.Contains(address) {
			continue
		}
		contracts = append(contracts, sacContracts[address])
	}

	var newSep41ContractAddresses []string
	for address := range sep41ContractTokensByID {
		if existingSet.Contains(address) {
			continue
		}
		newSep41ContractAddresses = append(newSep41ContractAddresses, address)
	}

	// Fetch metadata for new SEP-41 contracts via RPC (skip if no metadata service)
	if len(newSep41ContractAddresses) > 0 && m.contractMetadataService != nil {
		sep41Contracts, fetchErr := m.contractMetadataService.FetchSep41Metadata(ctx, newSep41ContractAddresses)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetching metadata for new SEP-41 contracts: %w", fetchErr)
		}
		contracts = append(contracts, sep41Contracts...)
	}

	return contracts, nil
}
