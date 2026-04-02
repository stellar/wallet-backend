package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

const (
	maxIngestProcessedDataRetries      = 5
	maxIngestProcessedDataRetryBackoff = 10 * time.Second
	oldestLedgerSyncInterval           = 100
	protocolContractRefreshInterval    = 100
)

// protocolContractCache caches classified protocol contracts to avoid per-ledger DB queries.
// Only accessed from the single-threaded live ingestion loop, so no mutex is needed.
type protocolContractCache struct {
	contractsByProtocol map[string][]data.ProtocolContracts
	lastRefreshLedger   uint32
}

func protocolStateCursorReady(cursorValue, ledgerSeq uint32) bool {
	if ledgerSeq == 0 {
		return true
	}

	return cursorValue >= ledgerSeq-1
}

// protocolProcessorsEligibleForProduction returns the processors that may persist
// history or current state for ledgerSeq. This is only a best-effort optimization:
// PersistLedgerData still performs the authoritative CAS check inside the DB
// transaction, so a later CAS loss can still skip persistence.
func (m *ingestService) protocolProcessorsEligibleForProduction(ctx context.Context, ledgerSeq uint32) (map[string]ProtocolProcessor, error) {
	if len(m.protocolProcessors) == 0 {
		return nil, nil
	}

	eligible := make(map[string]ProtocolProcessor, len(m.protocolProcessors))
	for protocolID, processor := range m.protocolProcessors {
		historyCursor := fmt.Sprintf("protocol_%s_history_cursor", protocolID)
		historyVal, err := m.models.IngestStore.Get(ctx, historyCursor)
		if err != nil {
			return nil, fmt.Errorf("reading history cursor for %s: %w", protocolID, err)
		}

		currentStateCursor := fmt.Sprintf("protocol_%s_current_state_cursor", protocolID)
		currentStateVal, err := m.models.IngestStore.Get(ctx, currentStateCursor)
		if err != nil {
			return nil, fmt.Errorf("reading current state cursor for %s: %w", protocolID, err)
		}

		if protocolStateCursorReady(historyVal, ledgerSeq) || protocolStateCursorReady(currentStateVal, ledgerSeq) {
			eligible[protocolID] = processor
		}
	}

	return eligible, nil
}

// PersistLedgerData persists processed ledger data to the database in a single atomic transaction.
// This is the shared core used by both live ingestion and loadtest.
// It handles: trustline assets, contract tokens, filtered data insertion,
// token changes, and cursor update.
func (m *ingestService) PersistLedgerData(ctx context.Context, ledgerSeq uint32, buffer *indexer.IndexerBuffer, cursorName string) (int, int, error) {
	var numTxs, numOps int

	err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		// 1. Insert unique trustline assets (FK prerequisite for trustline balances)
		uniqueAssets := buffer.GetUniqueTrustlineAssets()
		if len(uniqueAssets) > 0 {
			if txErr := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, uniqueAssets); txErr != nil {
				return fmt.Errorf("inserting trustline assets for ledger %d: %w", ledgerSeq, txErr)
			}
		}

		// 2. Insert new SAC contract tokens (filter existing, insert)
		contracts, txErr := m.prepareNewSACContracts(ctx, dbTx, buffer.GetSACContracts())
		if txErr != nil {
			return fmt.Errorf("preparing contract tokens for ledger %d: %w", ledgerSeq, txErr)
		}
		if len(contracts) > 0 {
			if txErr = m.models.Contract.BatchInsert(ctx, dbTx, contracts); txErr != nil {
				return fmt.Errorf("inserting contracts for ledger %d: %w", ledgerSeq, txErr)
			}
			log.Ctx(ctx).Infof("inserted %d SAC contract tokens", len(contracts))
		}

		// 2.5: Persist protocol wasms and contracts
		protocolWasms := buffer.GetProtocolWasms()
		if len(protocolWasms) > 0 {
			wasmSlice := make([]data.ProtocolWasms, 0, len(protocolWasms))
			for _, wasm := range protocolWasms {
				wasmSlice = append(wasmSlice, wasm)
			}
			if txErr = m.models.ProtocolWasms.BatchInsert(ctx, dbTx, wasmSlice); txErr != nil {
				return fmt.Errorf("inserting protocol wasms for ledger %d: %w", ledgerSeq, txErr)
			}
		}
		protocolContracts := buffer.GetProtocolContracts()
		if len(protocolContracts) > 0 {
			contractSlice := make([]data.ProtocolContracts, 0, len(protocolContracts))
			for _, contract := range protocolContracts {
				contractSlice = append(contractSlice, contract)
			}
			if txErr = m.models.ProtocolContracts.BatchInsert(ctx, dbTx, contractSlice); txErr != nil {
				return fmt.Errorf("inserting protocol contracts for ledger %d: %w", ledgerSeq, txErr)
			}
		}

		// 3. Insert transactions/operations/state_changes
		numTxs, numOps, txErr = m.insertIntoDB(ctx, dbTx, buffer)
		if txErr != nil {
			return fmt.Errorf("inserting processed data into db for ledger %d: %w", ledgerSeq, txErr)
		}

		// 4. Process token changes (trustline add/remove/update, native balance, SAC balance)
		if txErr = m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx,
			buffer.GetTrustlineChanges(),
			buffer.GetAccountChanges(),
			buffer.GetSACBalanceChanges(),
		); txErr != nil {
			return fmt.Errorf("processing token changes for ledger %d: %w", ledgerSeq, txErr)
		}

		// 5.5: Per-protocol dual CAS gating for state production
		if len(m.eligibleProtocolProcessors) > 0 {
			for protocolID, processor := range m.eligibleProtocolProcessors {
				if ledgerSeq == 0 {
					// No previous ledger to form an expected cursor value; skip CAS for this ledger.
					continue
				}
				historyCursor := fmt.Sprintf("protocol_%s_history_cursor", protocolID)
				currentStateCursor := fmt.Sprintf("protocol_%s_current_state_cursor", protocolID)

				expected := strconv.FormatUint(uint64(ledgerSeq-1), 10)
				next := strconv.FormatUint(uint64(ledgerSeq), 10)

				// --- History State Changes ---
				swapped, casErr := m.models.IngestStore.CompareAndSwap(ctx, dbTx, historyCursor, expected, next)
				if casErr != nil {
					return fmt.Errorf("CAS history cursor for %s: %w", protocolID, casErr)
				}
				if swapped {
					start := time.Now()
					persistErr := processor.PersistHistory(ctx, dbTx)
					m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_history").Observe(time.Since(start).Seconds())
					if persistErr != nil {
						return fmt.Errorf("persisting history for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
					}
				}

				// --- Current State ---
				swapped, casErr = m.models.IngestStore.CompareAndSwap(ctx, dbTx, currentStateCursor, expected, next)
				if casErr != nil {
					return fmt.Errorf("CAS current state cursor for %s: %w", protocolID, casErr)
				}
				if swapped {
					start := time.Now()
					persistErr := processor.PersistCurrentState(ctx, dbTx)
					m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_current_state").Observe(time.Since(start).Seconds())
					if persistErr != nil {
						return fmt.Errorf("persisting current state for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
					}
				}
			}
		}

		// 6. Update the specified cursor
		if txErr = m.models.IngestStore.Update(ctx, dbTx, cursorName, ledgerSeq); txErr != nil {
			return fmt.Errorf("updating cursor for ledger %d: %w", ledgerSeq, txErr)
		}

		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("persisting ledger data for ledger %d: %w", ledgerSeq, err)
	}

	return numTxs, numOps, nil
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
		err = m.checkpointService.PopulateFromCheckpoint(ctx, startLedger, func(dbTx pgx.Tx) error {
			return m.initializeCursors(ctx, dbTx, startLedger)
		})
		if err != nil {
			return fmt.Errorf("populating from checkpoint and initializing cursors: %w", err)
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
	for {
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}

		totalStart := time.Now()
		processStart := time.Now()
		buffer := indexer.NewIndexerBuffer()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("process_ledger").Observe(time.Since(processStart).Seconds())

		eligibleProcessors, err := m.protocolProcessorsEligibleForProduction(ctx, currentLedger)
		if err != nil {
			return fmt.Errorf("checking protocol state readiness for ledger %d: %w", currentLedger, err)
		}
		m.eligibleProtocolProcessors = eligibleProcessors

		// Run protocol state production (in-memory analysis before DB transaction) only
		// for processors that may actually persist this ledger.
		if err := m.produceProtocolStateForProcessors(ctx, ledgerMeta, currentLedger, eligibleProcessors); err != nil {
			return fmt.Errorf("producing protocol state for ledger %d: %w", currentLedger, err)
		}

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

func (m *ingestService) produceProtocolStateForProcessors(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, ledgerSeq uint32, processors map[string]ProtocolProcessor) error {
	if len(processors) == 0 {
		return nil
	}
	for protocolID, processor := range processors {
		contracts := m.getProtocolContracts(ctx, protocolID, ledgerSeq)
		input := ProtocolProcessorInput{
			LedgerSequence:    ledgerSeq,
			LedgerCloseMeta:   ledgerMeta,
			ProtocolContracts: contracts,
			NetworkPassphrase: m.networkPassphrase,
		}
		start := time.Now()
		if err := processor.ProcessLedger(ctx, input); err != nil {
			m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())
			return fmt.Errorf("processing ledger %d for protocol %s: %w", ledgerSeq, protocolID, err)
		}
		m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())
	}
	return nil
}

// getProtocolContracts returns cached contracts for a protocol, refreshing if stale.
func (m *ingestService) getProtocolContracts(ctx context.Context, protocolID string, currentLedger uint32) []data.ProtocolContracts {
	if m.protocolContractCache == nil {
		return nil
	}
	stale := m.protocolContractCache.lastRefreshLedger == 0 ||
		(currentLedger-m.protocolContractCache.lastRefreshLedger) >= protocolContractRefreshInterval

	if stale {
		m.appMetrics.Ingestion.ProtocolContractCacheAccess.WithLabelValues(protocolID, "miss").Inc()
		m.refreshProtocolContractCache(ctx, currentLedger)
	} else {
		m.appMetrics.Ingestion.ProtocolContractCacheAccess.WithLabelValues(protocolID, "hit").Inc()
	}

	return m.protocolContractCache.contractsByProtocol[protocolID]
}

// refreshProtocolContractCache reloads all protocol contracts from the DB.
// Only called from the single-threaded live ingestion loop.
func (m *ingestService) refreshProtocolContractCache(ctx context.Context, currentLedger uint32) {
	start := time.Now()
	protocolIDs := make([]string, 0, len(m.protocolProcessors))
	for protocolID := range m.protocolProcessors {
		protocolIDs = append(protocolIDs, protocolID)
	}
	newMap, err := m.models.ProtocolContracts.BatchGetByProtocolIDs(ctx, protocolIDs)
	if err != nil {
		log.Ctx(ctx).Warnf("Error refreshing protocol contract cache: %v; preserving previous entries", err)
		newMap = m.protocolContractCache.contractsByProtocol
	}

	m.protocolContractCache.contractsByProtocol = newMap
	m.protocolContractCache.lastRefreshLedger = currentLedger
	m.appMetrics.Ingestion.ProtocolContractCacheRefresh.Observe(time.Since(start).Seconds())
	if err != nil {
		log.Ctx(ctx).Warnf("Protocol contract cache refresh failed at ledger %d; will retry at next interval", currentLedger)
	} else {
		log.Ctx(ctx).Infof("Refreshed protocol contract cache at ledger %d", currentLedger)
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

// prepareNewSACContracts filters out existing contracts and returns new SAC contracts for insertion.
// SAC contracts get their metadata from ledger data (sacContracts parameter).
func (m *ingestService) prepareNewSACContracts(ctx context.Context, dbTx pgx.Tx, sacContracts map[string]*data.Contract) ([]*data.Contract, error) {
	if len(sacContracts) == 0 {
		return nil, nil
	}

	// Build list of contract IDs to check
	contractAddresses := make([]string, 0, len(sacContracts))
	for address := range sacContracts {
		contractAddresses = append(contractAddresses, address)
	}

	// Get existing contract IDs from DB (only checking the ones we need)
	existingAddresses, err := m.models.Contract.GetExisting(ctx, dbTx, contractAddresses)
	if err != nil {
		return nil, fmt.Errorf("getting existing contract IDs: %w", err)
	}
	existingSet := set.NewSet(existingAddresses...)

	// Collect new SAC contracts
	var contracts []*data.Contract
	for address := range sacContracts {
		if existingSet.Contains(address) {
			continue
		}
		contracts = append(contracts, sacContracts[address])
	}

	return contracts, nil
}
