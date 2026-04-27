package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	maxIngestProcessedDataRetries      = 5
	maxIngestProcessedDataRetryBackoff = 10 * time.Second
	oldestLedgerSyncInterval           = 100
	protocolContractRefreshInterval    = 100
)

// protocolContractCache caches classified protocol contracts to avoid per-ledger DB queries.
// Accessed only from the single-goroutine live ingestion loop; no locking needed.
type protocolContractCache struct {
	contractsByProtocol map[string][]data.ProtocolContracts
	lastRefreshLedger   uint32
}

type classificationOutcome struct {
	matches     map[types.HashBytea]string
	knownByHash map[types.HashBytea]string
}

func (o classificationOutcome) protocolIDForWasm(hash types.HashBytea) string {
	if pid, ok := o.matches[hash]; ok {
		return pid
	}
	return o.knownByHash[hash]
}

func protocolStateCursorReady(cursorValue, ledgerSeq uint32) bool {
	if ledgerSeq == 0 {
		return true
	}

	return cursorValue >= ledgerSeq-1
}

// protocolProductionTarget holds a processor together with per-cursor eligibility
// for state production on a single ledger. A cursor is eligible only when its row
// exists in ingest_store AND the cursor value has caught up to ledgerSeq-1. Rows
// that do not yet exist (protocol-setup / protocol-migrate has not run) keep the
// corresponding flag false so PersistLedgerData skips the CAS entirely — attempting
// one would surface ErrCASCursorMissing for a case that is operationally normal.
type protocolProductionTarget struct {
	processor            ProtocolProcessor
	historyEligible      bool
	currentStateEligible bool
}

// protocolProcessorsEligibleForProduction returns processors that may persist
// history or current state for ledgerSeq, with per-cursor eligibility flags.
// A protocol appears in the map only when at least one of its cursor rows exists
// and is at ledgerSeq-1. This is only a best-effort optimization: PersistLedgerData
// still performs the authoritative CAS check inside the DB transaction, so a later
// CAS loss (or mid-run row drop) can still skip persistence.
func (m *ingestService) protocolProcessorsEligibleForProduction(ctx context.Context, ledgerSeq uint32) (map[string]protocolProductionTarget, error) {
	if len(m.protocolProcessors) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(m.protocolProcessors)*2)
	for protocolID := range m.protocolProcessors {
		keys = append(keys, utils.ProtocolHistoryCursorName(protocolID))
		keys = append(keys, utils.ProtocolCurrentStateCursorName(protocolID))
	}

	cursorValues, err := m.models.IngestStore.GetMany(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("reading protocol cursors: %w", err)
	}

	eligible := make(map[string]protocolProductionTarget, len(m.protocolProcessors))
	for protocolID, processor := range m.protocolProcessors {
		historyVal, historyExists := cursorValues[utils.ProtocolHistoryCursorName(protocolID)]
		currentStateVal, currentStateExists := cursorValues[utils.ProtocolCurrentStateCursorName(protocolID)]

		historyEligible := historyExists && protocolStateCursorReady(historyVal, ledgerSeq)
		currentStateEligible := currentStateExists && protocolStateCursorReady(currentStateVal, ledgerSeq)

		switch {
		case !historyExists && !currentStateExists:
			if !m.protocolCursorInitPending[protocolID] {
				log.Ctx(ctx).Warnf("protocol %s registered but cursor rows absent; waiting on protocol-setup / protocol-migrate before producing state", protocolID)
				m.protocolCursorInitPending[protocolID] = true
			}
		case m.protocolCursorInitPending[protocolID]:
			log.Ctx(ctx).Infof("protocol %s cursor rows observed at ledger %d; state production can proceed once cursors catch up", protocolID, ledgerSeq)
			delete(m.protocolCursorInitPending, protocolID)
		}

		if historyEligible || currentStateEligible {
			eligible[protocolID] = protocolProductionTarget{
				processor:            processor,
				historyEligible:      historyEligible,
				currentStateEligible: currentStateEligible,
			}
		}
	}

	return eligible, nil
}

// PersistLedgerData persists processed ledger data to the database in a single atomic transaction.
// This is the shared core used by both live ingestion and loadtest.
// It handles: trustline assets, contract tokens, filtered data insertion,
// token changes, and cursor update.
func (m *ingestService) PersistLedgerData(ctx context.Context, ledgerSeq uint32, buffer *indexer.IndexerBuffer, cursorName string) (int, int, error) {
	return m.persistLedgerData(ctx, ledgerSeq, nil, buffer, cursorName)
}

func (m *ingestService) persistLedgerData(
	ctx context.Context,
	ledgerSeq uint32,
	ledgerMeta *xdr.LedgerCloseMeta,
	buffer *indexer.IndexerBuffer,
	cursorName string,
) (int, int, error) {
	var numTxs, numOps int
	// Track protocols that persisted current state in this transaction attempt
	// so we can reset their cache-loaded flag on rollback.
	var currentStatePersistedProtocols []string
	var invalidateProtocolContractCache bool

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

		// 2.5: Run protocol classification (black-box per protocol). Per-
		// protocol side effects (e.g. SEP-41 contract_tokens metadata) happen
		// inside this same dbTx via the validators' Validate calls. Live
		// protocol processors then stage ledger state from the classification
		// result before the generic protocol_wasms / protocol_contracts rows
		// are persisted below.
		bufferedWasms := buffer.GetProtocolWasms()
		bufferedBytecodes := buffer.GetProtocolWasmBytecodes()
		bufferedContracts := buffer.GetProtocolContracts()

		contractSlice := make([]data.ProtocolContracts, 0, len(bufferedContracts))
		for _, c := range bufferedContracts {
			contractSlice = append(contractSlice, c)
		}

		classification, classifyErr := m.runClassification(ctx, dbTx, bufferedWasms, bufferedBytecodes, contractSlice)
		if classifyErr != nil {
			return fmt.Errorf("classifying ledger %d: %w", ledgerSeq, classifyErr)
		}
		if ledgerMeta != nil && len(m.eligibleProtocolProcessors) > 0 {
			if produceErr := m.produceProtocolStateForProcessors(
				ctx, *ledgerMeta, ledgerSeq, m.eligibleProtocolProcessors, bufferedContracts, classification,
			); produceErr != nil {
				return fmt.Errorf("producing protocol state for ledger %d: %w", ledgerSeq, produceErr)
			}
		}

		if len(bufferedWasms) > 0 {
			wasmSlice := make([]data.ProtocolWasms, 0, len(bufferedWasms))
			for hash, wasm := range bufferedWasms {
				if pid, ok := classification.matches[types.HashBytea(hash)]; ok {
					stamped := pid
					wasm.ProtocolID = &stamped
				}
				wasmSlice = append(wasmSlice, wasm)
			}
			if txErr = m.models.ProtocolWasms.BatchInsert(ctx, dbTx, wasmSlice); txErr != nil {
				return fmt.Errorf("inserting protocol wasms for ledger %d: %w", ledgerSeq, txErr)
			}
		}
		if len(contractSlice) > 0 {
			if txErr = m.models.ProtocolContracts.BatchInsert(ctx, dbTx, contractSlice); txErr != nil {
				return fmt.Errorf("inserting protocol contracts for ledger %d: %w", ledgerSeq, txErr)
			}
			invalidateProtocolContractCache = true
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

		// 5.5: Per-protocol dual CAS gating for state production. Each cursor is
		// gated independently: the eligibility check guarantees the row exists,
		// and a missing-row CAS error here is treated as a soft skip (the metric
		// `cursor_missing` records it) rather than a fatal that would kill live
		// ingest — see ErrCASCursorMissing in internal/data/ingest_store.go.
		if len(m.eligibleProtocolProcessors) > 0 {
			for protocolID, target := range m.eligibleProtocolProcessors {
				if ledgerSeq == 0 {
					// No previous ledger to form an expected cursor value; skip CAS for this ledger.
					continue
				}
				historyCursor := utils.ProtocolHistoryCursorName(protocolID)
				currentStateCursor := utils.ProtocolCurrentStateCursorName(protocolID)

				expected := strconv.FormatUint(uint64(ledgerSeq-1), 10)
				next := strconv.FormatUint(uint64(ledgerSeq), 10)

				// --- History State Changes ---
				if target.historyEligible {
					swapped, casErr := m.models.IngestStore.CompareAndSwap(ctx, dbTx, historyCursor, expected, next)
					switch {
					case errors.Is(casErr, data.ErrCASCursorMissing):
						log.Ctx(ctx).Warnf("history cursor %s missing at ledger %d; skipping history production for this ledger", historyCursor, ledgerSeq)
					case casErr != nil:
						return fmt.Errorf("CAS history cursor for %s: %w", protocolID, casErr)
					case swapped:
						start := time.Now()
						persistErr := target.processor.PersistHistory(ctx, dbTx)
						m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_history").Observe(time.Since(start).Seconds())
						if persistErr != nil {
							return fmt.Errorf("persisting history for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
						}
					}
				}

				// --- Current State ---
				if target.currentStateEligible {
					swapped, casErr := m.models.IngestStore.CompareAndSwap(ctx, dbTx, currentStateCursor, expected, next)
					switch {
					case errors.Is(casErr, data.ErrCASCursorMissing):
						log.Ctx(ctx).Warnf("current_state cursor %s missing at ledger %d; skipping current-state production for this ledger", currentStateCursor, ledgerSeq)
					case casErr != nil:
						return fmt.Errorf("CAS current state cursor for %s: %w", protocolID, casErr)
					case swapped:
						// On first CAS success (handoff from migration), load current state
						// from DB into processor memory. Subsequent ledgers use the in-memory
						// state maintained by PersistCurrentState (write-through cache).
						if !m.protocolCurrentStateLoaded[protocolID] {
							loadStart := time.Now()
							if loadErr := target.processor.LoadCurrentState(ctx, dbTx); loadErr != nil {
								return fmt.Errorf("loading current state for %s at ledger %d: %w", protocolID, ledgerSeq, loadErr)
							}
							m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "load_current_state").Observe(time.Since(loadStart).Seconds())
							m.protocolCurrentStateLoaded[protocolID] = true
						}

						// Any rollback after a successful current-state CAS can leave the
						// processor's in-memory cache ahead of committed DB state, either
						// because we just loaded it for handoff or because PersistCurrentState
						// mutates the write-through cache before a later transactional failure.
						currentStatePersistedProtocols = append(currentStatePersistedProtocols, protocolID)

						start := time.Now()
						persistErr := target.processor.PersistCurrentState(ctx, dbTx)
						m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_current_state").Observe(time.Since(start).Seconds())
						if persistErr != nil {
							return fmt.Errorf("persisting current state for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
						}
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
		// Transaction rolled back — processor in-memory state loaded inside the
		// rolled-back transaction may not match the committed DB state. Reset
		// loaded flags to force a DB reload on the next successful CAS attempt.
		for _, pid := range currentStatePersistedProtocols {
			m.protocolCurrentStateLoaded[pid] = false
		}
		return 0, 0, fmt.Errorf("persisting ledger data for ledger %d: %w", ledgerSeq, err)
	}
	if invalidateProtocolContractCache && m.protocolContractCache != nil {
		// Force the next ledger to reload committed protocol_contracts from the DB
		// instead of serving a pre-commit snapshot from cache.
		m.protocolContractCache.lastRefreshLedger = 0
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
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
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
	if err := m.models.IngestStore.Update(ctx, dbTx, data.LatestLedgerCursorName, ledger); err != nil {
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
		ledgerMeta, ledgerErr := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff,
			func(ctx context.Context) (xdr.LedgerCloseMeta, error) {
				return m.ledgerBackend.GetLedger(ctx, currentLedger)
			},
			func(attempt int, err error, backoff time.Duration) {
				log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
					currentLedger, attempt+1, maxLedgerFetchRetries, err, backoff)
			},
		)
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

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetryLive(ctx, currentLedger, &ledgerMeta, buffer)
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

func (m *ingestService) produceProtocolStateForProcessors(
	ctx context.Context,
	ledgerMeta xdr.LedgerCloseMeta,
	ledgerSeq uint32,
	targets map[string]protocolProductionTarget,
	bufferedContracts map[string]data.ProtocolContracts,
	classification classificationOutcome,
) error {
	if len(targets) == 0 {
		return nil
	}
	for protocolID, target := range targets {
		contracts, err := m.getEffectiveProtocolContracts(ctx, protocolID, ledgerSeq, bufferedContracts, classification)
		if err != nil {
			return fmt.Errorf("getting protocol contracts for %s at ledger %d: %w", protocolID, ledgerSeq, err)
		}
		input := ProtocolProcessorInput{
			LedgerSequence:    ledgerSeq,
			LedgerCloseMeta:   ledgerMeta,
			ProtocolContracts: contracts,
			NetworkPassphrase: m.networkPassphrase,
		}
		start := time.Now()
		if err := target.processor.ProcessLedger(ctx, input); err != nil {
			m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())
			return fmt.Errorf("processing ledger %d for protocol %s: %w", ledgerSeq, protocolID, err)
		}
		m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())
	}
	return nil
}

func (m *ingestService) getEffectiveProtocolContracts(
	ctx context.Context,
	protocolID string,
	currentLedger uint32,
	bufferedContracts map[string]data.ProtocolContracts,
	classification classificationOutcome,
) ([]data.ProtocolContracts, error) {
	baseContracts, err := m.getProtocolContracts(ctx, protocolID, currentLedger)
	if err != nil {
		return nil, err
	}
	if len(bufferedContracts) == 0 {
		return baseContracts, nil
	}

	out := make([]data.ProtocolContracts, 0, len(baseContracts)+len(bufferedContracts))
	for _, contract := range baseContracts {
		if _, updatedThisLedger := bufferedContracts[string(contract.ContractID)]; updatedThisLedger {
			continue
		}
		out = append(out, contract)
	}

	contractIDs := make([]string, 0, len(bufferedContracts))
	for contractID := range bufferedContracts {
		contractIDs = append(contractIDs, contractID)
	}
	sort.Strings(contractIDs)

	for _, contractID := range contractIDs {
		contract := bufferedContracts[contractID]
		if classification.protocolIDForWasm(contract.WasmHash) != protocolID {
			continue
		}
		out = append(out, contract)
	}
	return out, nil
}

// getProtocolContracts returns cached contracts for a protocol, refreshing if stale.
func (m *ingestService) getProtocolContracts(ctx context.Context, protocolID string, currentLedger uint32) ([]data.ProtocolContracts, error) {
	if m.protocolContractCache == nil {
		return nil, nil
	}
	stale := m.protocolContractCache.lastRefreshLedger == 0 ||
		(currentLedger-m.protocolContractCache.lastRefreshLedger) >= protocolContractRefreshInterval

	if stale {
		m.appMetrics.Ingestion.ProtocolContractCacheAccess.WithLabelValues(protocolID, "miss").Inc()
		if err := m.refreshProtocolContractCache(ctx, currentLedger); err != nil {
			return nil, err
		}
	} else {
		m.appMetrics.Ingestion.ProtocolContractCacheAccess.WithLabelValues(protocolID, "hit").Inc()
	}

	return m.protocolContractCache.contractsByProtocol[protocolID], nil
}

// refreshProtocolContractCache reloads all protocol contracts from the DB in a
// single batch query. On failure, the existing cache and lastRefreshLedger are
// left untouched so the next ledger retries. If the cache has never been
// populated (first-refresh failure), the error is returned so the caller can
// fail the ledger loudly rather than letting processors run with empty state
// while the CAS gating still advances per-protocol cursors.
func (m *ingestService) refreshProtocolContractCache(ctx context.Context, currentLedger uint32) error {
	start := time.Now()
	protocolIDs := make([]string, 0, len(m.protocolProcessors))
	for protocolID := range m.protocolProcessors {
		protocolIDs = append(protocolIDs, protocolID)
	}
	newMap, err := m.models.ProtocolContracts.BatchGetByProtocolIDs(ctx, protocolIDs)
	m.appMetrics.Ingestion.ProtocolContractCacheRefresh.Observe(time.Since(start).Seconds())
	if err != nil {
		m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("protocol_contract_cache_refresh").Inc()
		log.Ctx(ctx).Warnf("Protocol contract cache refresh failed at ledger %d; preserving previous entries, will retry on next ledger: %v", currentLedger, err)
		if len(m.protocolContractCache.contractsByProtocol) == 0 {
			return fmt.Errorf("refreshing protocol contract cache at ledger %d (cache never loaded): %w", currentLedger, err)
		}
		return nil
	}
	m.protocolContractCache.contractsByProtocol = newMap
	m.protocolContractCache.lastRefreshLedger = currentLedger
	log.Ctx(ctx).Infof("Refreshed protocol contract cache at ledger %d", currentLedger)
	return nil
}

// ingestProcessedDataWithRetry wraps PersistLedgerData with retry logic.
func (m *ingestService) ingestProcessedDataWithRetry(ctx context.Context, currentLedger uint32, buffer *indexer.IndexerBuffer) (int, int, error) {
	return m.ingestProcessedDataWithRetryLive(ctx, currentLedger, nil, buffer)
}

func (m *ingestService) ingestProcessedDataWithRetryLive(
	ctx context.Context,
	currentLedger uint32,
	ledgerMeta *xdr.LedgerCloseMeta,
	buffer *indexer.IndexerBuffer,
) (int, int, error) {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		numTxs, numOps, err := m.persistLedgerData(ctx, currentLedger, ledgerMeta, buffer, data.LatestLedgerCursorName)
		if err == nil {
			return numTxs, numOps, nil
		}
		lastErr = err
		m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("db_persist").Inc()
		if attempt == maxIngestProcessedDataRetries-1 {
			break
		}

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

// runClassification builds a ValidationInput from this ledger's buffered
// raw WASMs and contracts and dispatches to each registered ProtocolValidator
// in priority order. The returned outcome carries both new wasm matches for
// this batch and previously-known protocol IDs for buffered contract hashes,
// so live ProcessLedger can reason about same-ledger deploy/upgrade events
// before protocol_contracts is committed.
//
// Validator side effects (e.g. SEP-41 contract_tokens metadata writes) happen
// inside dbTx via the validators themselves and commit atomically with the
// classification verdict.
func (m *ingestService) runClassification(
	ctx context.Context,
	dbTx pgx.Tx,
	bufferedWasms map[string]data.ProtocolWasms,
	bufferedBytecodes map[string][]byte,
	bufferedContracts []data.ProtocolContracts,
) (classificationOutcome, error) {
	out := classificationOutcome{}
	if len(bufferedWasms) == 0 && len(bufferedContracts) == 0 {
		return out, nil
	}

	rawWasms := make([]RawWasm, 0, len(bufferedWasms))
	thisBatch := make(map[types.HashBytea]struct{}, len(bufferedWasms))
	for hash := range bufferedWasms {
		bytecode := bufferedBytecodes[hash]
		rawWasms = append(rawWasms, RawWasm{
			Hash:     types.HashBytea(hash),
			Bytecode: bytecode,
		})
		thisBatch[types.HashBytea(hash)] = struct{}{}
	}

	known, err := ResolveKnownProtocols(ctx, dbTx, bufferedContracts, thisBatch)
	if err != nil {
		return out, fmt.Errorf("resolving known protocol classifications: %w", err)
	}
	out.knownByHash = known
	if len(m.protocolValidators) == 0 {
		return out, nil
	}

	matches, err := DispatchClassification(
		ctx, dbTx, m.wasmSpecExtractor, m.protocolValidators,
		rawWasms, bufferedContracts, m.rpcService, m.models, known,
	)
	if err != nil {
		return out, fmt.Errorf("dispatching classification: %w", err)
	}
	out.matches = matches
	return out, nil
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
