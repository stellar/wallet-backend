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
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	maxIngestProcessedDataRetries      = 5
	maxIngestProcessedDataRetryBackoff = 10 * time.Second
	oldestLedgerSyncInterval           = 100
)

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
		// 2.6: Per-protocol CAS-gated state production. The compare-and-swap on each
		// protocol cursor is the authoritative gate — exactly one of live ingestion or
		// protocol-migrate wins a given ledger. Staging (ProcessLedger) and persistence
		// run only for cursors that win the swap, so a protocol still backfilling (its
		// cursor behind tip) costs a single CAS and a continue. This block stays above
		// the protocol_wasms / protocol_contracts BatchInserts below, because
		// getEffectiveProtocolContracts overlays this-ledger buffered contracts onto the
		// committed set before those rows are inserted. It runs only at the live tip
		// (ledgerMeta != nil); backfill and loadtest skip protocol production.
		if ledgerMeta != nil && ledgerSeq != 0 && len(m.protocolProcessors) > 0 {
			ledgerCloseTime := ledgerMeta.LedgerCloseTime()
			contractEvents := buffer.GetContractEvents()
			expected := strconv.FormatUint(uint64(ledgerSeq-1), 10)
			next := strconv.FormatUint(uint64(ledgerSeq), 10)

			for protocolID, processor := range m.protocolProcessors {
				historyCursor := utils.ProtocolHistoryCursorName(protocolID)
				currentStateCursor := utils.ProtocolCurrentStateCursorName(protocolID)

				historySwapped, casErr := m.casProtocolCursor(ctx, dbTx, historyCursor, expected, next)
				if casErr != nil {
					return casErr
				}
				currentStateSwapped, casErr := m.casProtocolCursor(ctx, dbTx, currentStateCursor, expected, next)
				if casErr != nil {
					return casErr
				}
				if !historySwapped && !currentStateSwapped {
					// Behind tip (value mismatch) or not yet set up (missing row): another
					// process owns this ledger, so there is nothing to stage.
					continue
				}

				contracts, contractsErr := m.getEffectiveProtocolContracts(ctx, protocolID, bufferedContracts, classification)
				if contractsErr != nil {
					return fmt.Errorf("getting protocol contracts for %s at ledger %d: %w", protocolID, ledgerSeq, contractsErr)
				}
				input := ProtocolProcessorInput{
					LedgerSequence:    ledgerSeq,
					LedgerCloseTime:   ledgerCloseTime,
					ContractEvents:    contractEvents,
					ProtocolContracts: contracts,
					NetworkPassphrase: m.networkPassphrase,
				}
				start := time.Now()
				processErr := processor.ProcessLedger(ctx, input)
				m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())
				if processErr != nil {
					return fmt.Errorf("processing ledger %d for protocol %s: %w", ledgerSeq, protocolID, processErr)
				}

				if historySwapped {
					persistStart := time.Now()
					persistErr := processor.PersistHistory(ctx, dbTx)
					m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_history").Observe(time.Since(persistStart).Seconds())
					if persistErr != nil {
						return fmt.Errorf("persisting history for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
					}
				}
				if currentStateSwapped {
					persistStart := time.Now()
					persistErr := processor.PersistCurrentState(ctx, dbTx)
					m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_current_state").Observe(time.Since(persistStart).Seconds())
					if persistErr != nil {
						return fmt.Errorf("persisting current state for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
					}
				}
			}
		}

		if len(bufferedWasms) > 0 {
			wasmSlice := make([]data.ProtocolWasms, 0, len(bufferedWasms))
			for hash, wasm := range bufferedWasms {
				if pid, ok := classification[types.HashBytea(hash)]; ok {
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

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, ledgerMeta, buffer)
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

// casProtocolCursor performs the authoritative compare-and-swap on a protocol
// cursor. A missing cursor row (ErrCASCursorMissing) is operationally normal — the
// protocol is registered but protocol-setup / protocol-migrate has not initialized
// its cursor yet — so it is folded into a soft skip (false, nil); the data layer
// still records the cursor_missing query-error metric. A value mismatch (another
// process already owns this ledger) likewise returns (false, nil) from the model.
// Any other error is returned so the surrounding transaction aborts and retries.
func (m *ingestService) casProtocolCursor(ctx context.Context, dbTx pgx.Tx, cursorName, expected, next string) (bool, error) {
	swapped, err := m.models.IngestStore.CompareAndSwap(ctx, dbTx, cursorName, expected, next)
	if errors.Is(err, data.ErrCASCursorMissing) {
		log.Ctx(ctx).Debugf("protocol cursor %s missing at ledger %s; skipping production for this ledger", cursorName, next)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("comparing and swapping protocol cursor %s: %w", cursorName, err)
	}
	return swapped, nil
}

func (m *ingestService) getEffectiveProtocolContracts(
	ctx context.Context,
	protocolID string,
	bufferedContracts map[string]data.ProtocolContracts,
	classification map[types.HashBytea]string,
) ([]data.ProtocolContracts, error) {
	baseContracts, err := m.getProtocolContracts(ctx, protocolID)
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

	for _, contract := range bufferedContracts {
		if classification[contract.WasmHash] != protocolID {
			continue
		}
		out = append(out, contract)
	}
	return out, nil
}

// getProtocolContracts loads the committed protocol_contracts for a protocol
// directly from the DB. Called once per protocol per ledger in the live loop.
func (m *ingestService) getProtocolContracts(ctx context.Context, protocolID string) ([]data.ProtocolContracts, error) {
	if len(m.protocolProcessors) == 0 {
		return nil, nil
	}
	byProtocol, err := m.models.ProtocolContracts.BatchGetByProtocolIDs(ctx, []string{protocolID})
	if err != nil {
		return nil, fmt.Errorf("loading protocol contracts for %s: %w", protocolID, err)
	}
	return byProtocol[protocolID], nil
}

// ingestProcessedDataWithRetry wraps persistLedgerData with retry logic.
func (m *ingestService) ingestProcessedDataWithRetry(
	ctx context.Context,
	currentLedger uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	buffer *indexer.IndexerBuffer,
) (int, int, error) {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		numTxs, numOps, err := m.persistLedgerData(ctx, currentLedger, &ledgerMeta, buffer, data.LatestLedgerCursorName)
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
// in priority order. It returns a wasm hash → protocol_id map covering both
// previously-committed classifications and this-ledger matches, so live
// ProcessLedger can reason about same-ledger deploy/upgrade events before
// protocol_contracts is committed.
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
) (map[types.HashBytea]string, error) {
	if len(bufferedWasms) == 0 && len(bufferedContracts) == 0 {
		return nil, nil
	}

	bytecodesByHash := make(map[types.HashBytea][]byte, len(bufferedWasms))
	thisBatch := make(map[types.HashBytea]struct{}, len(bufferedWasms))
	for hash := range bufferedWasms {
		h := types.HashBytea(hash)
		bytecodesByHash[h] = bufferedBytecodes[hash]
		thisBatch[h] = struct{}{}
	}

	// Resolve hashes classified in earlier ledgers, skipping this-ledger
	// uploads (thisBatch) which the dispatcher classifies below.
	knownHashes := make([]types.HashBytea, 0, len(bufferedContracts))
	for _, c := range bufferedContracts {
		if _, inBatch := thisBatch[c.WasmHash]; inBatch {
			continue
		}
		knownHashes = append(knownHashes, c.WasmHash)
	}
	known, err := m.models.ProtocolWasms.GetClassifiedByHashes(ctx, dbTx, knownHashes)
	if err != nil {
		return nil, fmt.Errorf("resolving known protocol classifications: %w", err)
	}

	// protocolByWasm answers "what protocol owns this wasm hash?": previously
	// committed classifications overlaid with this-ledger matches. The two key
	// sets are disjoint (knownHashes excludes thisBatch), so the overlay never
	// actually collides.
	protocolByWasm := make(map[types.HashBytea]string, len(known))
	for hash, pid := range known {
		protocolByWasm[hash] = pid
	}
	if len(m.protocolValidators) == 0 {
		return protocolByWasm, nil
	}

	matches, err := DispatchClassification(
		ctx, dbTx, m.wasmSpecExtractor, m.protocolValidators,
		bytecodesByHash, bufferedContracts, m.rpcService, m.models, known,
		m.appMetrics.Ingestion.WasmClassificationFailuresTotal,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatching classification: %w", err)
	}
	for hash, pid := range matches {
		protocolByWasm[hash] = pid
	}
	return protocolByWasm, nil
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
