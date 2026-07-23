package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	lagMetricUpdateInterval            = 1 * time.Second
	// advisoryUnlockTimeout bounds the detached advisory-lock release at shutdown so a wedged
	// network session cannot block teardown (and the later pool close) indefinitely.
	advisoryUnlockTimeout = 10 * time.Second
)

// persistLedgerData persists processed ledger data to the database in a single
// atomic transaction. It handles: trustline assets, contract tokens, filtered
// data insertion, token changes, and cursor update. plan is this ledger's
// classification plan, computed by prepareClassificationPlan before any
// transaction opens (RPC calls already resolved); pass the same plan across
// ingestProcessedDataWithRetry's retry attempts so a retry never re-issues
// RPC calls. plan may be nil when there was nothing to classify this ledger.
func (m *ingestService) persistLedgerData(
	ctx context.Context,
	ledgerSeq uint32,
	ledgerMeta *xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
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

		// 2.5: Apply protocol classification (black-box per protocol). plan was
		// computed by prepareClassificationPlan before this transaction opened,
		// so any RPC calls (e.g. SEP-41 metadata) already happened;
		// ApplyClassificationPlan only performs each validator's DB writes
		// here, atomically with the classification verdict and wasm/contract
		// rows below. Live protocol processors then stage ledger state from
		// the classification result before the generic protocol_wasms /
		// protocol_contracts rows are persisted below.
		bufferedWasms := buffer.GetProtocolWasms()
		bufferedContracts := buffer.GetProtocolContracts()

		contractSlice := make([]data.ProtocolContracts, 0, len(bufferedContracts))
		for _, c := range bufferedContracts {
			contractSlice = append(contractSlice, c)
		}

		var classification map[types.HashBytea]string
		if plan != nil {
			classification = plan.Matches
		}
		if txErr = ApplyClassificationPlan(ctx, dbTx, m.models, plan, m.appMetrics.Ingestion.WasmClassificationFailuresTotal); txErr != nil {
			return fmt.Errorf("applying classification for ledger %d: %w", ledgerSeq, txErr)
		}

		// 2.6: Per-protocol CAS-gated state production. The compare-and-swap on each
		// protocol cursor is the authoritative gate — exactly one of live ingestion or
		// protocol-migrate wins a given ledger. Staging (ProcessLedger) and persistence
		// run only for cursors that win the swap, so a protocol still backfilling (its
		// cursor behind tip) costs a single CAS and a continue.
		if ledgerMeta != nil && ledgerSeq != 0 && len(m.protocolProcessors) > 0 {
			ledgerCloseTime := ledgerMeta.LedgerCloseTime()
			contractEvents := buffer.GetContractEvents()
			expected := strconv.FormatUint(uint64(ledgerSeq-1), 10)
			next := strconv.FormatUint(uint64(ledgerSeq), 10)

			// Resolve protocol membership once for the contracts that emitted events
			// this ledger. One bounded query serves every protocol; ledgers with no
			// contract events skip it entirely. The buffered overlay (below) covers
			// contracts deployed or upgraded this ledger, which are not yet committed.
			var committedByProtocol map[string][]data.ProtocolContracts
			if eventContractIDs := distinctEventContractIDs(contractEvents); len(eventContractIDs) > 0 {
				var lookupErr error
				committedByProtocol, lookupErr = m.models.ProtocolContracts.BatchGetByContractIDs(ctx, eventContractIDs)
				if lookupErr != nil {
					return fmt.Errorf("resolving protocol contracts for ledger %d: %w", ledgerSeq, lookupErr)
				}
			}

			for protocolID, processor := range m.protocolProcessors {
				// Only attempt the CAS for a cursor m.protocolCursors believes exists (see
				// snapshotProtocolCursors/reprobeProtocolCursors): a cursor known not yet
				// initialized is skipped entirely — no DB round trip, no metric — since
				// there is nothing to CAS against. This also means casProtocolCursor only
				// ever sees ErrCASCursorMissing for a cursor that existed as of the last
				// snapshot/re-probe, i.e. a genuine incident, not the operationally normal
				// not-yet-initialized case.
				var historySwapped, currentStateSwapped bool
				if m.protocolCursors.historyExists[protocolID] {
					var casErr error
					historyCursor := utils.ProtocolHistoryCursorName(protocolID)
					historySwapped, casErr = m.casProtocolCursor(ctx, dbTx, historyCursor, expected, next)
					if casErr != nil {
						return casErr
					}
				}
				if m.protocolCursors.currentStateExists[protocolID] {
					var casErr error
					currentStateCursor := utils.ProtocolCurrentStateCursorName(protocolID)
					currentStateSwapped, casErr = m.casProtocolCursor(ctx, dbTx, currentStateCursor, expected, next)
					if casErr != nil {
						return casErr
					}
				}
				if !historySwapped && !currentStateSwapped {
					// Behind tip (value mismatch), not yet set up (both cursors known
					// missing), or the value mismatch a live CAS returns when another
					// process already owns this ledger: nothing to stage.
					continue
				}

				contracts := getEffectiveProtocolContracts(protocolID, committedByProtocol[protocolID], bufferedContracts, classification)
				input := ProtocolProcessorInput{
					LedgerSequence:    ledgerSeq,
					LedgerCloseTime:   ledgerCloseTime,
					ContractEvents:    contractEvents,
					ProtocolContracts: contracts,
					StagingMode:       StagingModeBoth,
				}
				// Reset before staging so a retried transaction (ingestProcessedDataWithRetry)
				// re-stages cleanly; the processor is long-lived and accumulates across
				// ProcessLedger calls.
				processor.Reset()
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
			buffer.GetLiquidityPoolShareChanges(),
			buffer.GetLiquidityPoolChanges(),
		); txErr != nil {
			return fmt.Errorf("processing token changes for ledger %d: %w", ledgerSeq, txErr)
		}

		// 6. Update the specified cursor. The live latest-ledger cursor is guarded: a session
		// that silently lost its advisory lock (server-side failover, see startLiveIngestion's
		// checkLockSession) must not blindly overwrite a value a second instance already
		// advanced, or the cursor could regress. All other cursors keep the plain blind
		// upsert — only one process ever owns them by construction.
		if cursorName == data.LatestLedgerCursorName {
			if txErr = m.models.IngestStore.UpdateGuarded(ctx, dbTx, cursorName, ledgerSeq); txErr != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", ledgerSeq, txErr)
			}
		} else if txErr = m.models.IngestStore.Update(ctx, dbTx, cursorName, ledgerSeq); txErr != nil {
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

	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently.
	// Until the lock is confirmed held, the connection is released on every error path; once it
	// is held, the deferred release below owns the connection's lifecycle.
	lockAcquired, err := db.AcquireAdvisoryLock(ctx, conn, m.advisoryLockID)
	if err != nil {
		conn.Release()
		return fmt.Errorf("acquiring advisory lock: %w", err)
	}
	if !lockAcquired {
		conn.Release()
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		// Detach from ctx (a shutdown signal cancels it before this defer runs, and pgx
		// refuses to execute a query on an already-cancelled context) but keep a finite
		// deadline so a wedged session cannot block teardown forever. conn.Release only
		// returns the connection to the pool — it does not end the session — so on any
		// unlock failure we instead destroy the connection, which ends its session and
		// releases the advisory lock server-side, rather than handing a lock-holding
		// connection back to the pool.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockTimeout)
		defer cancel()
		if unlockErr := db.ReleaseAdvisoryLock(releaseCtx, conn, m.advisoryLockID); unlockErr != nil {
			log.Ctx(ctx).Errorf("releasing advisory lock, destroying connection to end its session: %v", unlockErr)
			if closeErr := conn.Hijack().Close(releaseCtx); closeErr != nil {
				log.Ctx(ctx).Warnf("closing advisory-lock connection after failed unlock: %v", closeErr)
			}
			return
		}
		conn.Release()
	}()

	// Snapshot which protocols' history/current-state cursors already exist, once, right
	// after the lock is confirmed held. See casProtocolCursor and snapshotProtocolCursors.
	if err := m.snapshotProtocolCursors(ctx); err != nil {
		return fmt.Errorf("snapshotting protocol cursors: %w", err)
	}

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

	// Re-enrich any SAC contract_tokens rows still left at their ledger-derived
	// defaults — covering both a fresh load whose enrichment just failed and rows
	// left stale by an earlier run. This is a best-effort startup pass: a failure is
	// logged and ingestion proceeds, because the rows keep working defaults and the
	// next restart retries.
	if err := m.checkpointService.EnrichStaleSACMetadata(ctx); err != nil {
		log.Ctx(ctx).Errorf("enriching stale SAC metadata at startup (defaults retained, retried on restart): %v", err)
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}

	// checkLockSession probes the SAME connection that holds the advisory lock: since we
	// never unlock mid-run, that session staying alive is equivalent to the lock still being
	// held. A CNPG failover kills the session server-side without this process observing the
	// TCP disconnect, silently releasing the lock while pgxpool never destroys the (now
	// server-dead) pooled conn — so without this probe, ingestLiveLedgers would keep writing
	// through other pool connections even after a second instance acquires the lock.
	checkLockSession := func(probeCtx context.Context) error {
		var one int
		return conn.QueryRow(probeCtx, "SELECT 1").Scan(&one)
	}

	return m.ingestLiveLedgers(ctx, startLedger, checkLockSession)
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

// lagLedgers returns how far latestIngested trails backendTip, and false when there is no
// valid measurement yet. GetLatestLedgerSequence reports 0 until the backend delivers its first
// batch, so an unsigned subtraction would otherwise underflow into a ~4-billion-ledger lag spike
// and trip false alerts during slow initial datastore fetches.
func lagLedgers(backendTip, latestIngested uint32) (float64, bool) {
	if backendTip < latestIngested {
		return 0, false
	}
	return float64(backendTip - latestIngested), true
}

// ingestLiveLedgers continuously processes ledgers starting from startLedger,
// updating cursors and metrics after each successful ledger. checkLockSession
// is called once per ledger to verify the advisory-lock-holding Postgres
// session is still alive (see startLiveIngestion): a CNPG failover can kill
// that session server-side without this process observing the disconnect, so
// the lock is silently released while this loop keeps writing through other
// pool connections. Failing this probe is treated as fatal so the process
// exits and can re-acquire the lock cleanly on restart, rather than racing a
// second instance that acquired it in the meantime.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32, checkLockSession func(ctx context.Context) error) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", currentLedger)

	// Refresh the lag gauge off the consumer goroutine. GetLatestLedgerSequence contends on the
	// datastore buffer's internal lock, which a download worker can hold while blocked on a full
	// queue; calling it on this goroutine — the only one that drains that queue — would deadlock.
	// A dedicated goroutine keeps the consumer draining, so the lock is always released promptly.
	var latestIngested atomic.Uint32
	latestIngested.Store(startLedger - 1)
	lagCtx, cancelLag := context.WithCancel(ctx)
	defer cancelLag()
	go func() {
		ticker := time.NewTicker(lagMetricUpdateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-lagCtx.Done():
				return
			case <-ticker.C:
				if backendTip, lagErr := m.ledgerBackend.GetLatestLedgerSequence(lagCtx); lagErr == nil {
					if lag, ok := lagLedgers(backendTip, latestIngested.Load()); ok {
						m.appMetrics.Ingestion.LagLedgers.Set(lag)
					}
				}
			}
		}
	}()

	for {
		if probeErr := checkLockSession(ctx); probeErr != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("advisory lock session is no longer alive, the lock may have been lost: %w", probeErr)
		}

		fetchStart := time.Now()
		ledgerMeta, ledgerErr := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff,
			func(ctx context.Context) (xdr.LedgerCloseMeta, error) {
				return m.ledgerBackend.GetLedger(ctx, currentLedger)
			},
			func(attempt int, err error, backoff time.Duration) {
				m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("ledger_fetch").Inc()
				log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
					currentLedger, attempt+1, maxLedgerFetchRetries, err, backoff)
			},
			m.isPermanentFetchError,
		)
		if ledgerErr != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}
		m.appMetrics.Ingestion.LedgerFetchDuration.Observe(time.Since(fetchStart).Seconds())

		totalStart := time.Now()
		processStart := time.Now()
		buffer := indexer.NewIndexerBuffer()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("process_ledger").Observe(time.Since(processStart).Seconds())

		// Classification runs once here, entirely before any database
		// transaction opens (RPC prefetch happens now, not while row locks are
		// held), and the resulting plan is reused verbatim across every retry
		// attempt below.
		classifyStart := time.Now()
		plan, err := m.prepareClassificationPlan(ctx, buffer.GetProtocolWasms(), buffer.GetProtocolWasmBytecodes(), buffer.GetProtocolContracts())
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("preparing classification plan for ledger %d: %w", currentLedger, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("prepare_classification").Observe(time.Since(classifyStart).Seconds())

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, ledgerMeta, plan, buffer)
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

		// Publish the just-ingested ledger for the lag updater goroutine started above.
		latestIngested.Store(currentLedger)

		// Periodically sync oldest ledger metric from DB (picks up changes from backfill jobs),
		// and re-probe protocol cursors that were missing at the last snapshot/re-probe (picks
		// up a protocol-setup/migrate run that has initialized one since — see
		// reprobeProtocolCursors).
		if currentLedger%oldestLedgerSyncInterval == 0 {
			if oldest, syncErr := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName); syncErr == nil {
				m.appMetrics.Ingestion.OldestLedger.Set(float64(oldest))
			}
			m.reprobeProtocolCursors(ctx)
		}

		log.Ctx(ctx).Infof("Ingested ledger %d in %.4fs", currentLedger, totalIngestionDuration)
		currentLedger++
	}
}

// protocolCursorSnapshot records, per protocol, whether its history and
// current-state ingest_store cursor rows exist. It only ever promotes an
// entry from missing to existing (see reprobeProtocolCursors) — a row
// vanishing after having existed is the genuine incident casProtocolCursor's
// error path handles, not something this snapshot demotes on its own. It is
// read and mutated only from the single live-ingestion goroutine — including
// retried persistLedgerData attempts, which run synchronously on it — so it
// needs no locking.
type protocolCursorSnapshot struct {
	historyExists      map[string]bool
	currentStateExists map[string]bool
}

// snapshotProtocolCursors populates m.protocolCursors from the DB, once, so
// casProtocolCursor's callers know which protocols' history/current-state
// cursors are operationally not-yet-initialized (skip the CAS silently)
// versus expected-present (a CAS reporting the row missing is a genuine
// incident). Called once by startLiveIngestion right after the advisory lock
// is confirmed held; ingestLiveLedgers re-probes the still-missing subset on
// the oldestLedgerSyncInterval cadence via reprobeProtocolCursors.
func (m *ingestService) snapshotProtocolCursors(ctx context.Context) error {
	if len(m.protocolProcessors) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m.protocolProcessors)*2)
	for protocolID := range m.protocolProcessors {
		keys = append(keys, utils.ProtocolHistoryCursorName(protocolID), utils.ProtocolCurrentStateCursorName(protocolID))
	}
	existing, err := m.models.IngestStore.GetMany(ctx, keys)
	if err != nil {
		return fmt.Errorf("getting protocol cursor rows: %w", err)
	}
	for protocolID := range m.protocolProcessors {
		_, hExists := existing[utils.ProtocolHistoryCursorName(protocolID)]
		_, csExists := existing[utils.ProtocolCurrentStateCursorName(protocolID)]
		m.protocolCursors.historyExists[protocolID] = hExists
		m.protocolCursors.currentStateExists[protocolID] = csExists
		if !hExists {
			log.Ctx(ctx).Infof("protocol %s history production disabled; cursor not initialized", protocolID)
		}
		if !csExists {
			log.Ctx(ctx).Infof("protocol %s current-state production disabled; cursor not initialized", protocolID)
		}
	}
	return nil
}

// reprobeProtocolCursors re-checks the ingest_store rows for protocols whose history or
// current-state cursor was missing at the last snapshot/re-probe, promoting missing ->
// existing when a protocol-setup/migrate run has initialized the row since (so production
// starts without a restart). Cursors already known to exist are never re-checked here — a row
// vanishing after having existed is the genuine incident casProtocolCursor's error path
// handles. Runs outside any transaction, on the oldestLedgerSyncInterval cadence; a DB error is
// logged and skipped (best-effort, like the oldest-ledger metric sync it runs alongside), not
// fatal — the next cadence tick tries again.
func (m *ingestService) reprobeProtocolCursors(ctx context.Context) {
	var keys []string
	for protocolID := range m.protocolProcessors {
		if !m.protocolCursors.historyExists[protocolID] {
			keys = append(keys, utils.ProtocolHistoryCursorName(protocolID))
		}
		if !m.protocolCursors.currentStateExists[protocolID] {
			keys = append(keys, utils.ProtocolCurrentStateCursorName(protocolID))
		}
	}
	if len(keys) == 0 {
		return
	}
	existing, err := m.models.IngestStore.GetMany(ctx, keys)
	if err != nil {
		log.Ctx(ctx).Warnf("re-probing protocol cursors: %v", err)
		return
	}
	for protocolID := range m.protocolProcessors {
		if !m.protocolCursors.historyExists[protocolID] {
			if _, ok := existing[utils.ProtocolHistoryCursorName(protocolID)]; ok {
				m.protocolCursors.historyExists[protocolID] = true
				log.Ctx(ctx).Infof("protocol %s history cursor initialized; production enabled", protocolID)
			}
		}
		if !m.protocolCursors.currentStateExists[protocolID] {
			if _, ok := existing[utils.ProtocolCurrentStateCursorName(protocolID)]; ok {
				m.protocolCursors.currentStateExists[protocolID] = true
				log.Ctx(ctx).Infof("protocol %s current-state cursor initialized; production enabled", protocolID)
			}
		}
	}
}

// casProtocolCursor performs the authoritative compare-and-swap on a protocol cursor.
// Callers only invoke this for a cursor m.protocolCursors marks as existing (see the gating in
// persistLedgerData) — a cursor known not yet initialized skips this call entirely, so
// ErrCASCursorMissing here always means a row that existed has since vanished: a genuine
// incident (dropped row, bad restore), not the operationally normal not-yet-initialized case.
// That is surfaced as an error like any other so the transaction aborts and live ingestion
// retries and eventually exits, rather than silently losing protocol state. A value mismatch
// (another process already owns this ledger — the normal, harmless CAS-lost-the-race case)
// still returns (false, nil).
func (m *ingestService) casProtocolCursor(ctx context.Context, dbTx pgx.Tx, cursorName, expected, next string) (bool, error) {
	swapped, err := m.models.IngestStore.CompareAndSwap(ctx, dbTx, cursorName, expected, next)
	if err != nil {
		return false, fmt.Errorf("comparing and swapping protocol cursor %s: %w", cursorName, err)
	}
	return swapped, nil
}

// distinctEventContractIDs returns the deduplicated raw 32-byte contract IDs that
// emitted events this ledger, ready to pass straight to a bytea[] query. Events with
// a nil ContractId are skipped. It is protocol-agnostic — membership is resolved
// downstream against protocol_contracts.
func distinctEventContractIDs(events map[indexer.ContractEventKey][]xdr.ContractEvent) [][]byte {
	ids := set.NewSet[xdr.ContractId]()
	for _, evs := range events {
		for _, ev := range evs {
			if ev.ContractId != nil {
				ids.Add(*ev.ContractId)
			}
		}
	}
	// Index into the slice rather than ranging by value so each [:] references a
	// distinct backing array (a ranged loop variable would alias the last element).
	slice := ids.ToSlice()
	raw := make([][]byte, len(slice))
	for i := range slice {
		raw[i] = slice[i][:]
	}
	return raw
}

// getEffectiveProtocolContracts overlays this-ledger buffered contracts onto the
// committed contracts resolved for this protocol. committed holds the protocol's
// contracts among those that emitted events this ledger. bufferedContracts holds
// contracts deployed or upgraded this ledger (keyed by hex contract id); classification
// maps this-ledger wasm hashes to their protocol. A contract whose binding changed this
// ledger is dropped from committed and re-added only if its new classification still
// matches the protocol.
func getEffectiveProtocolContracts(
	protocolID string,
	committed []data.ProtocolContracts,
	bufferedContracts map[string]data.ProtocolContracts,
	classification map[types.HashBytea]string,
) []data.ProtocolContracts {
	if len(bufferedContracts) == 0 {
		return committed
	}

	out := make([]data.ProtocolContracts, 0, len(committed)+len(bufferedContracts))
	for _, contract := range committed {
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
	return out
}

// ingestProcessedDataWithRetry wraps persistLedgerData with retry logic.
// plan was computed once by the caller before this loop started and is reused
// verbatim across every attempt, so a retried attempt never re-issues the
// classification RPC calls plan already resolved.
func (m *ingestService) ingestProcessedDataWithRetry(
	ctx context.Context,
	currentLedger uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	buffer *indexer.IndexerBuffer,
) (int, int, error) {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		numTxs, numOps, err := m.persistLedgerData(ctx, currentLedger, &ledgerMeta, plan, buffer, data.LatestLedgerCursorName)
		if err == nil {
			return numTxs, numOps, nil
		}
		lastErr = err
		if isPermanentPersistError(err) {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("db_persist").Inc()
			return 0, 0, fmt.Errorf("ingesting processed data for ledger %d failed with a permanent error: %w", currentLedger, err)
		}
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

// isPermanentPersistError classifies a persistLedgerData failure using its PostgreSQL SQLSTATE,
// when available, via errors.As through the wrap chain. SQLSTATE class 22 (data exception), 23
// (integrity constraint violation), and 42 (syntax or access-rule violation, e.g. an undefined
// column after schema drift) can never succeed by retrying the same statement, so they are
// permanent. Class 40 (40001 serialization_failure, 40P01 deadlock_detected), class 08
// (connection exception), and 57P0x (57P01 admin_shutdown, 57P02 crash_shutdown, 57P03
// cannot_connect_now — all raised during a CNPG failover) are transient and fall through to the
// default retry behavior, same as any other SQLSTATE or a non-PgError (e.g. a context deadline).
// ErrCursorGuardFailed (see IngestStoreModel.UpdateGuarded) is also permanent: it means another
// writer already owns this ledger's cursor range, so retrying the same write cannot succeed.
// ErrCASCursorMissing (see IngestStoreModel.CompareAndSwap) is likewise permanent: the cursor row
// this ledger's protocol CAS targets is gone, and no retry of the same transaction can recreate
// it — failing fast surfaces the incident instead of burning the whole retry ladder.
func isPermanentPersistError(err error) bool {
	if errors.Is(err, data.ErrCursorGuardFailed) || errors.Is(err, data.ErrCASCursorMissing) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23", "42":
		return true
	default:
		return false
	}
}

// prepareClassificationPlan runs Phase A of protocol classification for this
// ledger's buffered raw WASMs and contracts: pure signature matching plus any
// RPC-sourced enrichment prefetch (e.g. SEP-41 token metadata),
// entirely before any database transaction opens. Callers must compute this
// once per ledger and reuse the same plan across retry attempts (like
// buffer already is) — recomputing it would re-issue RPC calls
// on every retry. ApplyClassificationPlan (called from inside
// persistLedgerData's transaction) finishes the job with DB-only writes.
//
// The known-classification lookup is a non-transactional pool read:
// staleness is harmless (this-ledger uploads resolve from the buffer; prior
// rows are immutable once classified), and reading before any transaction
// opens means it never contends with the CAS/cursor row locks
// persistLedgerData holds later. Because it runs outside the persist retry
// ladder, it is wrapped in its own bounded backoff so a transient DB blip
// (e.g. a CNPG failover) does not exit live ingestion.
func (m *ingestService) prepareClassificationPlan(
	ctx context.Context,
	bufferedWasms map[string]data.ProtocolWasms,
	bufferedBytecodes map[string][]byte,
	bufferedContracts map[string]data.ProtocolContracts,
) (*ClassificationPlan, error) {
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

	contractSlice := make([]data.ProtocolContracts, 0, len(bufferedContracts))
	for _, c := range bufferedContracts {
		contractSlice = append(contractSlice, c)
	}

	// Each buffered contract carries the wasm hash its instance points at. That
	// hash may name a wasm uploaded in an earlier ledger (Soroban contract-code
	// and contract-instance entries are independent), so its bytecode is not in
	// this ledger's buffer — resolve those from the verdict already stored in
	// protocol_wasms. Hashes uploaded this ledger (thisBatch) are skipped here
	// because PrepareClassification classifies them from their buffered bytecode below.
	knownHashes := make([]types.HashBytea, 0, len(contractSlice))
	for _, c := range contractSlice {
		if _, inBatch := thisBatch[c.WasmHash]; inBatch {
			continue
		}
		knownHashes = append(knownHashes, c.WasmHash)
	}
	known, err := utils.RetryWithBackoff(ctx, maxClassificationReadRetries, maxRetryBackoff,
		func(ctx context.Context) (map[types.HashBytea]string, error) {
			return m.models.ProtocolWasms.GetClassifiedByHashes(ctx, m.models.DB, knownHashes)
		},
		func(attempt int, retryErr error, backoff time.Duration) {
			m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("classification_read").Inc()
			log.Ctx(ctx).Warnf("Error resolving known protocol classifications (attempt %d/%d): %v, retrying in %v...",
				attempt+1, maxClassificationReadRetries, retryErr, backoff)
		},
		isPermanentPersistError,
	)
	if err != nil {
		return nil, fmt.Errorf("resolving known protocol classifications: %w", err)
	}

	if len(m.protocolValidators) == 0 {
		plan := &ClassificationPlan{Matches: make(map[types.HashBytea]string, len(known))}
		for hash, pid := range known {
			plan.Matches[hash] = pid
		}
		return plan, nil
	}

	plan, err := PrepareClassification(
		ctx, m.wasmSpecExtractor, m.protocolValidators,
		bytecodesByHash, contractSlice, m.rpcService, known,
		m.appMetrics.Ingestion.WasmClassificationFailuresTotal,
	)
	if err != nil {
		return nil, fmt.Errorf("preparing classification: %w", err)
	}
	return plan, nil
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
