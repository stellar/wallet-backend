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
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

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

// contractDataMemo lazily extracts ContractData changes from a ledger's
// already-materialized transactions (from the processLedger staging pass) and
// memoizes the result. Extraction is pure over (transactions, ledgerSeq), so
// one memo shared across persistLedgerDataWithRetry's attempts means the
// extraction walk runs at most once per ledger — never inside a retried
// attempt's open transaction — and only when a CAS-winning processor
// requires it, so a protocol still backfilling costs nothing extra.
type contractDataMemo struct {
	transactions []ingest.LedgerTransaction
	ledgerSeq    uint32
	changes      map[string][]ingest.Change
	extracted    bool
}

func newContractDataMemo(transactions []ingest.LedgerTransaction, ledgerSeq uint32) *contractDataMemo {
	return &contractDataMemo{transactions: transactions, ledgerSeq: ledgerSeq}
}

// get returns the memoized extraction, running it on first use. The result is
// always non-nil, including over zero transactions, because a
// RequiresContractData processor must receive a ContractDataChanges map it can
// range over unconditionally.
func (c *contractDataMemo) get() (map[string][]ingest.Change, error) {
	if !c.extracted {
		changes, err := indexer.ExtractContractDataChangesFromTransactions(c.transactions, c.ledgerSeq)
		if err != nil {
			return nil, fmt.Errorf("extracting contract data changes for ledger %d: %w", c.ledgerSeq, err)
		}
		c.changes = changes
		c.extracted = true
	}
	return c.changes, nil
}

// ErrPartialPersist marks a persist failure that occurred after the first
// sibling commit succeeded: some of the ledger's tables are durable and the
// rest are not, the transaction set can no longer roll back atomically, and no
// in-process retry can fix it (COPY has no ON CONFLICT, so re-running the
// ledger would collide on primary keys). It is classified permanent so the
// process exits; startup reconciliation (DeleteRowsAboveLedger) removes the
// orphaned rows above the committed cursor before ingestion resumes.
var ErrPartialPersist = errors.New("ledger persist partially committed")

// persistLedgerData persists one ledger. The three bulk COPY families —
// transactions(+accounts), operations(+accounts), state_changes — stream
// concurrently on sibling connections, each in its own transaction, while the
// coordinating transaction stages everything else (assets, contracts,
// classification, protocol state, token changes, cursor). All the slow work
// happens uncommitted and invisible; only after every stream and the
// coordinator succeed do the commits fire, siblings first and the
// coordinating transaction strictly last. The cursor it carries is the
// authority: the only crash state this ordering can produce is orphaned bulk
// rows above the committed cursor, which DeleteRowsAboveLedger removes at
// startup. A failure before the first commit rolls everything back and is
// retryable as before; a failure after it wraps ErrPartialPersist and is
// fatal.
//
// plan is this ledger's classification plan, computed by
// prepareClassificationPlan before any transaction opens (RPC calls already
// resolved); pass the same plan across persistLedgerDataWithRetry's retry
// attempts so a retry never re-issues RPC calls. plan may be nil when there
// was nothing to classify this ledger. contractData carries the ledger's
// ContractData extraction memo; like plan, the same memo is shared across
// retry attempts so a retry never re-runs the extraction walk.
func (m *ingestService) persistLedgerData(
	ctx context.Context,
	ledgerSeq uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	contractData *contractDataMemo,
	buffer *indexer.IndexerBuffer,
) error {
	// The sibling transactions and the coordinating transaction are all opened
	// up front on this goroutine, so ownership at the commit barrier below is
	// deterministic; the deferred rollbacks tolerate ErrTxClosed and so are
	// no-ops for whatever committed.
	coordTx, err := m.models.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning coordinating transaction for ledger %d: %w", ledgerSeq, err)
	}
	defer func() {
		if rbErr := coordTx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Ctx(ctx).Warnf("rolling back coordinating transaction for ledger %d: %v", ledgerSeq, rbErr)
		}
	}()

	siblings := []struct {
		name string
		run  func(ctx context.Context, dbTx pgx.Tx) error
	}{
		{"transactions", func(ctx context.Context, dbTx pgx.Tx) error {
			return m.insertTransactions(ctx, dbTx, buffer.GetTransactions(), buffer.GetTransactionsParticipants())
		}},
		{"operations", func(ctx context.Context, dbTx pgx.Tx) error {
			return m.insertOperations(ctx, dbTx, buffer.GetOperations(), buffer.GetOperationsParticipants())
		}},
		{"state_changes", func(ctx context.Context, dbTx pgx.Tx) error {
			return m.insertStateChanges(ctx, dbTx, buffer.GetStateChanges())
		}},
	}
	siblingTxs := make([]pgx.Tx, len(siblings))
	for i, s := range siblings {
		conn, acquireErr := m.models.DB.Acquire(ctx)
		if acquireErr != nil {
			return fmt.Errorf("acquiring %s connection for ledger %d: %w", s.name, ledgerSeq, acquireErr)
		}
		defer conn.Release()
		tx, beginErr := conn.Begin(ctx)
		if beginErr != nil {
			return fmt.Errorf("beginning %s transaction for ledger %d: %w", s.name, ledgerSeq, beginErr)
		}
		siblingTxs[i] = tx
		defer func(name string, tx pgx.Tx) {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				log.Ctx(ctx).Warnf("rolling back %s transaction for ledger %d: %v", name, ledgerSeq, rbErr)
			}
		}(s.name, tx)
	}

	// Stream the COPY families and stage the coordinated writes concurrently.
	// The table sets are disjoint (no FKs among them), and the goroutines only
	// read the quiescent buffer, so the four transactions never contend.
	g, gctx := errgroup.WithContext(ctx)
	for i, s := range siblings {
		g.Go(func() error {
			if runErr := s.run(gctx, siblingTxs[i]); runErr != nil {
				return fmt.Errorf("streaming %s for ledger %d: %w", s.name, ledgerSeq, runErr)
			}
			return nil
		})
	}
	g.Go(func() error {
		return m.stageCoordinatedWrites(gctx, coordTx, ledgerSeq, ledgerMeta, plan, contractData, buffer)
	})
	if err = g.Wait(); err != nil {
		// Nothing has committed: the deferred rollbacks discard all four
		// transactions and the ledger is cleanly retryable.
		return fmt.Errorf("persisting ledger data for ledger %d: %w", ledgerSeq, err)
	}

	// Commit barrier. A failed FIRST commit still leaves nothing durable
	// (its transaction aborts, the others roll back), so it stays retryable;
	// once any commit has succeeded the set can no longer roll back
	// atomically and every subsequent failure is ErrPartialPersist. The one
	// indeterminate case — a first-commit error whose commit actually
	// reached the server — self-heals: the retry collides on primary keys,
	// which is permanent, and startup reconciliation repairs after restart.
	for i, s := range siblings {
		if commitErr := siblingTxs[i].Commit(ctx); commitErr != nil {
			if i > 0 {
				return fmt.Errorf("committing %s for ledger %d: %w: %w", s.name, ledgerSeq, ErrPartialPersist, commitErr)
			}
			return fmt.Errorf("committing %s for ledger %d: %w", s.name, ledgerSeq, commitErr)
		}
	}
	// The coordinating transaction commits strictly last: it carries the
	// cursor, so its commit is the point at which the ledger exists.
	if commitErr := coordTx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("committing coordinating transaction for ledger %d: %w: %w", ledgerSeq, ErrPartialPersist, commitErr)
	}
	return nil
}

// stageCoordinatedWrites runs every per-ledger write except the three bulk
// COPY families on the coordinating transaction: trustline assets, SAC
// contract tokens, protocol classification and wasm/contract rows,
// CAS-gated protocol state, token changes, and finally the guarded cursor.
func (m *ingestService) stageCoordinatedWrites(
	ctx context.Context,
	dbTx pgx.Tx,
	ledgerSeq uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	contractData *contractDataMemo,
	buffer *indexer.IndexerBuffer,
) error {
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

	// 3. Apply protocol classification (black-box per protocol). plan was
	// computed by prepareClassificationPlan before this transaction opened,
	// so any RPC calls (e.g. SEP-41 metadata) already happened;
	// ApplyClassificationPlan only performs each validator's DB writes
	// here, atomically with the classification verdict and wasm/contract
	// rows below. Wasm rows are persisted next, then live protocol
	// processors stage ledger state from the classification result, and
	// the generic protocol_contracts rows are persisted after them so a
	// processor's name-enriched row lands first (the generic insert's
	// COALESCE preserves it).
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

	// Persist this ledger's wasm rows BEFORE processors run: a processor
	// enriching protocol_contracts (e.g. contract names decoded from
	// instance storage) inserts rows that are FK-filtered against
	// protocol_wasms, so a contract deployed in the same ledger as its
	// wasm upload would otherwise be silently dropped.
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

	// 4. Per-protocol CAS-gated state production. The compare-and-swap on each
	// protocol cursor is the authoritative gate — exactly one of live ingestion or
	// protocol-migrate wins a given ledger. Staging (ProcessLedger) and persistence
	// run only for cursors that win the swap, so a protocol still backfilling (its
	// cursor behind tip) costs a single CAS and a continue.
	if len(m.protocolProcessors) > 0 {
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

		var contractDataChanges map[string][]ingest.Change

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
				// process already owns this ledger: nothing to stage. Skipping is
				// lossless: the migrate engine folds a ledger only after this
				// cursor's transaction commits (its frontier gate), so a ledger
				// lost here is folded later by a winner that sees every
				// classification this transaction commits — including contracts
				// classified this very ledger.
				continue
			}

			committed := committedByProtocol[protocolID]
			if processor.RequiresContractData() {
				var cdErr error
				contractDataChanges, cdErr = contractData.get()
				if cdErr != nil {
					return cdErr
				}

				// ContractData-driven processors need the protocol's FULL committed
				// membership, not just this ledger's event emitters: entries can
				// change on a contract that emitted no event this ledger, and event
				// decoding may disambiguate against tracked contracts that appear
				// only in another contract's topics. Protocols requiring contract
				// data have bounded membership, so the per-ledger query stays cheap.
				var fullErr error
				committed, fullErr = m.models.ProtocolContracts.GetByProtocolID(ctx, dbTx, protocolID)
				if fullErr != nil {
					return fmt.Errorf("resolving full protocol contracts for ledger %d protocol %s: %w", ledgerSeq, protocolID, fullErr)
				}
			}

			contracts := getEffectiveProtocolContracts(protocolID, committed, bufferedContracts, classification)
			input := ProtocolProcessorInput{
				LedgerSequence:      ledgerSeq,
				LedgerCloseTime:     ledgerCloseTime,
				ContractEvents:      contractEvents,
				ProtocolContracts:   contracts,
				StagingMode:         StagingModeBoth,
				ContractDataChanges: contractDataChanges,
			}
			// Reset before staging so a retried transaction (persistLedgerDataWithRetry)
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

	if len(contractSlice) > 0 {
		if txErr = m.models.ProtocolContracts.BatchInsert(ctx, dbTx, contractSlice); txErr != nil {
			return fmt.Errorf("inserting protocol contracts for ledger %d: %w", ledgerSeq, txErr)
		}
	}

	// 5. Process token changes (trustline add/remove/update, native balance, SAC balance)
	if txErr = m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx,
		buffer.GetTrustlineChanges(),
		buffer.GetAccountChanges(),
		buffer.GetSACBalanceChanges(),
		buffer.GetLiquidityPoolShareChanges(),
		buffer.GetLiquidityPoolChanges(),
	); txErr != nil {
		return fmt.Errorf("processing token changes for ledger %d: %w", ledgerSeq, txErr)
	}

	// 6. Advance the latest-ledger cursor. The update is guarded: a session that
	// silently lost its advisory lock (server-side failover, see startLiveIngestion's
	// checkLockSession) must not blindly overwrite a value a second instance already
	// advanced, or the cursor could regress.
	if txErr = m.models.IngestStore.UpdateGuarded(ctx, dbTx, data.LatestLedgerCursorName, ledgerSeq); txErr != nil {
		return fmt.Errorf("updating cursor for ledger %d: %w", ledgerSeq, txErr)
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

	// Background tasks gated on the advisory lock: an instance that failed to
	// acquire it must never run them, or a crash-looping/standby pod would
	// duplicate their RPC load and writes.
	for _, task := range m.postLockTasks {
		go task(ctx)
	}

	// Get latest ingested ledger to determine DB state
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	startLedger := latestIngestedLedger + 1
	if latestIngestedLedger == 0 && m.archive == nil {
		// No history archive (streaming-loadtest backend): there is no
		// checkpoint state to bootstrap from, so start from ledger 1 with an
		// empty database — balance state accumulates from the ledger stream.
		// The cursor row must exist before the first guarded cursor update.
		startLedger = 1
		err = db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			if txErr := m.initializeCursors(ctx, dbTx, startLedger); txErr != nil {
				return txErr
			}
			// Also seed every registered protocol's cursors. They are normally
			// created by the protocol-migrate CLI, which needs a replayable
			// ledger source this deployment does not have; without the rows the
			// per-ledger compare-and-swap gate silently skips all protocol
			// state production. startLedger-1 is exactly the value the first
			// ledger's CAS expects.
			for protocolID := range m.protocolProcessors {
				if txErr := m.models.IngestStore.Update(ctx, dbTx, utils.ProtocolHistoryCursorName(protocolID), startLedger-1); txErr != nil {
					return fmt.Errorf("initializing %s history cursor: %w", protocolID, txErr)
				}
				if txErr := m.models.IngestStore.Update(ctx, dbTx, utils.ProtocolCurrentStateCursorName(protocolID), startLedger-1); txErr != nil {
					return fmt.Errorf("initializing %s current-state cursor: %w", protocolID, txErr)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("initializing cursors without archive bootstrap: %w", err)
		}
		// The cursor snapshot above ran before these rows existed; refresh it
		// so protocol production is enabled from the first ledger.
		if err = m.snapshotProtocolCursors(ctx); err != nil {
			return fmt.Errorf("re-snapshotting protocol cursors after seeding: %w", err)
		}
		m.appMetrics.Ingestion.LatestLedger.Set(float64(startLedger))
		m.appMetrics.Ingestion.OldestLedger.Set(float64(startLedger))
	} else if latestIngestedLedger == 0 {
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
		// Remove any bulk rows a crashed run left above the cursor before that
		// ledger is re-ingested: persistLedgerData commits the sibling COPY
		// transactions before the coordinating transaction that carries the
		// cursor, so a crash between those commits orphans (at most) the single
		// ledger past the cursor. Fatal on failure — ingesting over the orphans
		// would collide on the bulk tables' primary keys anyway.
		if err := m.models.IngestStore.DeleteRowsAboveLedger(ctx, latestIngestedLedger); err != nil {
			return fmt.Errorf("reconciling bulk rows above cursor ledger %d: %w", latestIngestedLedger, err)
		}

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

// fetchedLedger is the fetch→process handoff: one ledger's raw close meta.
type fetchedLedger struct {
	seq  uint32
	meta xdr.LedgerCloseMeta
}

// processedLedger is the process→persist handoff. transactions are the
// materialized transactions from the staging pass, reused by the persist
// stage for ContractData extraction. processDuration rides along so the
// per-ledger Duration metric can sum the ledger's stage times instead of
// counting the time it sat queued between stages.
type processedLedger struct {
	seq             uint32
	meta            xdr.LedgerCloseMeta
	transactions    []ingest.LedgerTransaction
	buffer          *indexer.IndexerBuffer
	processDuration time.Duration
}

// ingestLiveLedgers runs live ingestion as a three-stage pipeline — fetch ‖
// process ‖ persist — over consecutive ledgers, connected by depth-1
// channels: while ledger N persists, N+1 processes and N+2 is fetched. The
// ledger time is therefore the slowest stage, not the sum of stages. Persist
// is strictly sequential in ledger order (the guarded cursor and the
// per-protocol CAS chain both advance N-1 → N), and any stage error cancels
// the whole pipeline and returns: the process exits and re-acquires the
// advisory lock cleanly on restart, resuming from the cursor.
//
// checkLockSession is probed before every persist to verify the
// advisory-lock-holding Postgres session is still alive (see
// startLiveIngestion): a CNPG failover can kill that session server-side
// without this process observing the disconnect, silently releasing the lock
// while writes keep flowing through other pool connections. Probing on the
// persist stage — the only stage that writes — is what makes that safe.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32, checkLockSession func(ctx context.Context) error) error {
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", startLedger)

	// Refresh the lag gauge off the pipeline goroutines. GetLatestLedgerSequence contends on the
	// datastore buffer's internal lock, which a download worker can hold while blocked on a full
	// queue; calling it on the goroutine that drains that queue would deadlock. A dedicated
	// goroutine keeps the consumer draining, so the lock is always released promptly.
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

	g, gctx := errgroup.WithContext(ctx)
	fetched := make(chan fetchedLedger, 1)
	processed := make(chan processedLedger, 1)

	// Two buffers rotate between the process and persist stages: process fills
	// one while persist drains the other, and reuse keeps the asset-parse memo
	// warm and the maps' backing arrays allocated across ledgers. Capacity 2
	// means handing a buffer back never blocks.
	freeBuffers := make(chan *indexer.IndexerBuffer, 2)
	freeBuffers <- indexer.NewIndexerBuffer()
	freeBuffers <- indexer.NewIndexerBuffer()

	g.Go(func() error { return m.fetchLedgers(gctx, startLedger, fetched) })
	g.Go(func() error { return m.processFetchedLedgers(gctx, fetched, freeBuffers, processed) })
	g.Go(func() error {
		return m.persistProcessedLedgers(gctx, processed, freeBuffers, checkLockSession, &latestIngested)
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("live ingestion pipeline: %w", err)
	}
	return nil
}

// fetchLedgers is the pipeline's fetch stage: it pulls consecutive ledgers
// from the backend (with the transient-fetch retry ladder) and hands them to
// the process stage.
func (m *ingestService) fetchLedgers(ctx context.Context, startLedger uint32, fetched chan<- fetchedLedger) error {
	defer close(fetched)
	for seq := startLedger; ; seq++ {
		fetchStart := time.Now()
		ledgerMeta, err := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff,
			func(ctx context.Context) (xdr.LedgerCloseMeta, error) {
				return m.ledgerBackend.GetLedger(ctx, seq)
			},
			func(attempt int, err error, backoff time.Duration) {
				m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("ledger_fetch").Inc()
				log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
					seq, attempt+1, maxLedgerFetchRetries, err, backoff)
			},
			m.isPermanentFetchError,
		)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("fetching ledger %d: %w", seq, err)
		}
		m.appMetrics.Ingestion.LedgerFetchDuration.Observe(time.Since(fetchStart).Seconds())

		select {
		case fetched <- fetchedLedger{seq: seq, meta: ledgerMeta}:
		case <-ctx.Done():
			return fmt.Errorf("pipeline cancelled: %w", ctx.Err())
		}
	}
}

// processFetchedLedgers is the pipeline's process stage: it stages each
// fetched ledger into a rotating buffer and hands the result to the persist
// stage.
func (m *ingestService) processFetchedLedgers(ctx context.Context, fetched <-chan fetchedLedger, freeBuffers <-chan *indexer.IndexerBuffer, processed chan<- processedLedger) error {
	defer close(processed)
	for {
		var fl fetchedLedger
		select {
		case f, ok := <-fetched:
			if !ok {
				return nil
			}
			fl = f
		case <-ctx.Done():
			return fmt.Errorf("pipeline cancelled: %w", ctx.Err())
		}

		var buffer *indexer.IndexerBuffer
		select {
		case buffer = <-freeBuffers:
		case <-ctx.Done():
			return fmt.Errorf("pipeline cancelled: %w", ctx.Err())
		}
		// Clear at take, not at hand-back: a buffer re-enters freeBuffers only
		// once the persist stage is completely done with it, so clearing here
		// can never tear a persist still reading the maps the buffer getters
		// alias.
		buffer.Clear()

		processStart := time.Now()
		transactions, err := m.processLedger(ctx, fl.meta, buffer)
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("processing ledger %d: %w", fl.seq, err)
		}
		processDuration := time.Since(processStart)
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("process_ledger").Observe(processDuration.Seconds())

		select {
		case processed <- processedLedger{seq: fl.seq, meta: fl.meta, transactions: transactions, buffer: buffer, processDuration: processDuration}:
		case <-ctx.Done():
			return fmt.Errorf("pipeline cancelled: %w", ctx.Err())
		}
	}
}

// persistProcessedLedgers is the pipeline's persist stage and the only stage
// that writes to the database. It runs strictly sequentially in ledger order,
// probes the advisory-lock session before each ledger, and returns each
// buffer to the rotation once its ledger is fully persisted.
func (m *ingestService) persistProcessedLedgers(ctx context.Context, processed <-chan processedLedger, freeBuffers chan<- *indexer.IndexerBuffer, checkLockSession func(ctx context.Context) error, latestIngested *atomic.Uint32) error {
	for {
		var pl processedLedger
		select {
		case p, ok := <-processed:
			if !ok {
				return nil
			}
			pl = p
		case <-ctx.Done():
			return fmt.Errorf("pipeline cancelled: %w", ctx.Err())
		}

		if probeErr := checkLockSession(ctx); probeErr != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("advisory lock session is no longer alive, the lock may have been lost: %w", probeErr)
		}

		// Classification runs on this stage, not the process stage: its
		// known-hash lookup is a non-transactional pool read of protocol_wasms
		// whose correctness depends on the previous ledger's persist having
		// committed — a contract deployed in ledger N+1 pointing at a wasm
		// uploaded in N must see N's row. RPC prefetch still happens before
		// any transaction opens, and the plan is reused verbatim across every
		// retry attempt below.
		classifyStart := time.Now()
		plan, err := m.prepareClassificationPlan(ctx, pl.buffer.GetProtocolWasms(), pl.buffer.GetProtocolWasmBytecodes(), pl.buffer.GetProtocolContracts())
		if err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("preparing classification plan for ledger %d: %w", pl.seq, err)
		}
		classifyDuration := time.Since(classifyStart)
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("prepare_classification").Observe(classifyDuration.Seconds())

		// All DB operations in a single atomic transaction with retry
		dbStart := time.Now()
		if err := m.persistLedgerDataWithRetry(ctx, pl.seq, pl.meta, plan, pl.transactions, pl.buffer); err != nil {
			m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ingest_live").Inc()
			return fmt.Errorf("persisting ledger %d: %w", pl.seq, err)
		}
		persistDuration := time.Since(dbStart)
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("insert_into_db").Observe(persistDuration.Seconds())

		ledgerDuration := pl.processDuration + classifyDuration + persistDuration
		m.appMetrics.Ingestion.Duration.Observe(ledgerDuration.Seconds())
		m.appMetrics.Ingestion.TransactionsTotal.Add(float64(pl.buffer.GetNumberOfTransactions()))
		m.appMetrics.Ingestion.OperationsTotal.Add(float64(pl.buffer.GetNumberOfOperations()))
		m.appMetrics.Ingestion.LedgersProcessed.Add(float64(1))
		m.appMetrics.Ingestion.LatestLedger.Set(float64(pl.seq))

		// Publish the just-ingested ledger for the lag updater goroutine.
		latestIngested.Store(pl.seq)

		// Periodically sync oldest ledger metric from DB (picks up changes from backfill jobs),
		// and re-probe protocol cursors that were missing at the last snapshot/re-probe (picks
		// up a protocol-setup/migrate run that has initialized one since — see
		// reprobeProtocolCursors).
		if pl.seq%oldestLedgerSyncInterval == 0 {
			if oldest, syncErr := m.models.IngestStore.Get(ctx, m.oldestLedgerCursorName); syncErr == nil {
				m.appMetrics.Ingestion.OldestLedger.Set(float64(oldest))
			}
			m.reprobeProtocolCursors(ctx)
		}

		log.Ctx(ctx).Infof("Ingested ledger %d in %.4fs", pl.seq, ledgerDuration.Seconds())

		freeBuffers <- pl.buffer
	}
}

// protocolCursorSnapshot records, per protocol, whether its history and
// current-state ingest_store cursor rows exist. It only ever promotes an
// entry from missing to existing (see reprobeProtocolCursors) — a row
// vanishing after having existed is the genuine incident casProtocolCursor's
// error path handles, not something this snapshot demotes on its own. It is
// read and mutated only from the pipeline's persist stage goroutine
// (persistProcessedLedgers) — including retried persistLedgerData attempts,
// which run synchronously on it — so it needs no locking.
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

// persistLedgerDataWithRetry wraps persistLedgerData with retry logic.
// plan was computed once by the caller before this call and is reused
// verbatim across every attempt, so a retried attempt never re-issues the
// classification RPC calls plan already resolved. The ContractData extraction
// memo is built here, outside the retried function, so it too is shared across
// attempts and a retry never re-runs the extraction walk over the ledger's
// transactions.
func (m *ingestService) persistLedgerDataWithRetry(
	ctx context.Context,
	currentLedger uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	transactions []ingest.LedgerTransaction,
	buffer *indexer.IndexerBuffer,
) error {
	contractData := newContractDataMemo(transactions, currentLedger)
	_, err := utils.RetryWithBackoff(ctx, maxIngestProcessedDataRetries, maxIngestProcessedDataRetryBackoff,
		func(ctx context.Context) (struct{}, error) {
			return struct{}{}, m.persistLedgerData(ctx, currentLedger, ledgerMeta, plan, contractData, buffer)
		},
		func(attempt int, err error, backoff time.Duration) {
			m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("db_persist").Inc()
			log.Ctx(ctx).Warnf("Error ingesting data for ledger %d (attempt %d/%d): %v, retrying in %v...",
				currentLedger, attempt+1, maxIngestProcessedDataRetries, err, backoff)
		},
		isPermanentPersistError,
	)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, utils.ErrRetriesExhausted):
		m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("db_persist").Inc()
		m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("db_persist").Inc()
	case isPermanentPersistError(err):
		m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("db_persist").Inc()
		return fmt.Errorf("ingesting processed data for ledger %d failed with a permanent error: %w", currentLedger, err)
	}
	// The remaining exit is context cancellation — a shutdown, not an ingestion fault, so it
	// counts against neither counter.
	return fmt.Errorf("ingesting processed data for ledger %d: %w", currentLedger, err)
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
// ErrPartialPersist (see persistLedgerData) is permanent by definition: part of the ledger is
// already durable, so re-running it collides on primary keys; the process must exit and let
// startup reconciliation repair.
func isPermanentPersistError(err error) bool {
	if errors.Is(err, data.ErrCursorGuardFailed) || errors.Is(err, data.ErrCASCursorMissing) || errors.Is(err, ErrPartialPersist) {
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
