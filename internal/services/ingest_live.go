package services

import (
	"context"
	"encoding/hex"
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
// one memo shared across ingestProcessedDataWithRetry's attempts means the
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

// get returns the memoized extraction, running it on first use. A nil
// receiver yields an empty result — callers with no materialized
// transactions pass a nil memo, and a RequiresContractData processor must
// still receive a non-nil (empty) ContractDataChanges map, same as an
// extraction over zero transactions produces.
func (c *contractDataMemo) get() (map[string][]ingest.Change, error) {
	if c == nil {
		return map[string][]ingest.Change{}, nil
	}
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

// persistLedgerData persists processed ledger data to the database in a single
// atomic transaction. It handles: trustline assets, contract tokens, filtered
// data insertion, token changes, and cursor update. plan is this ledger's
// classification plan, computed by prepareClassificationPlan before any
// transaction opens (RPC calls already resolved); pass the same plan across
// ingestProcessedDataWithRetry's retry attempts so a retry never re-issues
// RPC calls. plan may be nil when there was nothing to classify this ledger.
// contractData carries the ledger's ContractData extraction memo; like plan,
// the same memo is shared across retry attempts so a retry never re-runs the
// extraction walk. It is nil exactly when ledgerMeta is.
func (m *ingestService) persistLedgerData(
	ctx context.Context,
	ledgerSeq uint32,
	ledgerMeta *xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	contractData *contractDataMemo,
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
				// A lost swap on an existing cursor is normally nothing to stage:
				// behind tip (value mismatch), or another process already owns this
				// ledger. The exception — a contract classified this ledger whose
				// deploy-ledger state the winner could not have staged — is handled
				// by repairClassificationGap after the swapped halves persist.
				lostHistory := m.protocolCursors.historyExists[protocolID] && !historySwapped
				lostCurrentState := m.protocolCursors.currentStateExists[protocolID] && !currentStateSwapped

				if historySwapped || currentStateSwapped {
					if stageErr := m.stageAndPersistProtocolLedger(ctx, dbTx, protocolID, processor, ledgerSeq, ledgerCloseTime,
						contractEvents, committedByProtocol[protocolID], bufferedContracts, classification, contractData,
						historySwapped, currentStateSwapped); stageErr != nil {
						return stageErr
					}
				}

				if lostHistory || lostCurrentState {
					if repairErr := m.repairClassificationGap(ctx, dbTx, protocolID, processor, ledgerSeq, ledgerCloseTime,
						contractEvents, bufferedContracts, classification, contractData,
						lostHistory, lostCurrentState); repairErr != nil {
						return repairErr
					}
				}
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

// stageAndPersistProtocolLedger runs the CAS-winning path for one protocol at
// one ledger: resolve membership (full committed membership for
// ContractData-requiring processors, event-derived otherwise), overlay this
// ledger's buffered classifications, stage via ProcessLedger, and persist the
// halves whose cursor swap this process won.
func (m *ingestService) stageAndPersistProtocolLedger(
	ctx context.Context,
	dbTx pgx.Tx,
	protocolID string,
	processor ProtocolProcessor,
	ledgerSeq uint32,
	ledgerCloseTime int64,
	contractEvents map[indexer.ContractEventKey][]xdr.ContractEvent,
	committed []data.ProtocolContracts,
	bufferedContracts map[string]data.ProtocolContracts,
	classification map[types.HashBytea]string,
	contractData *contractDataMemo,
	historySwapped, currentStateSwapped bool,
) error {
	var contractDataChanges map[string][]ingest.Change
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
	return nil
}

// repairClassificationGap persists, for the cursor halves this process lost,
// this ledger's state for contracts whose classification into protocolID
// commits only with the transaction in flight. A concurrent protocol-migrate
// engine snapshots membership from committed protocol_contracts rows, so when
// it wins this ledger's swap it has folded the ledger without those contracts
// — their state here (constructor ContractData writes, first events) would
// otherwise be persisted by nobody: the engine's next membership refresh sees
// them only from the following ledger on, and cursor-passed ledgers are never
// replayed.
//
// The repair runs only when the lost cursor's committed value is at or past
// this ledger — proof the winner folded it without these contracts. That value
// is reliably visible: a CompareAndSwap that lost to a winner already past
// this ledger blocked on the winner's row lock before re-evaluating, so the
// winner's commit precedes the GetInTx read. A value still behind this ledger
// means the protocol is catching up and the engine folds this ledger later,
// after a refresh that sees the rows this transaction commits — except when
// the engine is already mid-window across this ledger, an accepted residual
// gap that requires live ingestion to lag the engine by a full window with the
// classification landing mid-window (current-state then heals on the
// contract's next write; history rows for that window stay missing).
//
// Membership for the repair run is exactly the gap contracts, which assumes
// processor staging is additive across disjoint membership sets: ContractData
// staging is keyed by owning contract and event handling filters on the
// emitting contract, so the winner's rows and the repair's rows partition
// cleanly.
func (m *ingestService) repairClassificationGap(
	ctx context.Context,
	dbTx pgx.Tx,
	protocolID string,
	processor ProtocolProcessor,
	ledgerSeq uint32,
	ledgerCloseTime int64,
	contractEvents map[indexer.ContractEventKey][]xdr.ContractEvent,
	bufferedContracts map[string]data.ProtocolContracts,
	classification map[types.HashBytea]string,
	contractData *contractDataMemo,
	lostHistory, lostCurrentState bool,
) error {
	// Almost every ledger classifies nothing, making a lost swap the ordinary
	// behind-tip/handoff case; the checks below run only past this point.
	candidates := make([]data.ProtocolContracts, 0, len(bufferedContracts))
	for _, contract := range bufferedContracts {
		if classification[contract.WasmHash] == protocolID {
			candidates = append(candidates, contract)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	// Drop candidates with a committed row for this protocol: those were in the
	// winner's membership already (e.g. a re-upload of an already-classified
	// contract), so the winner staged their state for this ledger itself.
	rawIDs := make([][]byte, 0, len(candidates))
	for _, c := range candidates {
		raw, decErr := hex.DecodeString(string(c.ContractID))
		if decErr != nil {
			return fmt.Errorf("decoding contract id %q for protocol %s classification-gap check at ledger %d: %w", string(c.ContractID), protocolID, ledgerSeq, decErr)
		}
		rawIDs = append(rawIDs, raw)
	}
	committedRows, err := m.models.ProtocolContracts.BatchGetByContractIDs(ctx, rawIDs)
	if err != nil {
		return fmt.Errorf("resolving committed contracts for protocol %s classification-gap check at ledger %d: %w", protocolID, ledgerSeq, err)
	}
	alreadyTracked := make(map[string]struct{}, len(committedRows[protocolID]))
	for _, c := range committedRows[protocolID] {
		alreadyTracked[string(c.ContractID)] = struct{}{}
	}
	gap := candidates[:0]
	for _, c := range candidates {
		if _, tracked := alreadyTracked[string(c.ContractID)]; !tracked {
			gap = append(gap, c)
		}
	}
	if len(gap) == 0 {
		return nil
	}

	repairHistory, repairCurrentState := false, false
	if lostHistory {
		value, cursorErr := m.models.IngestStore.GetInTx(ctx, dbTx, utils.ProtocolHistoryCursorName(protocolID))
		if cursorErr != nil {
			return fmt.Errorf("reading history cursor for protocol %s classification-gap check at ledger %d: %w", protocolID, ledgerSeq, cursorErr)
		}
		repairHistory = value >= ledgerSeq
	}
	if lostCurrentState {
		value, cursorErr := m.models.IngestStore.GetInTx(ctx, dbTx, utils.ProtocolCurrentStateCursorName(protocolID))
		if cursorErr != nil {
			return fmt.Errorf("reading current-state cursor for protocol %s classification-gap check at ledger %d: %w", protocolID, ledgerSeq, cursorErr)
		}
		repairCurrentState = value >= ledgerSeq
	}
	if !repairHistory && !repairCurrentState {
		return nil
	}

	mode := StagingModeBoth
	switch {
	case repairHistory && !repairCurrentState:
		mode = StagingModeHistory
	case repairCurrentState && !repairHistory:
		mode = StagingModeCurrentState
	}

	var contractDataChanges map[string][]ingest.Change
	if processor.RequiresContractData() {
		contractDataChanges, err = contractData.get()
		if err != nil {
			return err
		}
	}

	// Reset discards any staging left from this ledger's swapped-half run; the
	// repair persists only rows scoped to the gap contracts.
	processor.Reset()
	input := ProtocolProcessorInput{
		LedgerSequence:      ledgerSeq,
		LedgerCloseTime:     ledgerCloseTime,
		ContractEvents:      contractEvents,
		ProtocolContracts:   gap,
		StagingMode:         mode,
		ContractDataChanges: contractDataChanges,
	}
	start := time.Now()
	if processErr := processor.ProcessLedger(ctx, input); processErr != nil {
		return fmt.Errorf("processing classification-gap repair at ledger %d for protocol %s: %w", ledgerSeq, protocolID, processErr)
	}
	m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "process_ledger").Observe(time.Since(start).Seconds())

	if repairHistory {
		persistStart := time.Now()
		if persistErr := processor.PersistHistory(ctx, dbTx); persistErr != nil {
			return fmt.Errorf("persisting classification-gap history for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
		}
		m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_history").Observe(time.Since(persistStart).Seconds())
	}
	if repairCurrentState {
		persistStart := time.Now()
		if persistErr := processor.PersistCurrentState(ctx, dbTx); persistErr != nil {
			return fmt.Errorf("persisting classification-gap current state for %s at ledger %d: %w", protocolID, ledgerSeq, persistErr)
		}
		m.appMetrics.Ingestion.ProtocolStateProcessingDuration.WithLabelValues(protocolID, "persist_current_state").Observe(time.Since(persistStart).Seconds())
	}
	log.Ctx(ctx).Infof("repaired classification gap for protocol %s at ledger %d: persisted %d newly classified contract(s) behind a lost cursor swap (history=%t, current_state=%t)",
		protocolID, ledgerSeq, len(gap), repairHistory, repairCurrentState)
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

	// One buffer serves every ledger, cleared at the top of each iteration below. Reusing it keeps the
	// asset-parse memo warm and the maps' backing arrays allocated across ledgers, the way backfill
	// reuses its batch buffer. Nothing retains the buffer past an iteration — the persist retries run
	// synchronously on this goroutine — so a single instance is safe.
	buffer := indexer.NewIndexerBuffer()

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
		// Clearing here rather than after the persist keeps the reset unconditional: processLedger
		// always starts from an empty buffer regardless of how the previous iteration ended.
		buffer.Clear()
		transactions, err := m.processLedger(ctx, ledgerMeta, buffer)
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
		numTransactionProcessed, numOperationProcessed, err := m.ingestProcessedDataWithRetry(ctx, currentLedger, ledgerMeta, plan, transactions, buffer)
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
// classification RPC calls plan already resolved. The ContractData extraction
// memo is likewise shared across attempts, so a retry never re-runs the
// extraction walk over the ledger's transactions.
func (m *ingestService) ingestProcessedDataWithRetry(
	ctx context.Context,
	currentLedger uint32,
	ledgerMeta xdr.LedgerCloseMeta,
	plan *ClassificationPlan,
	transactions []ingest.LedgerTransaction,
	buffer *indexer.IndexerBuffer,
) (int, int, error) {
	contractData := newContractDataMemo(transactions, currentLedger)
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		numTxs, numOps, err := m.persistLedgerData(ctx, currentLedger, &ledgerMeta, plan, contractData, buffer, data.LatestLedgerCursorName)
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
