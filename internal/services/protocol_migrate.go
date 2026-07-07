package services

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// progressLogInterval is the fixed ledger cadence for migration progress logs. It is deliberately
// decoupled from the commit window (--window-size) so tuning the commit size does not change log
// cadence. At migration throughput (hundreds of l/s) this yields a progress line every few seconds.
const progressLogInterval uint32 = 1000

// tipRefreshInterval is how often the background goroutine polls getHealth to refresh the
// target_tip gauge. Loose freshness suffices — the gauge feeds only the %-remaining dashboard,
// and the chain tip advances ~1 ledger every few seconds.
const tipRefreshInterval = 15 * time.Second

// protocolTracker holds per-protocol state for the ledger-first migration loop.
type protocolTracker struct {
	protocolID  string
	cursorName  string
	cursorValue uint32
	processor   ProtocolProcessor
	handedOff   bool
	// pending counts folded-but-uncommitted ledgers; window = [cursorValue+1, cursorValue+pending].
	pending uint32
}

// migrationStrategy holds the injection points that differ between
// history and current-state migration. Plain struct with function fields
// rather than an interface, to avoid over-abstraction.
type migrationStrategy struct {
	// Label is a human-readable name for log/error messages (e.g., "history", "current state").
	Label string

	// Mode selects which staged sets processors build for this strategy.
	Mode StagingMode

	// UpdateMigrationStatus updates the migration status for the given protocol IDs.
	UpdateMigrationStatus func(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error

	// MigrationStatusField extracts the relevant migration status from a protocol record.
	MigrationStatusField func(p *data.Protocols) string

	// CursorName returns the ingest_store key for a protocol's migration cursor.
	CursorName func(protocolID string) string

	// Persist writes the strategy-specific data within a CAS transaction.
	Persist func(ctx context.Context, dbTx pgx.Tx, processor ProtocolProcessor) error

	// ResolveStartLedger returns the first ledger sequence to consider for migration.
	ResolveStartLedger func(ctx context.Context) (uint32, error)
}

// protocolMigrateEngine is the shared migration engine parameterized by a strategy.
type protocolMigrateEngine struct {
	db                     *pgxpool.Pool
	ledgerBackend          ledgerbackend.LedgerBackend
	protocolsModel         data.ProtocolsModelInterface
	protocolContractsModel data.ProtocolContractsModelInterface
	ingestStore            *data.IngestStoreModel
	networkPassphrase      string
	processors             map[string]ProtocolProcessor
	strategy               migrationStrategy
	// windowSize is the number of ledgers coalesced into one commit. 0 means 1 (commit every ledger).
	windowSize uint32
	// metrics records migration progress/throughput/outcome. Always non-nil:
	// the service constructors default it to a fresh registry when unset.
	metrics *metrics.MigrationMetrics
	// tipProvider returns the real chain tip (RPC getHealth) for the %-remaining
	// gauge, or nil when no RPC URL is configured.
	tipProvider func() (uint32, error)
}

// Run performs migration for the given protocol IDs using the configured strategy.
func (s *protocolMigrateEngine) Run(ctx context.Context, protocolIDs []string) error {
	// Phase 1: Validate
	activeProtocolIDs, err := s.validate(ctx, protocolIDs)
	if err != nil {
		return fmt.Errorf("validating protocols: %w", err)
	}

	if len(activeProtocolIDs) == 0 {
		log.Ctx(ctx).Infof("All protocols already completed %s migration, nothing to do", s.strategy.Label)
		return nil
	}

	if txErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.strategy.UpdateMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusInProgress)
	}); txErr != nil {
		return fmt.Errorf("setting %s migration status to in_progress: %w", s.strategy.Label, txErr)
	}
	for _, pid := range activeProtocolIDs {
		s.metrics.Status.WithLabelValues(pid).Set(metrics.MigrationStatusInProgress)
	}

	// Phase 2: Process each protocol
	handedOffIDs, err := s.processAllProtocols(ctx, activeProtocolIDs)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Mark handed-off protocols as success — live ingestion owns them now
		if len(handedOffIDs) > 0 {
			if txErr := db.RunInTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
				return s.strategy.UpdateMigrationStatus(cleanupCtx, dbTx, handedOffIDs, data.StatusSuccess)
			}); txErr != nil {
				log.Ctx(ctx).Errorf("error setting handed-off protocols to success: %v", txErr)
			}
		}

		// Mark only non-handed-off protocols as failed
		failedIDs := subtract(activeProtocolIDs, handedOffIDs)
		if len(failedIDs) > 0 {
			if txErr := db.RunInTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
				return s.strategy.UpdateMigrationStatus(cleanupCtx, dbTx, failedIDs, data.StatusFailed)
			}); txErr != nil {
				log.Ctx(ctx).Errorf("error setting %s migration status to failed: %v", s.strategy.Label, txErr)
			}
		}
		for _, pid := range handedOffIDs {
			s.metrics.Status.WithLabelValues(pid).Set(metrics.MigrationStatusSuccess)
		}
		for _, pid := range failedIDs {
			s.metrics.Status.WithLabelValues(pid).Set(metrics.MigrationStatusFailed)
		}

		return fmt.Errorf("processing protocols: %w", err)
	}

	// Phase 3: Set status to success
	if txErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.strategy.UpdateMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusSuccess)
	}); txErr != nil {
		return fmt.Errorf("setting %s migration status to success: %w", s.strategy.Label, txErr)
	}
	for _, pid := range activeProtocolIDs {
		s.metrics.Status.WithLabelValues(pid).Set(metrics.MigrationStatusSuccess)
	}

	log.Ctx(ctx).Infof("%s migration completed successfully for protocols: %v", s.strategy.Label, activeProtocolIDs)
	return nil
}

// validate checks that all protocol IDs are valid and ready for migration.
// Returns the list of protocol IDs that need processing (excludes already-success ones).
func (s *protocolMigrateEngine) validate(ctx context.Context, protocolIDs []string) ([]string, error) {
	// De-duplicate protocolIDs, preserving order.
	seen := make(map[string]struct{}, len(protocolIDs))
	unique := make([]string, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		if _, dup := seen[pid]; !dup {
			seen[pid] = struct{}{}
			unique = append(unique, pid)
		}
	}
	protocolIDs = unique

	// Check each protocol has a registered processor
	for _, pid := range protocolIDs {
		if _, ok := s.processors[pid]; !ok {
			return nil, fmt.Errorf("no processor registered for protocol %q", pid)
		}
	}

	// Verify all protocols exist in the DB and classification is complete
	protocols, err := s.protocolsModel.GetByIDs(ctx, protocolIDs)
	if err != nil {
		return nil, fmt.Errorf("querying protocols: %w", err)
	}

	foundSet := make(map[string]*data.Protocols, len(protocols))
	for i := range protocols {
		foundSet[protocols[i].ID] = &protocols[i]
	}

	var missing []string
	for _, pid := range protocolIDs {
		if _, ok := foundSet[pid]; !ok {
			missing = append(missing, pid)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("protocols not found in DB: %v", missing)
	}

	// Check classification status and filter out already-completed migrations
	var active []string
	for _, pid := range protocolIDs {
		p := foundSet[pid]
		if p.ClassificationStatus != data.StatusSuccess {
			return nil, fmt.Errorf("protocol %q classification not complete (status: %s)", pid, p.ClassificationStatus)
		}
		if s.strategy.MigrationStatusField(p) == data.StatusSuccess {
			log.Ctx(ctx).Infof("Protocol %q %s migration already completed, skipping", pid, s.strategy.Label)
			continue
		}
		active = append(active, pid)
	}

	return active, nil
}

// stageTimers accumulates per-stage wall-clock across the whole migration run so the loop can
// report where time goes: waiting for a ledger, extracting its contract events, folding it into
// the processors, or flushing a window to the DB. It is never reset, so breakdown() reports the
// cumulative share — stable line-to-line and fairly amortizing costs (like flush) that occur far
// less often than the progress-log interval.
type stageTimers struct {
	fetch   time.Duration // GetLedger wait (consumer stall, not download time); ≈0 while the datastore prefetch keeps up, rises only when it can't
	extract time.Duration // ExtractContractEventsForLedger
	process time.Duration // ProcessLedger across all trackers
	flush   time.Duration // flushWindow (CAS + Persist), including tip flushes
}

func (t stageTimers) total() time.Duration { return t.fetch + t.extract + t.process + t.flush }

// breakdown renders each stage's share of the cumulative staged total as a percentage.
func (t stageTimers) breakdown() string {
	total := t.total()
	if total == 0 {
		return "fetch-wait=0% extract=0% process=0% flush=0%"
	}
	pct := func(d time.Duration) float64 { return 100 * float64(d) / float64(total) }
	return fmt.Sprintf("fetch-wait=%.0f%% extract=%.0f%% process=%.0f%% flush=%.0f%%",
		pct(t.fetch), pct(t.extract), pct(t.process), pct(t.flush))
}

// processAllProtocols runs migration for all protocols using ledger-first iteration.
// Each ledger is fetched once and processed by all eligible protocols, avoiding redundant RPC calls.
func (s *protocolMigrateEngine) processAllProtocols(ctx context.Context, protocolIDs []string) ([]string, error) {
	startLedgerBase, err := s.strategy.ResolveStartLedger(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize trackers: read/initialize cursor for each protocol
	trackers := make([]*protocolTracker, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		cursorName := s.strategy.CursorName(pid)
		cursorValue, readErr := s.ingestStore.Get(ctx, cursorName)
		if readErr != nil {
			return nil, fmt.Errorf("reading %s cursor for %s: %w", s.strategy.Label, pid, readErr)
		}

		if cursorValue == 0 {
			initValue := startLedgerBase - 1
			if initErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
				return s.ingestStore.Update(ctx, dbTx, cursorName, initValue)
			}); initErr != nil {
				return nil, fmt.Errorf("initializing %s cursor for %s: %w", s.strategy.Label, pid, initErr)
			}
			cursorValue = initValue
		}

		trackers = append(trackers, &protocolTracker{
			protocolID:  pid,
			cursorName:  cursorName,
			cursorValue: cursorValue,
			processor:   s.processors[pid],
		})
	}

	// Load contracts once — all relevant contracts are in the DB before migration starts
	// (validate() requires ClassificationStatus == StatusSuccess).
	contractsByProtocol := make(map[string][]data.ProtocolContracts, len(trackers))
	for _, t := range trackers {
		contracts, loadErr := s.protocolContractsModel.GetByProtocolID(ctx, t.protocolID)
		if loadErr != nil {
			return nil, fmt.Errorf("loading contracts for %s: %w", t.protocolID, loadErr)
		}
		contractsByProtocol[t.protocolID] = contracts
	}

	for _, t := range trackers {
		s.metrics.StartLedger.WithLabelValues(t.protocolID).Set(float64(t.cursorValue))
	}

	windowSize := s.windowSize
	if windowSize == 0 {
		windowSize = 1
	}

	startLedger := minCursor(trackers) + 1
	log.Ctx(ctx).Infof("Processing ledgers starting at %d (unbounded) for %d protocol(s), window size %d",
		startLedger, len(protocolIDs), windowSize)

	prepareFn := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.ledgerBackend.PrepareRange(ctx, ledgerbackend.UnboundedRange(startLedger))
	}
	if _, prepErr := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff, prepareFn,
		func(attempt int, err error, backoff time.Duration) {
			log.Ctx(ctx).Warnf("Error preparing unbounded range from %d (attempt %d/%d): %v, retrying in %v...",
				startLedger, attempt+1, maxLedgerFetchRetries, err, backoff)
		},
	); prepErr != nil {
		return handedOffProtocolIDs(trackers), fmt.Errorf("preparing unbounded range from %d: %w", startLedger, prepErr)
	}

	// Refresh the target_tip gauge off the hot path: a background goroutine polls RPC health so
	// the migration loop never blocks on getHealth. The gauge feeds only the %-remaining dashboard
	// (nothing in the loop reads it), so the goroutine is its sole writer and needs no locking; it
	// stops when this function returns.
	if s.tipProvider != nil {
		tipCtx, cancelTip := context.WithCancel(ctx)
		defer cancelTip()
		go s.refreshTargetTip(tipCtx)
	}

	var (
		cachedTip       uint32
		timers          stageTimers
		intervalStart   = time.Now()
		intervalLedgers uint32
	)
	for seq := startLedger; ; seq++ {
		if err := ctx.Err(); err != nil {
			return handedOffProtocolIDs(trackers), fmt.Errorf("context cancelled: %w", err)
		}
		if allHandedOff(trackers) {
			return handedOffProtocolIDs(trackers), nil
		}

		// Before fetching seq, flush any open windows once we reach the live tip so the
		// next (blocking) GetLedger never strands the cursor behind the frontier.
		var flushErr error
		tipFlushStart := time.Now()
		cachedTip, flushErr = s.flushWindowsAtTip(ctx, trackers, seq, cachedTip)
		timers.flush += time.Since(tipFlushStart)
		if flushErr != nil {
			return handedOffProtocolIDs(trackers), flushErr
		}
		if allHandedOff(trackers) {
			return handedOffProtocolIDs(trackers), nil
		}

		fetchStart := time.Now()
		ledgerMeta, fetchErr := s.ledgerBackend.GetLedger(ctx, seq)
		fetchDur := time.Since(fetchStart)
		timers.fetch += fetchDur
		s.metrics.PhaseDuration.WithLabelValues("fetch").Observe(fetchDur.Seconds())
		if fetchErr != nil {
			return handedOffProtocolIDs(trackers), fmt.Errorf("fetching ledger %d: %w", seq, fetchErr)
		}

		// Extract contract events once per ledger; all trackers below share the
		// same map. This is the migration-side analogue of the live-ingest path
		// where buffer.GetContractEvents() is computed once per ledger.
		extractStart := time.Now()
		ledgerEvents, eventsErr := indexer.ExtractContractEventsForLedger(ledgerMeta)
		extractDur := time.Since(extractStart)
		timers.extract += extractDur
		s.metrics.PhaseDuration.WithLabelValues("extract").Observe(extractDur.Seconds())
		if eventsErr != nil {
			return handedOffProtocolIDs(trackers), fmt.Errorf("extracting contract events for ledger %d: %w", seq, eventsErr)
		}
		ledgerCloseTime := ledgerMeta.LedgerCloseTime()

		for _, t := range trackers {
			if t.handedOff || t.cursorValue+t.pending >= seq {
				continue
			}
			input := ProtocolProcessorInput{
				LedgerSequence:    seq,
				LedgerCloseTime:   ledgerCloseTime,
				ContractEvents:    ledgerEvents,
				ProtocolContracts: contractsByProtocol[t.protocolID],
				StagingMode:       s.strategy.Mode,
			}
			processStart := time.Now()
			processErr := t.processor.ProcessLedger(ctx, input)
			processDur := time.Since(processStart)
			timers.process += processDur
			s.metrics.PhaseDuration.WithLabelValues("process").Observe(processDur.Seconds())
			if processErr != nil {
				return handedOffProtocolIDs(trackers), fmt.Errorf("processing ledger %d for protocol %s: %w", seq, t.protocolID, processErr)
			}
			t.pending++
			if t.pending >= windowSize {
				flushStart := time.Now()
				flushWinErr := s.flushWindow(ctx, t)
				timers.flush += time.Since(flushStart)
				if flushWinErr != nil {
					return handedOffProtocolIDs(trackers), flushWinErr
				}
			}
		}

		s.metrics.CurrentLedger.Set(float64(seq))
		s.metrics.LedgersProcessed.Inc()
		intervalLedgers++
		// Gate on ledgers processed, not seq%interval: an absolute-seq gate emits nothing for a
		// run whose span never crosses a multiple of the interval (e.g. a short near-tip
		// current-state run that hands off before reaching one).
		if intervalLedgers >= progressLogInterval {
			wall := time.Since(intervalStart)
			var lps float64
			if wall > 0 {
				lps = float64(intervalLedgers) / wall.Seconds()
			}
			log.Ctx(ctx).Infof(
				"Progress: processed ledger %d | %d ledgers in %s (%.1f l/s) | %s",
				seq, intervalLedgers, wall.Round(time.Millisecond), lps,
				timers.breakdown(),
			)
			intervalLedgers = 0
			intervalStart = time.Now()
		}
	}
}

// refreshTargetTip polls the RPC chain tip on tipRefreshInterval and updates the target_tip
// gauge, keeping the getHealth call off the migration hot path. It is the gauge's sole writer
// (the loop never reads it), so it needs no synchronization. It checks ctx before each poll so
// cancellation stops further RPC calls promptly, and returns when ctx is cancelled.
func (s *protocolMigrateEngine) refreshTargetTip(ctx context.Context) {
	ticker := time.NewTicker(tipRefreshInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if tip, err := s.tipProvider(); err == nil {
			s.metrics.TargetTip.Set(float64(tip))
		} else {
			log.Ctx(ctx).Debugf("migration tip provider error (skipping target_tip update): %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// flushWindowsAtTip flushes every tracker's open window once seq reaches the live tip,
// so a coalescing window never straddles the migration<->live frontier.
//
// Below the tip it is a no-op: GetLedger(seq) returns immediately and an open window is
// harmless, so the bulk backfill keeps committing windowSize ledgers per transaction. At
// the tip, GetLedger(seq) blocks until the ledger closes — and a window held open across
// that block pins the cursor behind the frontier, so live ingestion's CAS(tip-1 -> tip)
// can never match and the handoff (hence migration termination) can never happen. Flushing
// first advances each cursor to the last closed ledger, arming the handoff and shrinking
// the window to 1 at the tip.
//
// Termination depends on live ingestion running concurrently: migration hands off only by
// losing a CAS at the tip, so if it always wins (e.g. live ingestion is down) it keeps
// producing rather than handing off — the correct degraded-mode behavior, not a hang.
//
// The tip is cached and only re-read once seq catches up to it, sparing the bulk a
// GetLatestLedgerSequence call per ledger; the tip is monotonic, so a stale value at worst
// costs one extra refresh. The updated cachedTip is returned for the next iteration. A
// flush can hand off the last active tracker, so callers must re-check allHandedOff after.
func (s *protocolMigrateEngine) flushWindowsAtTip(ctx context.Context, trackers []*protocolTracker, seq, cachedTip uint32) (uint32, error) {
	if seq <= cachedTip {
		return cachedTip, nil
	}
	if tip, tipErr := s.ledgerBackend.GetLatestLedgerSequence(ctx); tipErr == nil {
		cachedTip = tip
	}
	if seq <= cachedTip {
		return cachedTip, nil
	}
	for _, t := range trackers {
		if err := s.flushWindow(ctx, t); err != nil {
			return cachedTip, err
		}
	}
	return cachedTip, nil
}

// flushWindow commits a tracker's open window [cursorValue+1, cursorValue+pending] in a
// single transaction: CAS(winStart-1 -> winEnd) then merged Persist. A failed CAS means
// live ingestion took winStart, so the whole window is discarded and the tracker hands off.
func (s *protocolMigrateEngine) flushWindow(ctx context.Context, t *protocolTracker) error {
	if t.pending == 0 {
		return nil
	}
	flushStart := time.Now()
	winEnd := t.cursorValue + t.pending
	expected := strconv.FormatUint(uint64(t.cursorValue), 10) // winStart - 1
	next := strconv.FormatUint(uint64(winEnd), 10)

	var swapped bool
	if txErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// Disable synchronous commit for this transaction only — safe for migration
		// since data is re-ingestable via the CAS cursor on crash.
		if _, execErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); execErr != nil {
			return fmt.Errorf("setting synchronous_commit=off: %w", execErr)
		}
		var casErr error
		swapped, casErr = s.ingestStore.CompareAndSwap(ctx, dbTx, t.cursorName, expected, next)
		if casErr != nil {
			return fmt.Errorf("CAS %s cursor for %s: %w", s.strategy.Label, t.protocolID, casErr)
		}
		if swapped {
			return s.strategy.Persist(ctx, dbTx, t.processor)
		}
		return nil
	}); txErr != nil {
		return fmt.Errorf("persisting window [%d,%d] for protocol %s: %w", t.cursorValue+1, winEnd, t.protocolID, txErr)
	}

	t.processor.Reset()
	if swapped {
		t.cursorValue = winEnd
		s.metrics.Cursor.WithLabelValues(t.protocolID).Set(float64(winEnd))
	} else {
		log.Ctx(ctx).Infof("Protocol %s: CAS failed at window [%d,%d], handoff to live ingestion detected", t.protocolID, t.cursorValue+1, winEnd)
		t.handedOff = true
		s.metrics.Handoffs.WithLabelValues(t.protocolID).Inc()
	}
	t.pending = 0
	s.metrics.PhaseDuration.WithLabelValues("flush").Observe(time.Since(flushStart).Seconds())
	return nil
}

// minCursor returns the smallest cursorValue across trackers. Called once during
// init, where trackers is non-empty (Run returns early on an empty active set)
// and no tracker has been handed off yet.
func minCursor(trackers []*protocolTracker) uint32 {
	m := trackers[0].cursorValue
	for _, t := range trackers[1:] {
		if t.cursorValue < m {
			m = t.cursorValue
		}
	}
	return m
}

// allHandedOff returns true if every tracker has been handed off to live ingestion.
func allHandedOff(trackers []*protocolTracker) bool {
	for _, t := range trackers {
		if !t.handedOff {
			return false
		}
	}
	return true
}

// handedOffProtocolIDs returns the IDs of trackers that have been handed off to live ingestion.
func handedOffProtocolIDs(trackers []*protocolTracker) []string {
	var ids []string
	for _, t := range trackers {
		if t.handedOff {
			ids = append(ids, t.protocolID)
		}
	}
	return ids
}

// subtract returns all elements in `all` that are not in `remove`.
func subtract(all, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, id := range remove {
		removeSet[id] = struct{}{}
	}
	var result []string
	for _, id := range all {
		if _, ok := removeSet[id]; !ok {
			result = append(result, id)
		}
	}
	return result
}
