package services

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/utils"
)

// protocolTracker holds per-protocol state for the ledger-first migration loop.
type protocolTracker struct {
	protocolID  string
	cursorName  string
	cursorValue uint32
	processor   ProtocolProcessor
	handedOff   bool
}

// migrationStrategy holds the injection points that differ between
// history and current-state migration. Plain struct with function fields
// rather than an interface, to avoid over-abstraction.
type migrationStrategy struct {
	// Label is a human-readable name for log/error messages (e.g., "history", "current state").
	Label string

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
	db                     db.ConnectionPool
	ledgerBackend          ledgerbackend.LedgerBackend
	protocolsModel         data.ProtocolsModelInterface
	protocolContractsModel data.ProtocolContractsModelInterface
	ingestStore            *data.IngestStoreModel
	networkPassphrase      string
	processors             map[string]ProtocolProcessor
	strategy               migrationStrategy
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

	if txErr := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.strategy.UpdateMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusInProgress)
	}); txErr != nil {
		return fmt.Errorf("setting %s migration status to in_progress: %w", s.strategy.Label, txErr)
	}

	// Phase 2: Process each protocol
	handedOffIDs, err := s.processAllProtocols(ctx, activeProtocolIDs)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Mark handed-off protocols as success — live ingestion owns them now
		if len(handedOffIDs) > 0 {
			if txErr := db.RunInPgxTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
				return s.strategy.UpdateMigrationStatus(cleanupCtx, dbTx, handedOffIDs, data.StatusSuccess)
			}); txErr != nil {
				log.Ctx(ctx).Errorf("error setting handed-off protocols to success: %v", txErr)
			}
		}

		// Mark only non-handed-off protocols as failed
		failedIDs := subtract(activeProtocolIDs, handedOffIDs)
		if len(failedIDs) > 0 {
			if txErr := db.RunInPgxTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
				return s.strategy.UpdateMigrationStatus(cleanupCtx, dbTx, failedIDs, data.StatusFailed)
			}); txErr != nil {
				log.Ctx(ctx).Errorf("error setting %s migration status to failed: %v", s.strategy.Label, txErr)
			}
		}

		return fmt.Errorf("processing protocols: %w", err)
	}

	// Phase 3: Set status to success
	if txErr := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.strategy.UpdateMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusSuccess)
	}); txErr != nil {
		return fmt.Errorf("setting %s migration status to success: %w", s.strategy.Label, txErr)
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
			if initErr := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
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

	startLedger := minNonHandedOffCursor(trackers) + 1

	log.Ctx(ctx).Infof("Processing ledgers starting at %d (unbounded) for %d protocol(s)", startLedger, len(protocolIDs))

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

	for seq := startLedger; ; seq++ {
		if err := ctx.Err(); err != nil {
			return handedOffProtocolIDs(trackers), fmt.Errorf("context cancelled: %w", err)
		}
		if allHandedOff(trackers) {
			return handedOffProtocolIDs(trackers), nil
		}

		// Skip if no non-handed-off tracker needs this ledger.
		if !anyTrackerNeedsLedger(trackers, seq) {
			continue
		}

		ledgerMeta, fetchErr := s.ledgerBackend.GetLedger(ctx, seq)
		if fetchErr != nil {
			return handedOffProtocolIDs(trackers), fmt.Errorf("fetching ledger %d: %w", seq, fetchErr)
		}

		for _, t := range trackers {
			if t.handedOff || t.cursorValue >= seq {
				continue
			}

			contracts := contractsByProtocol[t.protocolID]
			input := ProtocolProcessorInput{
				LedgerSequence:    seq,
				LedgerCloseMeta:   ledgerMeta,
				ProtocolContracts: contracts,
				NetworkPassphrase: s.networkPassphrase,
			}
			if processErr := t.processor.ProcessLedger(ctx, input); processErr != nil {
				return handedOffProtocolIDs(trackers), fmt.Errorf("processing ledger %d for protocol %s: %w", seq, t.protocolID, processErr)
			}

			// CAS + persist in a transaction
			expected := strconv.FormatUint(uint64(seq-1), 10)
			next := strconv.FormatUint(uint64(seq), 10)

			var swapped bool
			if txErr := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
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
				return handedOffProtocolIDs(trackers), fmt.Errorf("persisting ledger %d for protocol %s: %w", seq, t.protocolID, txErr)
			}

			if !swapped {
				log.Ctx(ctx).Infof("Protocol %s: CAS failed at ledger %d, handoff to live ingestion detected", t.protocolID, seq)
				t.handedOff = true
			} else {
				t.cursorValue = seq
			}
		}

		if seq%100 == 0 {
			log.Ctx(ctx).Infof("Progress: processed ledger %d", seq)
		}
	}
}

// minNonHandedOffCursor returns the smallest cursorValue among trackers that
// have not yet been handed off. If every tracker is handed off, it returns 0.
func minNonHandedOffCursor(trackers []*protocolTracker) uint32 {
	var minCursor uint32
	first := true
	for _, t := range trackers {
		if t.handedOff {
			continue
		}
		if first || t.cursorValue < minCursor {
			minCursor = t.cursorValue
			first = false
		}
	}
	return minCursor
}

// anyTrackerNeedsLedger reports whether at least one non-handed-off tracker
// still needs to process the given ledger sequence.
func anyTrackerNeedsLedger(trackers []*protocolTracker, seq uint32) bool {
	for _, t := range trackers {
		if !t.handedOff && t.cursorValue < seq {
			return true
		}
	}
	return false
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
