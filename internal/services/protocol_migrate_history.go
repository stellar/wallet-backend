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
)

const (
	// convergencePollTimeout is the timeout for polling for new ledgers at the tip.
	convergencePollTimeout = 5 * time.Second
)

// protocolTracker holds per-protocol state for the ledger-first migration loop.
type protocolTracker struct {
	protocolID  string
	cursorName  string
	cursorValue uint32
	processor   ProtocolProcessor
	handedOff   bool
}

// ProtocolMigrateHistoryService backfills protocol state changes for historical ledgers.
type ProtocolMigrateHistoryService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

var _ ProtocolMigrateHistoryService = (*protocolMigrateHistoryService)(nil)

type protocolMigrateHistoryService struct {
	db                     db.ConnectionPool
	ledgerBackend          ledgerbackend.LedgerBackend
	protocolsModel         data.ProtocolsModelInterface
	protocolContractsModel data.ProtocolContractsModelInterface
	ingestStore            *data.IngestStoreModel
	networkPassphrase      string
	processors             map[string]ProtocolProcessor
}

// ProtocolMigrateHistoryConfig holds the configuration for creating a protocolMigrateHistoryService.
type ProtocolMigrateHistoryConfig struct {
	DB                     db.ConnectionPool
	LedgerBackend          ledgerbackend.LedgerBackend
	ProtocolsModel         data.ProtocolsModelInterface
	ProtocolContractsModel data.ProtocolContractsModelInterface
	IngestStore            *data.IngestStoreModel
	NetworkPassphrase      string
	Processors             []ProtocolProcessor
}

// NewProtocolMigrateHistoryService creates a new protocolMigrateHistoryService from the given config.
func NewProtocolMigrateHistoryService(cfg ProtocolMigrateHistoryConfig) (*protocolMigrateHistoryService, error) {
	ppMap, err := buildProtocolProcessorMap(cfg.Processors)
	if err != nil {
		return nil, err
	}

	return &protocolMigrateHistoryService{
		db:                     cfg.DB,
		ledgerBackend:          cfg.LedgerBackend,
		protocolsModel:         cfg.ProtocolsModel,
		protocolContractsModel: cfg.ProtocolContractsModel,
		ingestStore:            cfg.IngestStore,
		networkPassphrase:      cfg.NetworkPassphrase,
		processors:             ppMap,
	}, nil
}

// Run performs history migration for the given protocol IDs.
func (s *protocolMigrateHistoryService) Run(ctx context.Context, protocolIDs []string) error {
	// Phase 1: Validate
	activeProtocolIDs, err := s.validate(ctx, protocolIDs)
	if err != nil {
		return fmt.Errorf("validating protocols: %w", err)
	}

	if len(activeProtocolIDs) == 0 {
		log.Ctx(ctx).Info("All protocols already completed history migration, nothing to do")
		return nil
	}

	if err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.protocolsModel.UpdateHistoryMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusInProgress)
	}); err != nil {
		return fmt.Errorf("setting history migration status to in_progress: %w", err)
	}

	// Phase 2: Process each protocol
	if err := s.processAllProtocols(ctx, activeProtocolIDs); err != nil {
		// Best-effort set status to failed
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if txErr := db.RunInPgxTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
			return s.protocolsModel.UpdateHistoryMigrationStatus(cleanupCtx, dbTx, activeProtocolIDs, data.StatusFailed)
		}); txErr != nil {
			log.Ctx(ctx).Errorf("error setting history migration status to failed: %v", txErr)
		}
		return fmt.Errorf("processing protocols: %w", err)
	}

	// Phase 3: Set status to success
	if err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.protocolsModel.UpdateHistoryMigrationStatus(ctx, dbTx, activeProtocolIDs, data.StatusSuccess)
	}); err != nil {
		return fmt.Errorf("setting history migration status to success: %w", err)
	}

	log.Ctx(ctx).Infof("History migration completed successfully for protocols: %v", activeProtocolIDs)
	return nil
}

// validate checks that all protocol IDs are valid and ready for history migration.
// Returns the list of protocol IDs that need processing (excludes already-success ones).
func (s *protocolMigrateHistoryService) validate(ctx context.Context, protocolIDs []string) ([]string, error) {
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
		if p.HistoryMigrationStatus == data.StatusSuccess {
			log.Ctx(ctx).Infof("Protocol %q history migration already completed, skipping", pid)
			continue
		}
		active = append(active, pid)
	}

	return active, nil
}

// processAllProtocols runs history migration for all protocols using ledger-first iteration.
// Each ledger is fetched once and processed by all eligible protocols, avoiding redundant RPC calls.
func (s *protocolMigrateHistoryService) processAllProtocols(ctx context.Context, protocolIDs []string) error {
	// Read oldest_ingest_ledger
	oldestLedger, err := s.ingestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("reading oldest ingest ledger: %w", err)
	}
	if oldestLedger == 0 {
		return fmt.Errorf("ingestion has not started yet (oldest_ingest_ledger is 0)")
	}

	// Initialize trackers: read/initialize cursor for each protocol
	trackers := make([]*protocolTracker, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		cursorName := protocolHistoryCursorName(pid)
		cursorValue, readErr := s.ingestStore.Get(ctx, cursorName)
		if readErr != nil {
			return fmt.Errorf("reading history cursor for %s: %w", pid, readErr)
		}

		if cursorValue == 0 {
			initValue := oldestLedger - 1
			if initErr := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
				return s.ingestStore.Update(ctx, dbTx, cursorName, initValue)
			}); initErr != nil {
				return fmt.Errorf("initializing history cursor for %s: %w", pid, initErr)
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
		contracts, err := s.protocolContractsModel.GetByProtocolID(ctx, t.protocolID)
		if err != nil {
			return fmt.Errorf("loading contracts for %s: %w", t.protocolID, err)
		}
		contractsByProtocol[t.protocolID] = contracts
	}

	for {
		if allHandedOff(trackers) {
			return nil
		}

		latestLedger, err := s.ingestStore.Get(ctx, data.LatestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("reading latest ingest ledger: %w", err)
		}

		// Find minimum cursor among non-handed-off trackers
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

		startLedger := minCursor + 1
		if startLedger > latestLedger {
			log.Ctx(ctx).Infof("All protocols at or past tip %d, migration complete", latestLedger)
			return nil
		}

		log.Ctx(ctx).Infof("Processing ledgers %d to %d for %d protocol(s)", startLedger, latestLedger, len(protocolIDs))

		if err := s.ledgerBackend.PrepareRange(ctx, ledgerbackend.BoundedRange(startLedger, latestLedger)); err != nil {
			return fmt.Errorf("preparing ledger range [%d, %d]: %w", startLedger, latestLedger, err)
		}

		for seq := startLedger; seq <= latestLedger; seq++ {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled: %w", ctx.Err())
			default:
			}

			// Skip if no tracker needs this ledger
			needsFetch := false
			for _, t := range trackers {
				if !t.handedOff && t.cursorValue < seq {
					needsFetch = true
					break
				}
			}
			if !needsFetch {
				continue
			}

			// Fetch ledger ONCE for all protocols
			ledgerMeta, fetchErr := getLedgerWithRetry(ctx, s.ledgerBackend, seq)
			if fetchErr != nil {
				return fmt.Errorf("fetching ledger %d: %w", seq, fetchErr)
			}

			// Process each eligible tracker
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
				if err := t.processor.ProcessLedger(ctx, input); err != nil {
					return fmt.Errorf("processing ledger %d for protocol %s: %w", seq, t.protocolID, err)
				}

				// CAS + persist in a transaction
				expected := strconv.FormatUint(uint64(seq-1), 10)
				next := strconv.FormatUint(uint64(seq), 10)

				var swapped bool
				if err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
					var casErr error
					swapped, casErr = s.ingestStore.CompareAndSwap(ctx, dbTx, t.cursorName, expected, next)
					if casErr != nil {
						return fmt.Errorf("CAS history cursor for %s: %w", t.protocolID, casErr)
					}
					if swapped {
						return t.processor.PersistHistory(ctx, dbTx)
					}
					return nil
				}); err != nil {
					return fmt.Errorf("persisting ledger %d for protocol %s: %w", seq, t.protocolID, err)
				}

				if !swapped {
					log.Ctx(ctx).Infof("Protocol %s: CAS failed at ledger %d, handoff to live ingestion detected", t.protocolID, seq)
					t.handedOff = true
				} else {
					t.cursorValue = seq
				}
			}

			if allHandedOff(trackers) {
				return nil
			}

			if seq%100 == 0 {
				log.Ctx(ctx).Infof("Progress: processed ledger %d / %d", seq, latestLedger)
			}
		}

		if allHandedOff(trackers) {
			return nil
		}

		// Check if tip has advanced
		newLatest, err := s.ingestStore.Get(ctx, data.LatestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("re-reading latest ingest ledger: %w", err)
		}
		if newLatest > latestLedger {
			continue
		}

		// At tip — poll briefly for convergence
		pollCtx, cancel := context.WithTimeout(ctx, convergencePollTimeout)
		if err := s.ledgerBackend.PrepareRange(pollCtx, ledgerbackend.UnboundedRange(latestLedger+1)); err != nil {
			cancel()
			log.Ctx(ctx).Infof("Converged at ledger %d", latestLedger)
			return nil
		}

		_, getLedgerErr := s.ledgerBackend.GetLedger(pollCtx, latestLedger+1)
		cancel()
		if getLedgerErr != nil {
			log.Ctx(ctx).Infof("Converged at ledger %d", latestLedger)
			return nil
		}

		// New ledger available, loop again
	}
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
