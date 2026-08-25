package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// historyRebuildDeleteSlice is how many ledgers each history-wipe DELETE
// covers, one transaction per slice.
//
// Why sliced: state_changes is compressed. Deleting from a compressed batch
// decompresses the whole batch (~1000 rows) inside the transaction, so one
// full-window DELETE would decompress unbounded data. Slicing bounds each
// statement to 10k ledgers' worth of work, regardless of chunk-interval
// tuning (batch min/max metadata prunes within chunks).
//
// Why the cap is lifted anyway: TimescaleDB's 100k-decompressed-tuples cap
// counts the whole ~1000-row batch even when we delete one row from it, so
// it trips based on data interleaving, not slice size. The slice bounds the
// real work; the cap only adds data-dependent failures.
//
// Why 10k: at default 1-day chunks (~17k mainnet ledgers) a slice stays
// chunk-scale, and a 90-day window wipes in ~156 statements. Order of
// magnitude is all that matters; not a flag on purpose.
//
// Crash mid-wipe is safe: the cursor reset already stopped live's writes,
// and rerunning the rebuild is idempotent.
const historyRebuildDeleteSlice uint32 = 10_000

// ProtocolHistoryRebuildService deletes a protocol's state-change rows and
// re-runs the history migration over the retained window.
//
// Same shape as the current-state rebuild: the wipe makes the protocol look
// never-migrated (cursor at the retention floor, status not_started, no
// rows), then the unmodified engine migrates it and hands off to live
// ingestion at the frontier. Live writes no history for the protocol between
// the cursor reset and the handoff.
type ProtocolHistoryRebuildService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

var _ ProtocolHistoryRebuildService = (*protocolHistoryRebuildService)(nil)

// ProtocolHistoryRebuildConfig holds the configuration for creating a protocolHistoryRebuildService.
type ProtocolHistoryRebuildConfig struct {
	DB                     *pgxpool.Pool
	LedgerBackend          ledgerbackend.LedgerBackend
	ProtocolsModel         data.ProtocolsModelInterface
	ProtocolContractsModel data.ProtocolContractsModelInterface
	IngestStore            *data.IngestStoreModel
	StateChanges           *data.StateChangeModel
	NetworkPassphrase      string
	Processors             []ProtocolProcessor
	WindowSize             uint32
	Metrics                *metrics.MigrationMetrics
	TipProvider            func() (uint32, error)
}

type protocolHistoryRebuildService struct {
	engine       protocolMigrateEngine
	stateChanges *data.StateChangeModel
}

// NewProtocolHistoryRebuildService creates a rebuild service from the same
// configuration as the plain history migration, plus the state-changes model
// for the wipe.
func NewProtocolHistoryRebuildService(cfg ProtocolHistoryRebuildConfig) (*protocolHistoryRebuildService, error) {
	migrate, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
		DB:                     cfg.DB,
		LedgerBackend:          cfg.LedgerBackend,
		ProtocolsModel:         cfg.ProtocolsModel,
		ProtocolContractsModel: cfg.ProtocolContractsModel,
		IngestStore:            cfg.IngestStore,
		NetworkPassphrase:      cfg.NetworkPassphrase,
		Processors:             cfg.Processors,
		WindowSize:             cfg.WindowSize,
		Metrics:                cfg.Metrics,
		TipProvider:            cfg.TipProvider,
	})
	if err != nil {
		return nil, err
	}
	if cfg.StateChanges == nil {
		return nil, fmt.Errorf("state changes model is required")
	}
	return &protocolHistoryRebuildService{
		engine:       migrate.engine,
		stateChanges: cfg.StateChanges,
	}, nil
}

// Run wipes each protocol's history and re-runs the history migration,
// holding each protocol's history advisory lock throughout. Safe to rerun
// after a failure: the wipe is idempotent and re-derived rows land at
// deterministic state_change_ids.
func (s *protocolHistoryRebuildService) Run(ctx context.Context, protocolIDs []string) error {
	protocolIDs = dedupePreservingOrder(protocolIDs)
	if err := s.validate(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols for history rebuild: %w", err)
	}

	release, lockErr := acquireMigrateLocks(ctx, s.engine.db, lockScopeHistory, protocolIDs)
	if lockErr != nil {
		return fmt.Errorf("locking protocols for history rebuild: %w", lockErr)
	}
	defer release()

	oldest, err := s.oldestRetained(ctx)
	if err != nil {
		return err
	}

	for _, pid := range protocolIDs {
		if wipeErr := s.wipe(ctx, pid, oldest); wipeErr != nil {
			return wipeErr
		}
	}

	// Statuses are not_started after the wipe, so this is a normal migration
	// run: fold from the retention floor, hand off to live at the frontier.
	return s.engine.Run(ctx, protocolIDs)
}

// validate requires each protocol to exist, be classified, and not be marked
// in_progress (dead-run residue — investigate, don't wipe under it).
func (s *protocolHistoryRebuildService) validate(ctx context.Context, protocolIDs []string) error {
	for _, pid := range protocolIDs {
		if _, ok := s.engine.processors[pid]; !ok {
			return fmt.Errorf("no processor registered for protocol %q", pid)
		}
	}

	protocols, err := s.engine.protocolsModel.GetByIDs(ctx, protocolIDs)
	if err != nil {
		return fmt.Errorf("querying protocols: %w", err)
	}
	found := make(map[string]*data.Protocols, len(protocols))
	for i := range protocols {
		found[protocols[i].ID] = &protocols[i]
	}
	for _, pid := range protocolIDs {
		p, ok := found[pid]
		if !ok {
			return fmt.Errorf("protocol %q not found in DB", pid)
		}
		if p.ClassificationStatus != data.StatusSuccess {
			return fmt.Errorf("protocol %q classification not complete (status: %s)", pid, p.ClassificationStatus)
		}
		if p.HistoryMigrationStatus == data.StatusInProgress {
			return fmt.Errorf("protocol %q history migration is marked in_progress; investigate the dead run before rebuilding", pid)
		}
	}
	return nil
}

// oldestRetained returns the wipe's lower bound: the oldest retained ledger.
// The upper bound is read per protocol, inside wipe, after the cursor reset.
func (s *protocolHistoryRebuildService) oldestRetained(ctx context.Context) (uint32, error) {
	oldest, err := s.engine.ingestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return 0, fmt.Errorf("reading oldest ingest ledger: %w", err)
	}
	if oldest == 0 {
		return 0, fmt.Errorf("ingestion has not started yet (oldest_ingest_ledger is 0)")
	}
	return oldest, nil
}

// wipe resets the cursor to oldest−1 and the status to not_started (one
// transaction), then deletes the protocol's rows in ledger slices.
//
// Order matters: the cursor reset commits first, which makes live's per-ledger
// CAS fail — live stops writing this protocol's history, so the deletes race
// nothing. The cursor row is UPDATEd, never deleted: live treats a missing
// cursor row as a fatal incident (ErrCASCursorMissing).
func (s *protocolHistoryRebuildService) wipe(ctx context.Context, protocolID string, oldest uint32) error {
	cursorName := s.engine.strategy.CursorName(protocolID)
	if txErr := db.RunInTransaction(ctx, s.engine.db, func(dbTx pgx.Tx) error {
		if updErr := s.engine.ingestStore.Update(ctx, dbTx, cursorName, oldest-1); updErr != nil {
			return fmt.Errorf("resetting cursor %s: %w", cursorName, updErr)
		}
		if statusErr := s.engine.strategy.UpdateMigrationStatus(ctx, dbTx, []string{protocolID}, data.StatusNotStarted); statusErr != nil {
			return fmt.Errorf("resetting migration status: %w", statusErr)
		}
		return nil
	}); txErr != nil {
		return fmt.Errorf("resetting history migration state for %s: %w", protocolID, txErr)
	}

	// The delete's upper bound is read AFTER the reset commits, so it covers
	// every row that can exist. A live writer that already passed its CAS holds
	// the cursor row, so the reset above waited for it to commit — and live
	// writes a ledger's history and bumps latest_ingest_ledger in one
	// transaction, so that writer's rows are at or below this value. Live
	// writers after the reset fail their CAS and write nothing. Reading this
	// before the reset would instead leave rows above the bound undeleted, for
	// the re-migration to re-derive at the same state_change_ids.
	latest, latestErr := s.engine.ingestStore.Get(ctx, data.LatestLedgerCursorName)
	if latestErr != nil {
		return fmt.Errorf("reading latest ingest ledger: %w", latestErr)
	}
	// Guard the slice loop below: uint32 arithmetic on an inverted window never
	// reaches the upper bound, so it would run until the process is killed.
	if latest < oldest {
		return fmt.Errorf("latest ingest ledger %d is below the oldest retained ledger %d: refusing to wipe an inverted window", latest, oldest)
	}

	base := s.engine.processors[protocolID].StateChangeOrdinalBase()
	var total int64
	for start := oldest; ; {
		end := latest
		if latest-start >= historyRebuildDeleteSlice {
			end = start + historyRebuildDeleteSlice - 1
		}
		deleted, err := s.stateChanges.DeleteNamespaceLedgerRange(ctx, base, start, end)
		if err != nil {
			return fmt.Errorf("deleting history rows for %s: %w", protocolID, err)
		}
		total += deleted
		if end == latest {
			break
		}
		start = end + 1
	}
	log.Ctx(ctx).Infof("Protocol %s: deleted %d history rows over ledgers [%d, %d], cursor %s reset to %d", protocolID, total, oldest, latest, cursorName, oldest-1)
	return nil
}
