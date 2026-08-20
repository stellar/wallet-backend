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

// historyRebuildDeleteSlice is how many ledgers each DELETE statement covers
// while wiping a protocol's history rows. Each slice runs in its own
// transaction: small enough to bound decompression and WAL per statement,
// large enough that a full-window wipe stays a few thousand statements.
const historyRebuildDeleteSlice uint32 = 10_000

// ProtocolHistoryRebuildService deletes a protocol's state-change rows and
// re-runs the history migration over the retained window. Mirrors the
// current-state rebuild: the wipe restores a never-migrated protocol's
// preconditions (cursor at the retention floor, status not_started, no rows),
// after which the unmodified migration engine folds forward and hands off to
// live ingestion at the frontier. The cursor reset commits before any delete,
// so live stops writing the protocol's history for the duration — its writes
// resume at handoff, exactly as after a first migration.
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

// Run validates the protocols, wipes each one's history rows (resetting its
// cursor and migration status first), and re-runs the history migration over
// the retained window. Holds each protocol's history advisory lock for the
// duration. Rerunning after a failure is safe: the wipe is idempotent and the
// re-derived rows land at deterministic state_change_ids.
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

	oldest, latest, err := s.retainedWindow(ctx)
	if err != nil {
		return err
	}

	for _, pid := range protocolIDs {
		if wipeErr := s.wipe(ctx, pid, oldest, latest); wipeErr != nil {
			return wipeErr
		}
	}

	// The wipe reset every status to not_started, so the engine runs the
	// normal migration lifecycle: in_progress → fold from the retention
	// floor → CAS handoff at the frontier → success.
	return s.engine.Run(ctx, protocolIDs)
}

// validate requires each protocol to exist, be classified, and not have a
// history migration marked in_progress — that is dead-run residue to
// investigate, not state to wipe under.
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

// retainedWindow returns the ledger bounds of the rows a wipe must cover:
// from the oldest retained ledger through live ingestion's committed tip.
func (s *protocolHistoryRebuildService) retainedWindow(ctx context.Context) (uint32, uint32, error) {
	oldest, err := s.engine.ingestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return 0, 0, fmt.Errorf("reading oldest ingest ledger: %w", err)
	}
	if oldest == 0 {
		return 0, 0, fmt.Errorf("ingestion has not started yet (oldest_ingest_ledger is 0)")
	}
	latest, err := s.engine.ingestStore.Get(ctx, data.LatestLedgerCursorName)
	if err != nil {
		return 0, 0, fmt.Errorf("reading latest ingest ledger: %w", err)
	}
	if latest == 0 {
		return 0, 0, fmt.Errorf("ingestion has not started yet (latest_ingest_ledger is 0)")
	}
	return oldest, latest, nil
}

// wipe resets the protocol's cursor to oldest − 1 and its migration status to
// not_started in one transaction, then deletes the protocol's history rows in
// ledger slices, each its own transaction (DML on the compressed hypertable
// cannot run as one statement). The cursor reset commits FIRST: from that
// moment live ingestion's per-ledger CAS fails and it writes no new history
// rows for this protocol, so the deletes race nothing and the engine's folds
// re-derive every ledger exactly once. The cursor row is updated, never
// deleted — live treats a vanished cursor row as a fatal incident
// (ErrCASCursorMissing).
func (s *protocolHistoryRebuildService) wipe(ctx context.Context, protocolID string, oldest, latest uint32) error {
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
