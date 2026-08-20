package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
)

// ProtocolCurrentStateRebuildService wipes a protocol's current-state rows
// and rebuilds them from the protocol's first ledger. Current-state columns
// are running totals over the full event history, so — unlike history, which
// rebuilds any range in isolation — a correct rebuild must replay from
// genesis. The wipe and the cursor reset share one transaction per protocol:
// the cursor UPDATE takes the row lock live ingestion's per-ledger CAS also
// needs, so live serializes against the wipe and skips the protocol's writes
// until the rebuild reaches the tip and hands ownership back.
type ProtocolCurrentStateRebuildService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

var _ ProtocolCurrentStateRebuildService = (*protocolCurrentStateRebuildService)(nil)

type protocolCurrentStateRebuildService struct {
	engine      protocolMigrateEngine
	startLedger uint32
}

// NewProtocolCurrentStateRebuildService creates a rebuild service from the
// same configuration as the plain current-state migration.
func NewProtocolCurrentStateRebuildService(cfg ProtocolMigrateCurrentStateConfig) (*protocolCurrentStateRebuildService, error) {
	migrate, err := NewProtocolMigrateCurrentStateService(cfg)
	if err != nil {
		return nil, err
	}
	return &protocolCurrentStateRebuildService{
		engine:      migrate.engine,
		startLedger: cfg.StartLedger,
	}, nil
}

// Run validates the protocols, wipes each one's current-state rows (resetting
// its cursor and migration status in the same transaction), and re-runs the
// current-state migration from the start ledger. Holds each protocol's
// current-state advisory lock for the duration.
func (s *protocolCurrentStateRebuildService) Run(ctx context.Context, protocolIDs []string) error {
	protocolIDs = dedupePreservingOrder(protocolIDs)
	if err := s.validate(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols for current-state rebuild: %w", err)
	}

	release, lockErr := acquireMigrateLocks(ctx, s.engine.db, lockScopeCurrentState, protocolIDs)
	if lockErr != nil {
		return fmt.Errorf("locking protocols for current-state rebuild: %w", lockErr)
	}
	defer release()

	for _, pid := range protocolIDs {
		if err := s.wipe(ctx, pid); err != nil {
			return err
		}
	}

	// The wipe reset every status to not_started, so the engine runs the
	// normal migration lifecycle: in_progress → fold from the cursor → CAS
	// handoff at the tip → success.
	return s.engine.Run(ctx, protocolIDs)
}

// validate requires each protocol to exist, be classified, and not have a
// migration marked in_progress — that is dead-run residue to investigate, not
// state to wipe under.
func (s *protocolCurrentStateRebuildService) validate(ctx context.Context, protocolIDs []string) error {
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
		if p.CurrentStateMigrationStatus == data.StatusInProgress {
			return fmt.Errorf("protocol %q current-state migration is marked in_progress; investigate the dead run before rebuilding", pid)
		}
	}
	return nil
}

// wipe deletes the protocol's current-state rows and resets its cursor to
// startLedger − 1 and its migration status to not_started, all in one
// transaction. The cursor row is updated, never deleted — live ingestion
// treats a vanished cursor row as a fatal incident (ErrCASCursorMissing).
func (s *protocolCurrentStateRebuildService) wipe(ctx context.Context, protocolID string) error {
	processor := s.engine.processors[protocolID]
	cursorName := s.engine.strategy.CursorName(protocolID)
	if txErr := db.RunInTransaction(ctx, s.engine.db, func(dbTx pgx.Tx) error {
		if updErr := s.engine.ingestStore.Update(ctx, dbTx, cursorName, s.startLedger-1); updErr != nil {
			return fmt.Errorf("resetting cursor %s: %w", cursorName, updErr)
		}
		if statusErr := s.engine.strategy.UpdateMigrationStatus(ctx, dbTx, []string{protocolID}, data.StatusNotStarted); statusErr != nil {
			return fmt.Errorf("resetting migration status: %w", statusErr)
		}
		return processor.WipeCurrentState(ctx, dbTx)
	}); txErr != nil {
		return fmt.Errorf("wiping current state for %s: %w", protocolID, txErr)
	}
	log.Ctx(ctx).Infof("Protocol %s: current-state rows wiped, cursor %s reset to %d", protocolID, cursorName, s.startLedger-1)
	return nil
}
