package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
)

const (
	// repairPageSize is how many repair units ListUnits returns per page. Pages are
	// fetched sequentially; units within a page are repaired concurrently.
	repairPageSize = 1000
	// maxRepairAttempts bounds the fetch/apply retry loop per unit. Each retry
	// needs fresh truth (an apply loses only to a newer fold write); a unit hot
	// enough to lose every attempt is logged and skipped.
	maxRepairAttempts = 5
)

// RepairScope filters which units a repair run covers. Zero-valued fields are
// unset; ListUnits interprets the combination.
type RepairScope struct {
	// ContractAddress limits the run to one token contract (C... strkey).
	ContractAddress string
	// AccountAddress limits the run to one holder (G... or C... strkey).
	AccountAddress string
}

// RepairUnit identifies one repairable piece of a protocol's current state
// (for SEP-41: a (holder, token) pair). Truth is the authoritative value(s)
// for a unit as read from the network. Both are opaque to the engine — only
// the protocol's ProtocolCurrentStateRepair implementation interprets them.
type (
	RepairUnit any
	Truth      any
)

// ProtocolCurrentStateRepair is the per-protocol seam the repair engine drives,
// mirroring how the migration engine drives ProtocolProcessor. The protocol owns
// what a unit is, how its truth is read, and how truth lands in its tables; the
// engine owns iteration, concurrency, and retries.
type ProtocolCurrentStateRepair interface {
	// ListUnits pages repair units in scope from the protocol's own tables.
	// cursor is "" for the first page; a returned "" cursor ends iteration.
	ListUnits(ctx context.Context, scope RepairScope, cursor string, limit int) ([]RepairUnit, string, error)
	// FetchTruth reads the unit's authoritative state via RPC simulation and the
	// ledger it is true at. Multi-read truth must be consistent at that single
	// ledger. An error means the unit can't be verified right now (e.g. archived
	// contract) — the engine skips it and reports.
	FetchTruth(ctx context.Context, unit RepairUnit) (Truth, uint32, error)
	// Apply conditionally writes the truth inside dbTx. applied=false means the
	// row moved past ledger — the engine refetches and retries.
	Apply(ctx context.Context, dbTx pgx.Tx, unit RepairUnit, truth Truth, ledger uint32) (bool, error)
}

// currentStateRepairRegistry holds repairer factories keyed by protocol ID,
// following the validator/processor registries: writes happen only from
// per-protocol package init() functions, reads only after main() starts, so no
// synchronization is needed (see validator_registry.go). Factories are invoked
// per run.
var currentStateRepairRegistry = map[string]func(ProtocolDeps) ProtocolCurrentStateRepair{}

// RegisterCurrentStateRepairer registers a repairer factory for a protocol ID.
// Called from per-protocol package init() functions.
func RegisterCurrentStateRepairer(protocolID string, factory func(ProtocolDeps) ProtocolCurrentStateRepair) {
	currentStateRepairRegistry[protocolID] = factory
}

// BuildCurrentStateRepairers materializes repairers for the given protocol IDs
// from the registry, erroring on IDs with no registered factory.
func BuildCurrentStateRepairers(deps ProtocolDeps, protocolIDs []string) (map[string]ProtocolCurrentStateRepair, error) {
	out := make(map[string]ProtocolCurrentStateRepair, len(protocolIDs))
	for _, pid := range protocolIDs {
		factory, ok := currentStateRepairRegistry[pid]
		if !ok {
			return nil, fmt.Errorf("no current-state repairer registered for protocol %q", pid)
		}
		r := factory(deps)
		if r == nil {
			return nil, fmt.Errorf("current-state repairer factory for protocol %q returned nil", pid)
		}
		out[pid] = r
	}
	return out, nil
}

// repairStats accumulates a run's outcome counts. Mutex-guarded: units within a
// page are repaired concurrently.
type repairStats struct {
	mu      sync.Mutex
	checked int
	applied int
	skipped int // FetchTruth failed (e.g. archived contract)
	gaveUp  int // lost every apply attempt to concurrent fold writes
}

func (s *repairStats) record(fn func(*repairStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

// protocolCurrentStateRepairService repairs a protocol's current-state tables
// from network truth, unit by unit, while live ingestion keeps writing. No
// coordination is needed: Apply's conditional write and the fold's ledger guard
// are both evaluated in SQL against the committed row, so either write order
// converges (see the SEP-41 balances model).
type protocolCurrentStateRepairService struct {
	db             *pgxpool.Pool
	protocolsModel data.ProtocolsModelInterface
	repairers      map[string]ProtocolCurrentStateRepair
	// concurrency bounds simultaneous FetchTruth/Apply pipelines within a page —
	// effectively the RPC simulation parallelism.
	concurrency int
}

// NewProtocolCurrentStateRepairService creates the repair engine. repairers maps
// protocol ID to its ProtocolCurrentStateRepair implementation.
func NewProtocolCurrentStateRepairService(
	dbPool *pgxpool.Pool,
	protocolsModel data.ProtocolsModelInterface,
	repairers map[string]ProtocolCurrentStateRepair,
	concurrency int,
) *protocolCurrentStateRepairService {
	if concurrency < 1 {
		concurrency = 1
	}
	return &protocolCurrentStateRepairService{
		db:             dbPool,
		protocolsModel: protocolsModel,
		repairers:      repairers,
		concurrency:    concurrency,
	}
}

// Run repairs the protocol's current state within scope.
func (s *protocolCurrentStateRepairService) Run(ctx context.Context, protocolID string, scope RepairScope) error {
	repairer, ok := s.repairers[protocolID]
	if !ok {
		return fmt.Errorf("no current-state repairer registered for protocol %q", protocolID)
	}

	// Refuse to run during a current-state migration: its multi-ledger window
	// sums can straddle a repair's ledger, which the per-row guard cannot split.
	// Live ingestion commits per ledger, where the guard is exact.
	protocols, err := s.protocolsModel.GetByIDs(ctx, []string{protocolID})
	if err != nil {
		return fmt.Errorf("querying protocol %q: %w", protocolID, err)
	}
	if len(protocols) == 0 {
		return fmt.Errorf("protocol %q not found in DB", protocolID)
	}
	if protocols[0].CurrentStateMigrationStatus == data.StatusInProgress {
		return fmt.Errorf("protocol %q current-state migration is in progress; repair would race its windowed commits", protocolID)
	}

	stats := &repairStats{}
	startTime := time.Now()

	for cursor := ""; ; {
		units, nextCursor, listErr := repairer.ListUnits(ctx, scope, cursor, repairPageSize)
		if listErr != nil {
			return fmt.Errorf("listing repair units for %q: %w", protocolID, listErr)
		}
		if len(units) == 0 {
			break
		}
		if runErr := s.repairPage(ctx, repairer, units, stats); runErr != nil {
			return runErr
		}
		if nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	log.Ctx(ctx).Infof(
		"current-state repair for %s finished in %s: %d checked, %d applied, %d skipped (truth unavailable), %d gave up (hot rows)",
		protocolID, time.Since(startTime).Round(time.Millisecond),
		stats.checked, stats.applied, stats.skipped, stats.gaveUp,
	)
	return nil
}

// repairPage repairs one page of units with bounded concurrency. The first hard
// error (DB failure) cancels the page and is returned; FetchTruth failures and
// exhausted retries are per-unit outcomes recorded in stats, not errors.
func (s *protocolCurrentStateRepairService) repairPage(ctx context.Context, repairer ProtocolCurrentStateRepair, units []RepairUnit, stats *repairStats) error {
	pageCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	sem := make(chan struct{}, s.concurrency)

	for _, unit := range units {
		if pageCtx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(unit RepairUnit) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.repairUnit(pageCtx, repairer, unit, stats); err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}(unit)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled while repairing page: %w", err)
	}
	return nil
}

// repairUnit runs the fetch/apply loop for one unit. Every attempt refetches:
// an apply loses only when the fold wrote past the truth's ledger, which makes
// the previous truth stale by definition.
func (s *protocolCurrentStateRepairService) repairUnit(ctx context.Context, repairer ProtocolCurrentStateRepair, unit RepairUnit, stats *repairStats) error {
	stats.record(func(st *repairStats) { st.checked++ })

	for attempt := 1; attempt <= maxRepairAttempts; attempt++ {
		truth, ledger, err := repairer.FetchTruth(ctx, unit)
		if err != nil {
			log.Ctx(ctx).Warnf("skipping repair unit %v: fetching truth: %v", unit, err)
			stats.record(func(st *repairStats) { st.skipped++ })
			return nil
		}
		var applied bool
		// Single-row tx: live ingestion writes multi-row batches, and two
		// multi-row writers with different key orders can deadlock.
		txErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
			var applyErr error
			applied, applyErr = repairer.Apply(ctx, dbTx, unit, truth, ledger)
			if applyErr != nil {
				return fmt.Errorf("applying truth at ledger %d: %w", ledger, applyErr)
			}
			return nil
		})
		if txErr != nil {
			return fmt.Errorf("applying repair for unit %v: %w", unit, txErr)
		}
		if applied {
			stats.record(func(st *repairStats) { st.applied++ })
			return nil
		}
	}

	log.Ctx(ctx).Warnf("giving up on repair unit %v after %d attempts: row kept moving past the truth ledger", unit, maxRepairAttempts)
	stats.record(func(st *repairStats) { st.gaveUp++ })
	return nil
}
