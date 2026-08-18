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
	// maxRepairAttempts bounds the fetch-truth/apply retry loop per unit. An apply
	// loses only when live ingestion wrote the row after the truth's ledger, so each
	// retry needs a fresh simulation; a unit hot enough to lose every attempt is
	// logged and skipped rather than chased.
	maxRepairAttempts = 5
	// finalizeCursorPollInterval / finalizeCursorWait bound the wait for live
	// ingestion's cursor to pass the run's highest truth ledger before Finalize
	// removes zero barrier rows. With live down the wait times out and barriers are
	// left in place (a later run's Finalize removes them).
	finalizeCursorPollInterval = 2 * time.Second
	finalizeCursorWait         = 5 * time.Minute
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
// mirroring how the migration engine drives ProtocolProcessor. Implementations
// own what a unit is, how its on-chain truth is read, and how that truth lands
// in their tables; the engine owns iteration, concurrency, retries, and the
// finalize barrier-cleanup ordering.
type ProtocolCurrentStateRepair interface {
	// ListUnits pages repair units in scope from the protocol's own tables.
	// cursor is "" for the first page; a returned "" cursor ends iteration.
	ListUnits(ctx context.Context, scope RepairScope, cursor string, limit int) ([]RepairUnit, string, error)
	// FetchTruth reads the unit's authoritative state from the network (RPC
	// simulation of the protocol's read-only interface) and returns it with the
	// ledger it is true at. Values spanning multiple reads must be internally
	// consistent at that single ledger. An error means the unit cannot be
	// verified right now (e.g. the contract is archived) — the engine skips it
	// and reports.
	FetchTruth(ctx context.Context, unit RepairUnit) (Truth, uint32, error)
	// Apply conditionally writes the unit's truth inside dbTx, no-oping with
	// applied=false when the row has moved past ledger — the engine then
	// refetches truth and retries.
	Apply(ctx context.Context, dbTx pgx.Tx, unit RepairUnit, truth Truth, ledger uint32) (bool, error)
	// Finalize runs once after all units, with live ingestion's cursor at or
	// beyond every ledger this run wrote (so no stale fold delta remains in
	// flight below it). SEP-41 uses it to delete the zero barrier rows the run
	// left behind.
	Finalize(ctx context.Context, dbTx pgx.Tx, liveCursor uint32) error
}

// currentStateRepairRegistry holds repairer factories keyed by protocol ID,
// following the validator/processor registries: writes happen only from
// per-protocol package init() functions, reads only after main() starts, so no
// synchronization is needed (see validator_registry.go). Factories are invoked
// per run — a repairer may carry per-run state (SEP-41 accumulates the zero
// barrier rows it wrote for Finalize).
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
	mu       sync.Mutex
	checked  int
	applied  int
	skipped  int // FetchTruth failed (e.g. archived contract)
	gaveUp   int // lost every apply attempt to concurrent fold writes
	maxTruth uint32
}

func (s *repairStats) record(fn func(*repairStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

// protocolCurrentStateRepairService repairs a protocol's current-state tables
// from network truth, unit by unit, concurrently with live ingestion. No
// coordination with the live fold is needed: Apply's conditional write and the
// fold's strict-monotone ledger guard are evaluated against the same row inside
// Postgres, so either order of concurrent writes converges (see the SEP-41
// balances model for the guard).
type protocolCurrentStateRepairService struct {
	db             *pgxpool.Pool
	ingestStore    *data.IngestStoreModel
	protocolsModel data.ProtocolsModelInterface
	repairers      map[string]ProtocolCurrentStateRepair
	// concurrency bounds simultaneous FetchTruth/Apply pipelines within a page —
	// effectively the RPC simulation parallelism.
	concurrency int
	// finalizeWait / finalizePoll bound the wait for live ingestion's cursor before
	// Finalize. Defaulted in the constructor; overridable in tests.
	finalizeWait time.Duration
	finalizePoll time.Duration
}

// NewProtocolCurrentStateRepairService creates the repair engine. repairers maps
// protocol ID to its ProtocolCurrentStateRepair implementation.
func NewProtocolCurrentStateRepairService(
	dbPool *pgxpool.Pool,
	ingestStore *data.IngestStoreModel,
	protocolsModel data.ProtocolsModelInterface,
	repairers map[string]ProtocolCurrentStateRepair,
	concurrency int,
) *protocolCurrentStateRepairService {
	if concurrency < 1 {
		concurrency = 1
	}
	return &protocolCurrentStateRepairService{
		db:             dbPool,
		ingestStore:    ingestStore,
		protocolsModel: protocolsModel,
		repairers:      repairers,
		concurrency:    concurrency,
		finalizeWait:   finalizeCursorWait,
		finalizePoll:   finalizeCursorPollInterval,
	}
}

// Run repairs the protocol's current state within scope.
func (s *protocolCurrentStateRepairService) Run(ctx context.Context, protocolID string, scope RepairScope) error {
	repairer, ok := s.repairers[protocolID]
	if !ok {
		return fmt.Errorf("no current-state repairer registered for protocol %q", protocolID)
	}

	// A current-state migration folds multi-ledger windows whose summed deltas the
	// per-row ledger guard cannot split, so repairing concurrently with one could
	// double-count. Live ingestion (post-handoff) commits per ledger, where the
	// guard is exact.
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

	if finalizeErr := s.finalize(ctx, repairer, stats); finalizeErr != nil {
		return finalizeErr
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

// repairUnit runs the fetch-truth/apply loop for one unit. Every attempt uses a
// fresh truth: an apply loses only when the fold wrote the row after the truth's
// ledger, making the previous truth stale by definition.
func (s *protocolCurrentStateRepairService) repairUnit(ctx context.Context, repairer ProtocolCurrentStateRepair, unit RepairUnit, stats *repairStats) error {
	stats.record(func(st *repairStats) { st.checked++ })

	for attempt := 1; attempt <= maxRepairAttempts; attempt++ {
		truth, ledger, err := repairer.FetchTruth(ctx, unit)
		if err != nil {
			log.Ctx(ctx).Warnf("skipping repair unit %v: fetching truth: %v", unit, err)
			stats.record(func(st *repairStats) { st.skipped++ })
			return nil
		}
		stats.record(func(st *repairStats) {
			if ledger > st.maxTruth {
				st.maxTruth = ledger
			}
		})

		var applied bool
		// Single-row transaction: live ingestion upserts multi-row batches, and two
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

// finalize waits for live ingestion's cursor to reach the run's highest truth
// ledger — past it no fold delta at or below any repair write remains in flight —
// then lets the repairer clean up (SEP-41: delete zero barrier rows). If the
// cursor doesn't get there in time (live ingestion down or far behind), barriers
// are left in place: they are correct rows, just not yet removable, and a later
// run's Finalize sweeps them.
func (s *protocolCurrentStateRepairService) finalize(ctx context.Context, repairer ProtocolCurrentStateRepair, stats *repairStats) error {
	if stats.maxTruth == 0 {
		return nil
	}

	deadline := time.Now().Add(s.finalizeWait)
	var liveCursor uint32
	for {
		var err error
		liveCursor, err = s.ingestStore.Get(ctx, data.LatestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("reading live ingestion cursor before finalize: %w", err)
		}
		if liveCursor >= stats.maxTruth {
			break
		}
		if time.Now().After(deadline) {
			log.Ctx(ctx).Warnf(
				"skipping repair finalize: live cursor %d has not reached the run's highest truth ledger %d after %s; zero barrier rows left for a later run",
				liveCursor, stats.maxTruth, s.finalizeWait,
			)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting to finalize repair: %w", ctx.Err())
		case <-time.After(s.finalizePoll):
		}
	}

	if err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return repairer.Finalize(ctx, dbTx, liveCursor)
	}); err != nil {
		return fmt.Errorf("finalizing repair run: %w", err)
	}
	return nil
}
