package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// historyRebuildDeleteSlice is how many ledgers each DELETE statement covers
// while wiping a protocol's history rows. Each slice runs in its own
// transaction: small enough to bound decompression and WAL per statement,
// large enough that a full-window wipe stays a few thousand statements.
const historyRebuildDeleteSlice uint32 = 10_000

// ProtocolHistoryRebuildService deletes a protocol's state-change rows for a
// ledger range and re-derives them from the ledger backend. History rows are
// per-ledger records with no running totals, so any range rebuilds in
// isolation — unlike current state, which must replay from the protocol's
// first ledger. The rebuild never touches the protocol's cursor or migration
// status: its range is capped at live ingestion's committed history frontier,
// below which no concurrent writer exists.
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
	// FromLedger is the first ledger to rebuild; 0 means the oldest retained
	// ledger. Values below the retention floor are clamped up to it — those
	// rows are already gone.
	FromLedger uint32
	// ToLedger is the last ledger to rebuild; 0 means the protocol's committed
	// history frontier. Values above the frontier are capped to it — ledgers
	// past it are live ingestion's to write.
	ToLedger    uint32
	WindowSize  uint32
	Metrics     *metrics.MigrationMetrics
	TipProvider func() (uint32, error)
}

type protocolHistoryRebuildService struct {
	db             *pgxpool.Pool
	ledgerBackend  ledgerbackend.LedgerBackend
	protocolsModel data.ProtocolsModelInterface
	contractsModel data.ProtocolContractsModelInterface
	ingestStore    *data.IngestStoreModel
	stateChanges   *data.StateChangeModel
	passphrase     string
	processors     map[string]ProtocolProcessor
	fromLedger     uint32
	toLedger       uint32
	windowSize     uint32
	metrics        *metrics.MigrationMetrics
	tipProvider    func() (uint32, error)
}

// NewProtocolHistoryRebuildService creates a new protocolHistoryRebuildService from the given config.
func NewProtocolHistoryRebuildService(cfg ProtocolHistoryRebuildConfig) (*protocolHistoryRebuildService, error) {
	for i, p := range cfg.Processors {
		if p == nil {
			return nil, fmt.Errorf("protocol processor at index %d is nil", i)
		}
	}
	ppMap, err := utils.BuildMap(cfg.Processors, func(p ProtocolProcessor) string {
		return p.ProtocolID()
	})
	if err != nil {
		return nil, fmt.Errorf("building protocol processor map: %w", err)
	}
	if cfg.FromLedger != 0 && cfg.ToLedger != 0 && cfg.FromLedger > cfg.ToLedger {
		return nil, fmt.Errorf("from ledger %d is after to ledger %d", cfg.FromLedger, cfg.ToLedger)
	}

	mm := cfg.Metrics
	if mm == nil {
		mm = metrics.NewMetrics(prometheus.NewRegistry()).Migration
	}

	return &protocolHistoryRebuildService{
		db:             cfg.DB,
		ledgerBackend:  cfg.LedgerBackend,
		protocolsModel: cfg.ProtocolsModel,
		contractsModel: cfg.ProtocolContractsModel,
		ingestStore:    cfg.IngestStore,
		stateChanges:   cfg.StateChanges,
		passphrase:     cfg.NetworkPassphrase,
		processors:     ppMap,
		fromLedger:     cfg.FromLedger,
		toLedger:       cfg.ToLedger,
		windowSize:     cfg.WindowSize,
		metrics:        mm,
		tipProvider:    cfg.TipProvider,
	}, nil
}

// Run validates the protocols, resolves the rebuild range, deletes the
// protocols' history rows for that range, and re-derives them with a bounded
// fold. Rerunning after a failure is safe: the delete is idempotent and the
// fold re-copies exactly the deleted rows (state_change_ids are deterministic).
func (s *protocolHistoryRebuildService) Run(ctx context.Context, protocolIDs []string) error {
	protocolIDs = dedupePreservingOrder(protocolIDs)
	if err := s.validate(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols for history rebuild: %w", err)
	}

	release, lockErr := acquireMigrateLocks(ctx, s.db, lockScopeHistory, protocolIDs)
	if lockErr != nil {
		return fmt.Errorf("locking protocols for history rebuild: %w", lockErr)
	}
	defer release()

	from, to, err := s.resolveRange(ctx, protocolIDs)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Infof("Rebuilding history for %v over ledgers [%d, %d]", protocolIDs, from, to)

	for _, pid := range protocolIDs {
		if delErr := s.deleteHistoryRows(ctx, pid, from, to); delErr != nil {
			return delErr
		}
	}

	if foldErr := s.reDerive(ctx, protocolIDs, from, to); foldErr != nil {
		return fmt.Errorf("re-deriving history for %v over ledgers [%d, %d]: %w", protocolIDs, from, to, foldErr)
	}

	log.Ctx(ctx).Infof("History rebuild completed for %v over ledgers [%d, %d]", protocolIDs, from, to)
	return nil
}

// reDerive replays the inclusive ledger range [from, to] and persists each
// protocol's history rows in windows. It is deliberately not the migrate
// engine: every ledger in the range sits at or below live ingestion's
// committed frontier, so there is no cursor to advance, no frontier to gate
// on, and no handoff — and classification for the whole range was committed
// before the run started, so membership is loaded once instead of refreshed
// per window.
func (s *protocolHistoryRebuildService) reDerive(ctx context.Context, protocolIDs []string, from, to uint32) error {
	contractsByProtocol := make(map[string][]data.ProtocolContracts, len(protocolIDs))
	requiresContractData := false
	tracked := map[xdr.ContractId]struct{}{}
	for _, pid := range protocolIDs {
		contracts, loadErr := s.contractsModel.GetByProtocolID(ctx, s.db, pid)
		if loadErr != nil {
			return fmt.Errorf("loading contracts for %s: %w", pid, loadErr)
		}
		contractsByProtocol[pid] = contracts
		if !s.processors[pid].RequiresContractData() {
			continue
		}
		requiresContractData = true
		for _, c := range contracts {
			idBytes, decErr := hex.DecodeString(string(c.ContractID))
			if decErr != nil || len(idBytes) != len(xdr.ContractId{}) {
				return fmt.Errorf("protocol %s contract id %q is not a 32-byte hex hash: %w", pid, c.ContractID, decErr)
			}
			tracked[xdr.ContractId(idBytes)] = struct{}{}
		}
	}

	prepareFn := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.ledgerBackend.PrepareRange(ctx, ledgerbackend.BoundedRange(from, to))
	}
	if _, prepErr := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff, prepareFn,
		func(attempt int, err error, backoff time.Duration) {
			log.Ctx(ctx).Warnf("Error preparing range [%d, %d] (attempt %d/%d): %v, retrying in %v...",
				from, to, attempt+1, maxLedgerFetchRetries, err, backoff)
		},
	); prepErr != nil {
		return fmt.Errorf("preparing range [%d, %d]: %w", from, to, prepErr)
	}

	windowSize := s.windowSize
	if windowSize == 0 {
		windowSize = 1
	}

	flush := func(winStart, winEnd uint32) error {
		for _, pid := range protocolIDs {
			processor := s.processors[pid]
			if txErr := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
				// Safe to relax durability: a crash just means rerunning the
				// rebuild, whose delete makes the re-derivation idempotent.
				if _, execErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); execErr != nil {
					return fmt.Errorf("setting synchronous_commit=off: %w", execErr)
				}
				return processor.PersistHistory(ctx, dbTx)
			}); txErr != nil {
				return fmt.Errorf("persisting window [%d,%d] for protocol %s: %w", winStart, winEnd, pid, txErr)
			}
			processor.Reset()
		}
		return nil
	}

	var pending uint32
	for seq := from; seq <= to; seq++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		ledgerMeta, fetchErr := s.ledgerBackend.GetLedger(ctx, seq)
		if fetchErr != nil {
			return fmt.Errorf("fetching ledger %d: %w", seq, fetchErr)
		}
		ledgerEvents, eventsErr := indexer.ExtractContractEventsForLedger(ledgerMeta)
		if eventsErr != nil {
			return fmt.Errorf("extracting contract events for ledger %d: %w", seq, eventsErr)
		}
		var contractDataChanges map[string][]ingest.Change
		if requiresContractData {
			var cdErr error
			contractDataChanges, cdErr = indexer.ExtractContractDataChangesForLedger(ledgerMeta, tracked)
			if cdErr != nil {
				return fmt.Errorf("extracting contract data changes for ledger %d: %w", seq, cdErr)
			}
		}

		for _, pid := range protocolIDs {
			input := ProtocolProcessorInput{
				LedgerSequence:      seq,
				LedgerCloseTime:     ledgerMeta.LedgerCloseTime(),
				ContractEvents:      ledgerEvents,
				ProtocolContracts:   contractsByProtocol[pid],
				StagingMode:         StagingModeHistory,
				ContractDataChanges: contractDataChanges,
			}
			if processErr := s.processors[pid].ProcessLedger(ctx, input); processErr != nil {
				return fmt.Errorf("processing ledger %d for protocol %s: %w", seq, pid, processErr)
			}
		}

		pending++
		if pending >= windowSize {
			if flushErr := flush(seq-pending+1, seq); flushErr != nil {
				return flushErr
			}
			pending = 0
		}

		s.metrics.CurrentLedger.Set(float64(seq))
		s.metrics.LedgersProcessed.Inc()
		if (seq-from+1)%progressLogInterval == 0 {
			log.Ctx(ctx).Infof("Rebuild progress: processed ledger %d of [%d, %d]", seq, from, to)
		}
	}
	if pending > 0 {
		return flush(to-pending+1, to)
	}
	return nil
}

// validate requires each protocol to exist, be classified, and have a
// COMPLETED history migration. Completion pins the protocol's history cursor
// to live's frontier, which is what makes the cursor a safe range cap; an
// incomplete migration means rows above the cursor don't exist yet and a
// concurrent migration could COPY into the range being rebuilt.
func (s *protocolHistoryRebuildService) validate(ctx context.Context, protocolIDs []string) error {
	for _, pid := range protocolIDs {
		if _, ok := s.processors[pid]; !ok {
			return fmt.Errorf("no processor registered for protocol %q", pid)
		}
	}

	protocols, err := s.protocolsModel.GetByIDs(ctx, protocolIDs)
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
		if p.HistoryMigrationStatus != data.StatusSuccess {
			return fmt.Errorf("protocol %q history migration has not completed (status: %s); run protocol-migrate history first", pid, p.HistoryMigrationStatus)
		}
	}
	return nil
}

// resolveRange clamps the configured range to what actually exists: the floor
// is the oldest retained ledger (rows below it were dropped by retention) and
// the cap is the lowest committed history frontier across the protocols
// (ledgers above it belong to live ingestion).
func (s *protocolHistoryRebuildService) resolveRange(ctx context.Context, protocolIDs []string) (uint32, uint32, error) {
	oldest, err := s.ingestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return 0, 0, fmt.Errorf("reading oldest ingest ledger: %w", err)
	}
	if oldest == 0 {
		return 0, 0, fmt.Errorf("ingestion has not started yet (oldest_ingest_ledger is 0)")
	}

	var frontier uint32
	for _, pid := range protocolIDs {
		cursor, readErr := s.ingestStore.Get(ctx, utils.ProtocolHistoryCursorName(pid))
		if readErr != nil {
			return 0, 0, fmt.Errorf("reading history cursor for %s: %w", pid, readErr)
		}
		if cursor == 0 {
			return 0, 0, fmt.Errorf("protocol %s has no history cursor despite a completed migration", pid)
		}
		if frontier == 0 || cursor < frontier {
			frontier = cursor
		}
	}

	from := s.fromLedger
	if from < oldest {
		if from != 0 {
			log.Ctx(ctx).Infof("Clamping rebuild start %d up to the oldest retained ledger %d", from, oldest)
		}
		from = oldest
	}
	to := s.toLedger
	if to == 0 || to > frontier {
		if to != 0 {
			log.Ctx(ctx).Infof("Capping rebuild end %d down to the committed history frontier %d", to, frontier)
		}
		to = frontier
	}
	if from > to {
		return 0, 0, fmt.Errorf("resolved rebuild range is empty: from %d > to %d", from, to)
	}
	return from, to, nil
}

// deleteHistoryRows wipes one protocol's state-change rows over [from, to] in
// ledger slices, each its own transaction. No atomicity with the fold is
// needed: these ledgers sit at or below the committed frontier, so nothing
// else writes them.
func (s *protocolHistoryRebuildService) deleteHistoryRows(ctx context.Context, protocolID string, from, to uint32) error {
	base := s.processors[protocolID].StateChangeOrdinalBase()
	var total int64
	slices := 0
	for start := from; ; {
		end := to
		if to-start >= historyRebuildDeleteSlice {
			end = start + historyRebuildDeleteSlice - 1
		}
		deleted, err := s.stateChanges.DeleteNamespaceLedgerRange(ctx, base, start, end)
		if err != nil {
			return fmt.Errorf("deleting history rows for %s: %w", protocolID, err)
		}
		total += deleted
		slices++
		if slices%100 == 0 {
			log.Ctx(ctx).Infof("Protocol %s: deleted %d history rows through ledger %d", protocolID, total, end)
		}
		if end == to {
			break
		}
		start = end + 1
	}
	log.Ctx(ctx).Infof("Protocol %s: deleted %d history rows over ledgers [%d, %d]", protocolID, total, from, to)
	return nil
}

// dedupePreservingOrder returns protocolIDs with duplicates removed, keeping
// first occurrences in order.
func dedupePreservingOrder(protocolIDs []string) []string {
	seen := make(map[string]struct{}, len(protocolIDs))
	unique := make([]string, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		if _, dup := seen[pid]; !dup {
			seen[pid] = struct{}{}
			unique = append(unique, pid)
		}
	}
	return unique
}
