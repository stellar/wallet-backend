package services

import (
	"context"
	"fmt"
	"hash/fnv"
	"runtime"
	"strings"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	// maxLedgerFetchRetries is the maximum number of retry attempts when fetching a ledger fails.
	maxLedgerFetchRetries = 10
	// maxRetryBackoff is the maximum backoff duration between retry attempts.
	maxRetryBackoff = 30 * time.Second
	// maxClassificationReadRetries bounds retries of the non-transactional read that resolves
	// prior-ledger protocol classifications. It runs before the persist transaction opens, so a
	// transient DB blip (e.g. a CNPG failover) there must not exit live ingestion — this keeps the
	// read under the same retry umbrella as the ledger fetch and the persist ladder.
	maxClassificationReadRetries = 5
	// IngestionModeLive represents continuous ingestion from the latest ledger onwards.
	IngestionModeLive = "live"
	// IngestionModeBackfill represents historical ledger ingestion for a specified range.
	IngestionModeBackfill = "backfill"
)

// LedgerBackendFactory creates new LedgerBackend instances for parallel batch processing.
// Each batch needs its own backend because LedgerBackend is not thread-safe.
type LedgerBackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

// IngestServiceConfig holds the configuration for creating an IngestService.
type IngestServiceConfig struct {
	// === Core ===
	IngestionMode string
	Models        *data.Models
	AppTracker    apptracker.AppTracker
	Metrics       *metrics.Metrics

	// === Stellar Network ===
	Network           string
	NetworkPassphrase string
	Archive           historyarchive.ArchiveInterface
	RPCService        RPCService

	// === Ledger Backend ===
	LedgerBackend        ledgerbackend.LedgerBackend
	LedgerBackendFactory LedgerBackendFactory
	// IsPermanentFetchError classifies a GetLedger error as permanent, i.e. no
	// amount of retrying will fix it (e.g. the datastore backend's prefetch
	// buffer has died). Optional: nil (the zero value) retries every
	// ledger-fetch error as before.
	IsPermanentFetchError func(error) bool

	// === Cursors ===
	OldestLedgerCursorName string

	// === Live Mode Dependencies ===
	TokenIngestionService TokenIngestionService
	CheckpointService     CheckpointService

	// === Protocol Processors ===
	ProtocolProcessors []ProtocolProcessor // nil means no protocol state production

	// === Live Classification ===
	// ProtocolValidators are run once per ledger inside persistLedgerData against
	// the buffered raw WASMs and contracts. Order matters: validators earlier in
	// the slice win first-match-wins ties for the same wasm hash. Pass the result
	// of services.BuildValidators with services.GetAllValidatorIDs() (already
	// sorted) for deterministic priority.
	ProtocolValidators []ProtocolValidator
	// WasmSpecExtractor is owned by the caller (lifecycle is process-wide). The
	// dispatcher uses it to extract spec entries from candidate wasm bytecode.
	WasmSpecExtractor WasmSpecExtractor

	// === Metadata Service (used for SAC; per-protocol validators/processors get
	// it via ProtocolDeps and are responsible for their own protocol metadata) ===
	ContractMetadataService ContractMetadataService

	// === Processing Options ===
	GetLedgersLimit int

	// === Backfill Tuning ===
	BackfillWorkers           int
	BackfillBatchSize         int
	BackfillDBInsertBatchSize int
}

// generateAdvisoryLockID creates a deterministic advisory lock ID based on the network name.
// This ensures different networks (mainnet, testnet) get separate locks while being consistent across restarts.
func generateAdvisoryLockID(network string) int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-ingest-" + network))
	return int(h.Sum64())
}

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
	// Close stops the worker pools this service owns. Call it after Run has
	// returned (no ledger indexing/backfill in flight) and before the shared
	// DB pool is closed.
	Close()
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	ingestionMode             string
	models                    *data.Models
	oldestLedgerCursorName    string
	advisoryLockID            int
	appTracker                apptracker.AppTracker
	rpcService                RPCService
	ledgerBackend             ledgerbackend.LedgerBackend
	ledgerBackendFactory      LedgerBackendFactory
	isPermanentFetchError     func(error) bool
	tokenIngestionService     TokenIngestionService
	checkpointService         CheckpointService
	appMetrics                *metrics.Metrics
	networkPassphrase         string
	getLedgersLimit           int
	ledgerIndexer             *indexer.Indexer
	archive                   historyarchive.ArchiveInterface
	ledgerIndexerPool         pond.Pool
	backfillPool              pond.Pool
	backfillBatchSize         uint32
	backfillDBInsertBatchSize uint32
	knownContractIDs          set.Set[string]
	contractMetadataService   ContractMetadataService
	protocolProcessors        map[string]ProtocolProcessor
	protocolValidators        []ProtocolValidator
	wasmSpecExtractor         WasmSpecExtractor
	// protocolCursors tracks, per protocol, whether its history/current-state
	// ingest_store cursor rows exist. See casProtocolCursor and
	// snapshotProtocolCursors in ingest_live.go. Always non-nil; empty maps
	// (the zero value for every protocol) mean "not yet initialized",
	// matching the pre-startLiveIngestion state.
	protocolCursors *protocolCursorSnapshot
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a
	// ledger). This is CPU-bound XDR decode/processing work, not RPC-bound, so it's sized off
	// NumCPU rather than an RPC batch size; 2x gives headroom for goroutines blocked on the
	// occasional DB lookup without letting the pool grow unbounded (pond.NewPool(0) is
	// unbounded).
	ledgerIndexerPool := pond.NewPool(2 * runtime.NumCPU())
	cfg.Metrics.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	// Create backfill pool with bounded size to control memory usage.
	// Default to NumCPU if not specified.
	backfillWorkers := cfg.BackfillWorkers
	if backfillWorkers <= 0 {
		backfillWorkers = runtime.NumCPU()
	}
	backfillPool := pond.NewPool(backfillWorkers)
	cfg.Metrics.RegisterPoolMetrics("backfill", backfillPool)

	// Build protocol processor map from slice
	for i, p := range cfg.ProtocolProcessors {
		if p == nil {
			return nil, fmt.Errorf("protocol processor at index %d is nil", i)
		}
	}
	ppMap, err := utils.BuildMap(cfg.ProtocolProcessors, func(p ProtocolProcessor) string {
		return p.ProtocolID()
	})
	if err != nil {
		return nil, fmt.Errorf("building protocol processor map: %w", err)
	}

	ledgerIndexer, err := indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.Metrics.Ingestion)
	if err != nil {
		return nil, fmt.Errorf("creating ledger indexer: %w", err)
	}

	return &ingestService{
		ingestionMode:             cfg.IngestionMode,
		models:                    cfg.Models,
		oldestLedgerCursorName:    cfg.OldestLedgerCursorName,
		advisoryLockID:            generateAdvisoryLockID(cfg.Network),
		appTracker:                cfg.AppTracker,
		rpcService:                cfg.RPCService,
		ledgerBackend:             cfg.LedgerBackend,
		ledgerBackendFactory:      cfg.LedgerBackendFactory,
		isPermanentFetchError:     cfg.IsPermanentFetchError,
		tokenIngestionService:     cfg.TokenIngestionService,
		checkpointService:         cfg.CheckpointService,
		appMetrics:                cfg.Metrics,
		networkPassphrase:         cfg.NetworkPassphrase,
		getLedgersLimit:           cfg.GetLedgersLimit,
		ledgerIndexer:             ledgerIndexer,
		ledgerIndexerPool:         ledgerIndexerPool,
		contractMetadataService:   cfg.ContractMetadataService,
		protocolValidators:        cfg.ProtocolValidators,
		wasmSpecExtractor:         cfg.WasmSpecExtractor,
		archive:                   cfg.Archive,
		backfillPool:              backfillPool,
		backfillBatchSize:         uint32(cfg.BackfillBatchSize),
		backfillDBInsertBatchSize: uint32(cfg.BackfillDBInsertBatchSize),
		knownContractIDs:          set.NewSet[string](),
		protocolProcessors:        ppMap,
		protocolCursors: &protocolCursorSnapshot{
			historyExists:      make(map[string]bool, len(ppMap)),
			currentStateExists: make(map[string]bool, len(ppMap)),
		},
	}, nil
}

// Run starts the ingestion service in the configured mode (live or backfill).
// For live mode, startLedger and endLedger are ignored and ingestion runs continuously from the last checkpoint.
// For backfill mode, processes ledgers in the range [startLedger, endLedger].
func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
	switch m.ingestionMode {
	case IngestionModeLive:
		return m.startLiveIngestion(ctx)
	case IngestionModeBackfill:
		return m.startBackfilling(ctx, startLedger, endLedger)
	default:
		return fmt.Errorf("unsupported ingestion mode %q, must be %q or %q", m.ingestionMode, IngestionModeLive, IngestionModeBackfill)
	}
}

// Close stops the worker pools this service owns: the ledger-indexer pool, the
// backfill pool, and any protocol validator that owns resources (e.g. the
// SEP-41 validator's metadata-fetch pool). It must be called after Run has
// returned — when nothing is being indexed or backfilled — and before the
// shared DB pool closes, since a draining task may still issue DB work.
func (m *ingestService) Close() {
	m.ledgerIndexerPool.StopAndWait()
	m.backfillPool.StopAndWait()
	for _, v := range m.protocolValidators {
		if closer, ok := v.(interface{ Close() }); ok {
			closer.Close()
		}
	}
}

// processLedger processes a single ledger - gets the transactions and processes them using indexer processors.
func (m *ingestService) processLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	participantCount, err := indexer.ProcessLedger(ctx, m.networkPassphrase, ledgerMeta, m.ledgerIndexer, buffer)
	if err != nil {
		return fmt.Errorf("processing ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}
	m.appMetrics.Ingestion.ParticipantsCount.Observe(float64(participantCount))
	return nil
}

// insertIntoDB persists the processed data from the buffer to the database.
func (m *ingestService) insertIntoDB(ctx context.Context, dbTx pgx.Tx, buffer indexer.IndexerBufferInterface) (int, int, error) {
	txs := buffer.GetTransactions()
	txParticipants := buffer.GetTransactionsParticipants()
	ops := buffer.GetOperations()
	opParticipants := buffer.GetOperationsParticipants()
	stateChanges := buffer.GetStateChanges()

	if err := m.insertTransactions(ctx, dbTx, txs, txParticipants); err != nil {
		return 0, 0, err
	}
	if err := m.insertOperations(ctx, dbTx, ops, opParticipants); err != nil {
		return 0, 0, err
	}
	if err := m.insertStateChanges(ctx, dbTx, stateChanges); err != nil {
		return 0, 0, err
	}
	log.Ctx(ctx).Debugf("✅ inserted %d txs, %d ops, %d state_changes", len(txs), len(ops), len(stateChanges))
	return len(txs), len(ops), nil
}

// insertTransactions batch inserts transactions with their participants into the database.
func (m *ingestService) insertTransactions(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction, stellarAddressesByToID map[int64]map[string]struct{}) error {
	if len(txs) == 0 {
		return nil
	}
	_, err := m.models.Transactions.BatchCopy(ctx, pgxTx, txs, stellarAddressesByToID)
	if err != nil {
		return fmt.Errorf("batch inserting transactions: %w", err)
	}
	return nil
}

// insertOperations batch inserts operations with their participants into the database.
func (m *ingestService) insertOperations(ctx context.Context, pgxTx pgx.Tx, ops []*types.Operation, stellarAddressesByOpID map[int64]map[string]struct{}) error {
	if len(ops) == 0 {
		return nil
	}
	_, err := m.models.Operations.BatchCopy(ctx, pgxTx, ops, stellarAddressesByOpID)
	if err != nil {
		return fmt.Errorf("batch inserting operations: %w", err)
	}
	return nil
}

// insertStateChanges batch inserts state changes and records metrics.
func (m *ingestService) insertStateChanges(ctx context.Context, pgxTx pgx.Tx, stateChanges []types.StateChange) error {
	if len(stateChanges) == 0 {
		return nil
	}
	_, err := m.models.StateChanges.BatchCopy(ctx, pgxTx, stateChanges)
	if err != nil {
		return fmt.Errorf("batch inserting state changes: %w", err)
	}
	m.recordStateChangeMetrics(stateChanges)
	return nil
}

// recordStateChangeMetrics aggregates state changes by reason and category, then records metrics.
func (m *ingestService) recordStateChangeMetrics(stateChanges []types.StateChange) {
	counts := make(map[string]int) // key: "reason|category"
	for _, sc := range stateChanges {
		reason := string(sc.StateChangeReason)
		key := reason + "|" + string(sc.StateChangeCategory)
		counts[key]++
	}
	for key, count := range counts {
		parts := strings.SplitN(key, "|", 2)
		m.appMetrics.Ingestion.StateChangesTotal.WithLabelValues(parts[0], parts[1]).Add(float64(count))
	}
}
