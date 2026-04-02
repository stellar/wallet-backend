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
)

const (
	// maxLedgerFetchRetries is the maximum number of retry attempts when fetching a ledger fails.
	maxLedgerFetchRetries = 10
	// maxRetryBackoff is the maximum backoff duration between retry attempts.
	maxRetryBackoff = 30 * time.Second
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

	// === Cursors ===
	LatestLedgerCursorName string
	OldestLedgerCursorName string

	// === Live Mode Dependencies ===
	TokenIngestionService TokenIngestionService
	CheckpointService     CheckpointService

	// === Protocol Processors ===
	ProtocolProcessors []ProtocolProcessor // nil means no protocol state production

	// === Processing Options ===
	GetLedgersLimit int

	// === Backfill Tuning ===
	BackfillWorkers           int
	BackfillBatchSize         int
	BackfillDBInsertBatchSize int
	CatchupThreshold          int
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
	// PersistLedgerData persists processed ledger data to the database in a single atomic transaction.
	// This is the shared core used by both live ingestion and loadtest.
	// Returns the number of transactions and operations persisted.
	PersistLedgerData(ctx context.Context, ledgerSeq uint32, buffer *indexer.IndexerBuffer, cursorName string) (int, int, error)
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	ingestionMode             string
	models                    *data.Models
	latestLedgerCursorName    string
	oldestLedgerCursorName    string
	advisoryLockID            int
	appTracker                apptracker.AppTracker
	rpcService                RPCService
	ledgerBackend             ledgerbackend.LedgerBackend
	ledgerBackendFactory      LedgerBackendFactory
	tokenIngestionService     TokenIngestionService
	checkpointService         CheckpointService
	appMetrics                *metrics.Metrics
	networkPassphrase         string
	getLedgersLimit           int
	ledgerIndexer             *indexer.Indexer
	archive                   historyarchive.ArchiveInterface
	backfillPool              pond.Pool
	backfillBatchSize         uint32
	backfillDBInsertBatchSize uint32
	catchupThreshold          uint32
	knownContractIDs          set.Set[string]
	protocolProcessors        map[string]ProtocolProcessor
	protocolContractCache     *protocolContractCache
	// eligibleProtocolProcessors is set by ingestLiveLedgers before each call
	// to PersistLedgerData, scoping the CAS loop to only processors that had
	// ProcessLedger called. Only accessed from the single-threaded live ingestion loop.
	eligibleProtocolProcessors map[string]ProtocolProcessor
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)
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
	ppMap := make(map[string]ProtocolProcessor, len(cfg.ProtocolProcessors))
	for i, p := range cfg.ProtocolProcessors {
		if p == nil {
			return nil, fmt.Errorf("protocol processor at index %d is nil", i)
		}
		id := p.ProtocolID()
		if _, exists := ppMap[id]; exists {
			return nil, fmt.Errorf("duplicate protocol processor ID %q", id)
		}
		ppMap[id] = p
	}

	var ppCache *protocolContractCache
	if len(ppMap) > 0 {
		ppCache = &protocolContractCache{
			contractsByProtocol: make(map[string][]data.ProtocolContracts),
		}
	}

	return &ingestService{
		ingestionMode:             cfg.IngestionMode,
		models:                    cfg.Models,
		latestLedgerCursorName:    cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:    cfg.OldestLedgerCursorName,
		advisoryLockID:            generateAdvisoryLockID(cfg.Network),
		appTracker:                cfg.AppTracker,
		rpcService:                cfg.RPCService,
		ledgerBackend:             cfg.LedgerBackend,
		ledgerBackendFactory:      cfg.LedgerBackendFactory,
		tokenIngestionService:     cfg.TokenIngestionService,
		checkpointService:         cfg.CheckpointService,
		appMetrics:                cfg.Metrics,
		networkPassphrase:         cfg.NetworkPassphrase,
		getLedgersLimit:           cfg.GetLedgersLimit,
		ledgerIndexer:             indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.Metrics.Ingestion),
		archive:                   cfg.Archive,
		backfillPool:              backfillPool,
		backfillBatchSize:         uint32(cfg.BackfillBatchSize),
		backfillDBInsertBatchSize: uint32(cfg.BackfillDBInsertBatchSize),
		catchupThreshold:          uint32(cfg.CatchupThreshold),
		knownContractIDs:          set.NewSet[string](),
		protocolProcessors:        ppMap,
		protocolContractCache:     ppCache,
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
		return m.startBackfilling(ctx, startLedger, endLedger, BackfillModeHistorical)
	default:
		return fmt.Errorf("unsupported ingestion mode %q, must be %q or %q", m.ingestionMode, IngestionModeLive, IngestionModeBackfill)
	}
}

// getLedgerWithRetry fetches a ledger with exponential backoff retry logic.
// It respects context cancellation and limits retries to maxLedgerFetchRetries attempts.
// LedgerFetchDuration is always observed (including on error/exhaustion paths) to capture full retry latency.
func (m *ingestService) getLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	fetchStart := time.Now()
	defer func() {
		m.appMetrics.Ingestion.LedgerFetchDuration.Observe(time.Since(fetchStart).Seconds())
	}()

	var lastErr error
	for attempt := 0; attempt < maxLedgerFetchRetries; attempt++ {
		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		ledgerMeta, err := backend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return ledgerMeta, nil
		}
		lastErr = err

		m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("ledger_fetch").Inc()

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
			ledgerSeq, attempt+1, maxLedgerFetchRetries, err, backoff)

		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("ledger_fetch").Inc()
	m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("ledger_fetch").Inc()
	return xdr.LedgerCloseMeta{}, fmt.Errorf("failed after %d attempts: %w", maxLedgerFetchRetries, lastErr)
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
	log.Ctx(ctx).Infof("✅ inserted %d txs, %d ops, %d state_changes", len(txs), len(ops), len(stateChanges))
	return len(txs), len(ops), nil
}

// insertTransactions batch inserts transactions with their participants into the database.
func (m *ingestService) insertTransactions(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction, stellarAddressesByToID map[int64]set.Set[string]) error {
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
func (m *ingestService) insertOperations(ctx context.Context, pgxTx pgx.Tx, ops []*types.Operation, stellarAddressesByOpID map[int64]set.Set[string]) error {
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
