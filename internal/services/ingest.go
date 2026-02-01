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
	"github.com/stellar/wallet-backend/internal/signing/store"
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
	IngestionMode  string
	Models         *data.Models
	AppTracker     apptracker.AppTracker
	MetricsService metrics.MetricsService

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
	ChannelAccountStore     store.ChannelAccountStore
	TokenIngestionService   TokenIngestionService
	ContractMetadataService ContractMetadataService

	// === Processing Options ===
	GetLedgersLimit            int
	SkipTxMeta                 bool
	SkipTxEnvelope             bool
	EnableParticipantFiltering bool

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
	ingestionMode              string
	models                     *data.Models
	latestLedgerCursorName     string
	oldestLedgerCursorName     string
	advisoryLockID             int
	appTracker                 apptracker.AppTracker
	rpcService                 RPCService
	ledgerBackend              ledgerbackend.LedgerBackend
	ledgerBackendFactory       LedgerBackendFactory
	chAccStore                 store.ChannelAccountStore
	tokenIngestionService      TokenIngestionService
	contractMetadataService    ContractMetadataService
	metricsService             metrics.MetricsService
	networkPassphrase          string
	getLedgersLimit            int
	ledgerIndexer              *indexer.Indexer
	archive                    historyarchive.ArchiveInterface
	enableParticipantFiltering bool
	backfillPool               pond.Pool
	backfillBatchSize          uint32
	backfillDBInsertBatchSize  uint32
	catchupThreshold           uint32
	knownContractIDs           set.Set[string]
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)
	cfg.MetricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	// Create backfill pool with bounded size to control memory usage.
	// Default to NumCPU if not specified.
	backfillWorkers := cfg.BackfillWorkers
	if backfillWorkers <= 0 {
		backfillWorkers = runtime.NumCPU()
	}
	backfillPool := pond.NewPool(backfillWorkers)
	cfg.MetricsService.RegisterPoolMetrics("backfill", backfillPool)

	return &ingestService{
		ingestionMode:              cfg.IngestionMode,
		models:                     cfg.Models,
		latestLedgerCursorName:     cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:     cfg.OldestLedgerCursorName,
		advisoryLockID:             generateAdvisoryLockID(cfg.Network),
		appTracker:                 cfg.AppTracker,
		rpcService:                 cfg.RPCService,
		ledgerBackend:              cfg.LedgerBackend,
		ledgerBackendFactory:       cfg.LedgerBackendFactory,
		chAccStore:                 cfg.ChannelAccountStore,
		tokenIngestionService:      cfg.TokenIngestionService,
		contractMetadataService:    cfg.ContractMetadataService,
		metricsService:             cfg.MetricsService,
		networkPassphrase:          cfg.NetworkPassphrase,
		getLedgersLimit:            cfg.GetLedgersLimit,
		ledgerIndexer:              indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.MetricsService, cfg.SkipTxMeta, cfg.SkipTxEnvelope),
		archive:                    cfg.Archive,
		enableParticipantFiltering: cfg.EnableParticipantFiltering,
		backfillPool:               backfillPool,
		backfillBatchSize:          uint32(cfg.BackfillBatchSize),
		backfillDBInsertBatchSize:  uint32(cfg.BackfillDBInsertBatchSize),
		catchupThreshold:           uint32(cfg.CatchupThreshold),
		knownContractIDs:           set.NewSet[string](),
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
func (m *ingestService) getLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
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
	return xdr.LedgerCloseMeta{}, fmt.Errorf("failed after %d attempts: %w", maxLedgerFetchRetries, lastErr)
}

// processLedger processes a single ledger - gets the transactions and processes them using indexer processors.
func (m *ingestService) processLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	participantCount, err := indexer.ProcessLedger(ctx, m.networkPassphrase, ledgerMeta, m.ledgerIndexer, buffer)
	if err != nil {
		return fmt.Errorf("processing ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}
	m.metricsService.ObserveIngestionParticipantsCount(participantCount)
	return nil
}

// filteredIngestionData holds the filtered transaction, operation, and state change
// data ready for database insertion after participant filtering.
type filteredIngestionData struct {
	txs            []*types.Transaction
	txParticipants map[int64]set.Set[string]
	ops            []*types.Operation
	opParticipants map[int64]set.Set[string]
	stateChanges   []types.StateChange
}

// hasRegisteredParticipant checks if any participant in the set is registered.
func hasRegisteredParticipant(participants set.Set[string], registered set.Set[string]) bool {
	for p := range participants.Iter() {
		if registered.Contains(p) {
			return true
		}
	}
	return false
}

// filterByRegisteredAccounts filters ingestion data to only include items
// where at least one participant is a registered account.
// If a transaction/operation has ANY registered participant, it is included with ALL its participants.
func (m *ingestService) filterByRegisteredAccounts(
	ctx context.Context,
	dbTx pgx.Tx,
	txs []*types.Transaction,
	txParticipants map[int64]set.Set[string],
	ops []*types.Operation,
	opParticipants map[int64]set.Set[string],
	stateChanges []types.StateChange,
	allParticipants []string,
) (*filteredIngestionData, error) {
	// Get registered accounts from DB
	existing, err := m.models.Account.BatchGetByIDs(ctx, dbTx, allParticipants)
	if err != nil {
		return nil, fmt.Errorf("getting registered accounts: %w", err)
	}
	registeredAccounts := set.NewSet(existing...)

	log.Ctx(ctx).Infof("filtering enabled: %d/%d participants are registered", len(existing), len(allParticipants))

	// Filter transactions: include if ANY participant is registered
	toIDsToInclude := set.NewSet[int64]()
	for toID, participants := range txParticipants {
		if hasRegisteredParticipant(participants, registeredAccounts) {
			toIDsToInclude.Add(toID)
		}
	}

	filteredTxs := make([]*types.Transaction, 0, toIDsToInclude.Cardinality())
	filteredTxParticipants := make(map[int64]set.Set[string])
	for _, tx := range txs {
		if toIDsToInclude.Contains(tx.ToID) {
			filteredTxs = append(filteredTxs, tx)
			filteredTxParticipants[tx.ToID] = txParticipants[tx.ToID]
		}
	}

	// Filter operations: include if ANY participant is registered
	opIDsToInclude := set.NewSet[int64]()
	for opID, participants := range opParticipants {
		if hasRegisteredParticipant(participants, registeredAccounts) {
			opIDsToInclude.Add(opID)
		}
	}

	filteredOps := make([]*types.Operation, 0, opIDsToInclude.Cardinality())
	filteredOpParticipants := make(map[int64]set.Set[string])
	for _, op := range ops {
		if opIDsToInclude.Contains(op.ID) {
			filteredOps = append(filteredOps, op)
			filteredOpParticipants[op.ID] = opParticipants[op.ID]
		}
	}

	// Filter state changes: include if account is registered
	filteredSC := make([]types.StateChange, 0)
	for _, sc := range stateChanges {
		if registeredAccounts.Contains(sc.AccountID) {
			filteredSC = append(filteredSC, sc)
		}
	}

	log.Ctx(ctx).Infof("after filtering: %d txs, %d ops, %d state_changes",
		len(filteredTxs), len(filteredOps), len(filteredSC))

	return &filteredIngestionData{
		txs:            filteredTxs,
		txParticipants: filteredTxParticipants,
		ops:            filteredOps,
		opParticipants: filteredOpParticipants,
		stateChanges:   filteredSC,
	}, nil
}

// filterParticipantData filters the data to only include participants that are registered
func (m *ingestService) filterParticipantData(ctx context.Context, dbTx pgx.Tx, indexerBuffer indexer.IndexerBufferInterface) (*filteredIngestionData, error) {
	// Get data from indexer buffer
	txs := indexerBuffer.GetTransactions()
	txParticipants := indexerBuffer.GetTransactionsParticipants()
	ops := indexerBuffer.GetOperations()
	opParticipants := indexerBuffer.GetOperationsParticipants()
	stateChanges := indexerBuffer.GetStateChanges()

	// When filtering is enabled, only store data for registered accounts
	if m.enableParticipantFiltering {
		filtered, err := m.filterByRegisteredAccounts(
			ctx, dbTx, txs, txParticipants, ops, opParticipants, stateChanges,
			indexerBuffer.GetAllParticipants(),
		)
		if err != nil {
			return nil, fmt.Errorf("filtering by registered accounts: %w", err)
		}
		return filtered, nil
	}

	return &filteredIngestionData{
		txs:            txs,
		txParticipants: txParticipants,
		ops:            ops,
		opParticipants: opParticipants,
		stateChanges:   stateChanges,
	}, nil
}

// insertIntoDB persists the processed data to the database.
func (m *ingestService) insertIntoDB(ctx context.Context, dbTx pgx.Tx, data *filteredIngestionData) error {
	if err := m.insertTransactions(ctx, dbTx, data.txs, data.txParticipants); err != nil {
		return err
	}
	if err := m.insertOperations(ctx, dbTx, data.ops, data.opParticipants); err != nil {
		return err
	}
	if err := m.insertStateChanges(ctx, dbTx, data.stateChanges); err != nil {
		return err
	}
	log.Ctx(ctx).Infof("✅ inserted %d txs, %d ops, %d state_changes", len(data.txs), len(data.ops), len(data.stateChanges))
	return nil
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
		reason := ""
		if sc.StateChangeReason != nil {
			reason = string(*sc.StateChangeReason)
		}
		key := reason + "|" + string(sc.StateChangeCategory)
		counts[key]++
	}
	for key, count := range counts {
		parts := strings.SplitN(key, "|", 2)
		m.metricsService.IncStateChanges(parts[0], parts[1], count)
	}
}
