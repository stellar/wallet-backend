package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime"
	"strings"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

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
	ChannelAccountStore     store.ChannelAccountStore
	TokenIngestionService   TokenIngestionService
	ContractMetadataService ContractMetadataService

	// === Processing Options ===
	GetLedgersLimit int

	// === Backfill Tuning ===
	BackfillProcessWorkers    int // Stage 2 process workers (default: NumCPU)
	BackfillFlushWorkers      int // Stage 3 flush workers (default: 4)
	BackfillDBInsertBatchSize int // Ledgers per flush batch (default: 100)
	BackfillLedgerChanSize    int // ledgerCh buffer size (default: 256)
	BackfillFlushChanSize     int // flushCh buffer size (default: 8)
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
	ingestionMode           string
	models                  *data.Models
	latestLedgerCursorName  string
	oldestLedgerCursorName  string
	advisoryLockID          int
	appTracker              apptracker.AppTracker
	rpcService              RPCService
	ledgerBackend           ledgerbackend.LedgerBackend
	ledgerBackendFactory    LedgerBackendFactory
	chAccStore              store.ChannelAccountStore
	tokenIngestionService   TokenIngestionService
	contractMetadataService ContractMetadataService
	appMetrics              *metrics.Metrics
	networkPassphrase       string
	getLedgersLimit         int
	ledgerIndexer           *indexer.Indexer
	archive                 historyarchive.ArchiveInterface
	backfillProcessWorkers  int
	backfillFlushWorkers    int
	backfillFlushBatchSize  uint32
	backfillLedgerChanSize  int
	backfillFlushChanSize   int
	knownContractIDs        set.Set[string]
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger).
	// Bounded to NumCPU to avoid goroutine oversubscription on busy ledgers.
	ledgerIndexerPool := pond.NewPool(runtime.NumCPU())
	cfg.Metrics.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	// Backfill pipeline defaults — only process workers needs a runtime default (NumCPU).
	// All other defaults are set via CLI flag defaults in cmd/ingest.go.
	processWorkers := cfg.BackfillProcessWorkers
	if processWorkers <= 0 {
		processWorkers = runtime.NumCPU()
	}

	// Validate connection pool can handle flush worker concurrency.
	// Each flush worker runs 5 parallel COPYs, each needing its own connection.
	if cfg.IngestionMode == IngestionModeBackfill && cfg.BackfillFlushWorkers > 0 {
		requiredConns := int32(cfg.BackfillFlushWorkers*5 + 5) // +5 headroom for cursor updates
		maxConns := cfg.Models.DB.Config().MaxConns
		if maxConns > 0 && maxConns < requiredConns {
			return nil, fmt.Errorf(
				"pgxpool max connections (%d) too low for %d flush workers (need at least %d: %d workers × 5 COPYs + 5 headroom)",
				maxConns, cfg.BackfillFlushWorkers, requiredConns, cfg.BackfillFlushWorkers)
		}
	}

	return &ingestService{
		ingestionMode:           cfg.IngestionMode,
		models:                  cfg.Models,
		latestLedgerCursorName:  cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:  cfg.OldestLedgerCursorName,
		advisoryLockID:          generateAdvisoryLockID(cfg.Network),
		appTracker:              cfg.AppTracker,
		rpcService:              cfg.RPCService,
		ledgerBackend:           cfg.LedgerBackend,
		ledgerBackendFactory:    cfg.LedgerBackendFactory,
		chAccStore:              cfg.ChannelAccountStore,
		tokenIngestionService:   cfg.TokenIngestionService,
		contractMetadataService: cfg.ContractMetadataService,
		appMetrics:              cfg.Metrics,
		networkPassphrase:       cfg.NetworkPassphrase,
		getLedgersLimit:         cfg.GetLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.Metrics.Ingestion),
		archive:                 cfg.Archive,
		backfillProcessWorkers:  processWorkers,
		backfillFlushWorkers:    cfg.BackfillFlushWorkers,
		backfillFlushBatchSize:  uint32(cfg.BackfillDBInsertBatchSize),
		backfillLedgerChanSize:  cfg.BackfillLedgerChanSize,
		backfillFlushChanSize:   cfg.BackfillFlushChanSize,
		knownContractIDs:        set.NewSet[string](),
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

// processLedgerSequential processes a single ledger without the pond pool.
// Used by backfill where multiple process workers already provide parallelism.
func (m *ingestService) processLedgerSequential(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	participantCount, err := indexer.ProcessLedgerSequential(ctx, m.networkPassphrase, ledgerMeta, m.ledgerIndexer, buffer)
	if err != nil {
		return fmt.Errorf("processing ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}
	m.appMetrics.Ingestion.ParticipantsCount.Observe(float64(participantCount))
	return nil
}

// insertAndUpsertParallel runs parallel goroutines via errgroup: 5 COPY operations (transactions,
// transactions_accounts, operations, operations_accounts, state_changes) plus, in live mode only,
// 4 balance upserts (trustline, native, SAC, account-contract tokens). Balance upserts are
// skipped in backfill mode since they represent current state, not historical data.
// Each goroutine acquires its own pool connection. UniqueViolation errors are treated as
// success for idempotent retry.
func (m *ingestService) insertAndUpsertParallel(ctx context.Context, txs []*types.Transaction, buffer indexer.IndexerBufferInterface) (int, int, error) {
	txParticipants := buffer.GetTransactionsParticipants()
	ops := buffer.GetOperations()
	opParticipants := buffer.GetOperationsParticipants()
	stateChanges := buffer.GetStateChanges()

	g, gCtx := errgroup.WithContext(ctx)

	// 5 COPY goroutines
	g.Go(func() error {
		return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := m.models.Transactions.BatchCopy(ctx, tx, txs); err != nil {
				return fmt.Errorf("copying transactions: %w", err)
			}
			return nil
		})
	})
	g.Go(func() error {
		return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
			return m.models.Transactions.BatchCopyAccounts(ctx, tx, txs, txParticipants)
		})
	})
	g.Go(func() error {
		return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := m.models.Operations.BatchCopy(ctx, tx, ops); err != nil {
				return fmt.Errorf("copying operations: %w", err)
			}
			return nil
		})
	})
	g.Go(func() error {
		return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
			return m.models.Operations.BatchCopyAccounts(ctx, tx, ops, opParticipants)
		})
	})
	g.Go(func() error {
		return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := m.models.StateChanges.BatchCopy(ctx, tx, stateChanges); err != nil {
				return fmt.Errorf("copying state changes: %w", err)
			}
			return nil
		})
	})

	// 4 upsert goroutines — skipped in backfill mode (balance tables represent current state)
	if m.ingestionMode != IngestionModeBackfill {
		g.Go(func() error {
			return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
				return m.tokenIngestionService.ProcessTrustlineChanges(ctx, tx, buffer.GetTrustlineChanges())
			})
		})
		g.Go(func() error {
			return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
				return m.tokenIngestionService.ProcessNativeBalanceChanges(ctx, tx, buffer.GetAccountChanges())
			})
		})
		g.Go(func() error {
			return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
				return m.tokenIngestionService.ProcessSACBalanceChanges(ctx, tx, buffer.GetSACBalanceChanges())
			})
		})
		g.Go(func() error {
			return m.copyWithPoolConn(gCtx, func(ctx context.Context, tx pgx.Tx) error {
				return m.tokenIngestionService.ProcessContractTokenChanges(ctx, tx, buffer.GetContractChanges())
			})
		})
	}

	if err := g.Wait(); err != nil {
		return 0, 0, fmt.Errorf("parallel insert/upsert: %w", err)
	}

	m.recordStateChangeMetrics(stateChanges)
	log.Ctx(ctx).Infof("✅ inserted %d txs, %d ops, %d state_changes", len(txs), len(ops), len(stateChanges))
	return len(txs), len(ops), nil
}

// copyWithPoolConn acquires a connection from the pool, begins a transaction, runs fn,
// and commits. On UniqueViolation the insert is idempotent (prior partial insert) so we
// return nil.
func (m *ingestService) copyWithPoolConn(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	conn, err := m.models.DB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring pool connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Ingestion is idempotent (replayed from cursor on crash), so WAL fsync durability
	// is unnecessary. This eliminates fsync latency from each of the 9 parallel commits.
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = off"); err != nil {
		return fmt.Errorf("setting synchronous_commit=off: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		if isUniqueViolation(err) {
			return nil
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing copy transaction: %w", err)
	}
	return nil
}

// isUniqueViolation checks if an error is a PostgreSQL unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
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
