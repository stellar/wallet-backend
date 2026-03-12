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
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
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
	GetLedgersLimit int
	SkipTxMeta      bool
	SkipTxEnvelope  bool

	// === Backfill Tuning ===
	BackfillWorkers           int
	BackfillBatchSize         int
	BackfillDBInsertBatchSize int
	CatchupThreshold          int
	ChunkInterval             string
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
	chAccStore                store.ChannelAccountStore
	tokenIngestionService     TokenIngestionService
	contractMetadataService   ContractMetadataService
	metricsService            metrics.MetricsService
	networkPassphrase         string
	getLedgersLimit           int
	ledgerIndexer             *indexer.Indexer
	archive                   historyarchive.ArchiveInterface
	backfillPool              pond.Pool
	backfillDBInsertBatchSize uint32
	catchupThreshold          uint32
	chunkInterval             string
	knownContractIDs          types.StringSet
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
		ingestionMode:             cfg.IngestionMode,
		models:                    cfg.Models,
		latestLedgerCursorName:    cfg.LatestLedgerCursorName,
		oldestLedgerCursorName:    cfg.OldestLedgerCursorName,
		advisoryLockID:            generateAdvisoryLockID(cfg.Network),
		appTracker:                cfg.AppTracker,
		rpcService:                cfg.RPCService,
		ledgerBackend:             cfg.LedgerBackend,
		ledgerBackendFactory:      cfg.LedgerBackendFactory,
		chAccStore:                cfg.ChannelAccountStore,
		tokenIngestionService:     cfg.TokenIngestionService,
		contractMetadataService:   cfg.ContractMetadataService,
		metricsService:            cfg.MetricsService,
		networkPassphrase:         cfg.NetworkPassphrase,
		getLedgersLimit:           cfg.GetLedgersLimit,
		ledgerIndexer:             indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.MetricsService, cfg.SkipTxMeta, cfg.SkipTxEnvelope),
		archive:                   cfg.Archive,
		backfillPool:              backfillPool,
		backfillDBInsertBatchSize: uint32(cfg.BackfillDBInsertBatchSize),
		catchupThreshold:          uint32(cfg.CatchupThreshold),
		chunkInterval:             cfg.ChunkInterval,
		knownContractIDs:          types.NewStringSet(),
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
		return m.startHistoricalBackfill(ctx, startLedger, endLedger)
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

// processLedgerSequential processes a single ledger without tx-level parallelism.
// Used by the pipelined backfill where ledger-level parallelism replaces tx-level parallelism.
func (m *ingestService) processLedgerSequential(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta, buffer *indexer.IndexerBuffer) error {
	participantCount, err := indexer.ProcessLedgerSequential(ctx, m.networkPassphrase, ledgerMeta, m.ledgerIndexer, buffer)
	if err != nil {
		return fmt.Errorf("processing ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}
	m.metricsService.ObserveIngestionParticipantsCount(participantCount)
	return nil
}

// setLocalBackfillOpts sets transaction-local options for backfill flushes:
// - synchronous_commit=off: skip waiting for WAL flush (data is re-derivable from ledger)
// - session_replication_role=replica: skip CHECK constraints and triggers (trusted ledger data)
func setLocalBackfillOpts(ctx context.Context, dbTx pgx.Tx) error {
	if _, err := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); err != nil {
		return fmt.Errorf("setting synchronous_commit=off: %w", err)
	}
	if _, err := dbTx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
		return fmt.Errorf("setting session_replication_role=replica: %w", err)
	}
	if _, err := dbTx.Exec(ctx, "SET LOCAL statement_timeout = '30s'"); err != nil {
		return fmt.Errorf("setting statement_timeout: %w", err)
	}
	return nil
}

// isUniqueViolation returns true if the error is a PostgreSQL unique_violation (23505).
// Used to make parallel COPY idempotent on retry: if a group was already committed
// in a previous partial flush, the COPY will fail with a unique violation on the
// duplicate rows, and we can safely skip it.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// insertOpts configures behavior for insertIntoDB.
type insertOpts struct {
	backfillMode bool // sets synchronous_commit=off, session_replication_role=replica
}

// insertIntoDB persists the processed data from the buffer to the database.
// Uses 5 independent goroutines (one per table) to maximize COPY throughput.
// This is safe because there are no foreign keys between these tables.
func (m *ingestService) insertIntoDB(ctx context.Context, buffer indexer.IndexerBufferInterface, opts insertOpts) (int, int, error) {
	txs := buffer.GetTransactions()
	txParticipants := buffer.GetTransactionsParticipants()
	ops := buffer.GetOperations()
	opParticipants := buffer.GetOperationsParticipants()
	stateChanges := buffer.GetStateChanges()

	g, dbCtx := errgroup.WithContext(ctx)

	// 1. transactions
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Transactions.BatchCopy(dbCtx, dbTx, txs)
			if err != nil {
				return fmt.Errorf("copying transactions: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping transactions: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying transactions tx: %w", err)
		}
		return nil
	})

	// 2. transactions_accounts
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Transactions.BatchCopyParticipants(dbCtx, dbTx, txs, txParticipants)
			if err != nil {
				return fmt.Errorf("copying transactions_accounts: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping transactions_accounts: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying transactions_accounts tx: %w", err)
		}
		return nil
	})

	// 3. operations
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Operations.BatchCopy(dbCtx, dbTx, ops)
			if err != nil {
				return fmt.Errorf("copying operations: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping operations: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying operations tx: %w", err)
		}
		return nil
	})

	// 4. operations_accounts
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Operations.BatchCopyParticipants(dbCtx, dbTx, ops, opParticipants)
			if err != nil {
				return fmt.Errorf("copying operations_accounts: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping operations_accounts: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying operations_accounts tx: %w", err)
		}
		return nil
	})

	// 5. state_changes
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.StateChanges.BatchCopy(dbCtx, dbTx, stateChanges)
			if err != nil {
				return fmt.Errorf("copying state_changes: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping state_changes: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying state_changes tx: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return 0, 0, fmt.Errorf("inserting data into db: %w", err)
	}
	m.recordStateChangeMetrics(stateChanges)
	log.Ctx(ctx).Infof("✅ inserted %d txs, %d ops, %d state_changes", len(txs), len(ops), len(stateChanges))
	return len(txs), len(ops), nil
}

// insertBatchIntoDB concatenates data from multiple IndexerBuffers and persists to the database.
// Same 5-goroutine errgroup pattern as insertIntoDB, but avoids expensive buffer merging by
// concatenating cheap pointer slices and doing trivial map assignments (no set cloning needed
// since operation/transaction IDs don't overlap across ledgers).
func (m *ingestService) insertBatchIntoDB(ctx context.Context, buffers []*indexer.IndexerBuffer, opts insertOpts) (int, int, error) {
	// Concatenate all buffer data — cheap pointer slice appends, no data copying.
	var allTxs []*types.Transaction
	var allOps []*types.Operation
	var allStateChanges []types.StateChange
	allTxParticipants := make(map[int64]types.StringSet)
	allOpParticipants := make(map[int64]types.StringSet)

	for _, buf := range buffers {
		allTxs = append(allTxs, buf.GetTransactions()...)
		allOps = append(allOps, buf.GetOperations()...)
		allStateChanges = append(allStateChanges, buf.GetStateChanges()...)
		// No overlap across ledgers — simple map assignment, no set union needed.
		for id, addrs := range buf.GetTransactionsParticipants() {
			allTxParticipants[id] = addrs
		}
		for id, addrs := range buf.GetOperationsParticipants() {
			allOpParticipants[id] = addrs
		}
	}

	g, dbCtx := errgroup.WithContext(ctx)

	// 1. transactions
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Transactions.BatchCopy(dbCtx, dbTx, allTxs)
			if err != nil {
				return fmt.Errorf("copying transactions: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping transactions: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying transactions tx: %w", err)
		}
		return nil
	})

	// 2. transactions_accounts
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Transactions.BatchCopyParticipants(dbCtx, dbTx, allTxs, allTxParticipants)
			if err != nil {
				return fmt.Errorf("copying transactions_accounts: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping transactions_accounts: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying transactions_accounts tx: %w", err)
		}
		return nil
	})

	// 3. operations
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Operations.BatchCopy(dbCtx, dbTx, allOps)
			if err != nil {
				return fmt.Errorf("copying operations: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping operations: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying operations tx: %w", err)
		}
		return nil
	})

	// 4. operations_accounts
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.Operations.BatchCopyParticipants(dbCtx, dbTx, allOps, allOpParticipants)
			if err != nil {
				return fmt.Errorf("copying operations_accounts: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping operations_accounts: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying operations_accounts tx: %w", err)
		}
		return nil
	})

	// 5. state_changes
	g.Go(func() error {
		err := db.RunInTransaction(dbCtx, m.models.DB, func(dbTx pgx.Tx) error {
			if opts.backfillMode {
				if err := setLocalBackfillOpts(dbCtx, dbTx); err != nil {
					return err
				}
			}
			_, err := m.models.StateChanges.BatchCopy(dbCtx, dbTx, allStateChanges)
			if err != nil {
				return fmt.Errorf("copying state_changes: %w", err)
			}
			return nil
		})
		if isUniqueViolation(err) {
			log.Ctx(ctx).Infof("Skipping state_changes: data already committed (unique violation)")
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying state_changes tx: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return 0, 0, fmt.Errorf("inserting batch data into db: %w", err)
	}
	m.recordStateChangeMetrics(allStateChanges)
	log.Ctx(ctx).Infof("✅ inserted %d txs, %d ops, %d state_changes (from %d buffers)", len(allTxs), len(allOps), len(allStateChanges), len(buffers))
	return len(allTxs), len(allOps), nil
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
