package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
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
	AccountTokenService     AccountTokenService
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
	accountTokenService        AccountTokenService
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
		accountTokenService:        cfg.AccountTokenService,
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
	ledgerSeq := ledgerMeta.LedgerSequence()
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	participantCount, err := m.ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionParticipantsCount(participantCount)
	return nil
}

func (m *ingestService) getLedgerTransactions(ctx context.Context, xdrLedgerCloseMeta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.networkPassphrase, xdrLedgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer utils.DeferredClose(ctx, ledgerTxReader, "closing ledger transaction reader")

	transactions := make([]ingest.LedgerTransaction, 0)
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading ledger: %w", err)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// filteredIngestionData holds the filtered transaction, operation, and state change
// data ready for database insertion after participant filtering.
type filteredIngestionData struct {
	txs                  []*types.Transaction
	txParticipants       map[string]set.Set[string]
	ops                  []*types.Operation
	opParticipants       map[int64]set.Set[string]
	stateChanges         []types.StateChange
	trustlineChanges     []types.TrustlineChange
	contractTokenChanges []types.ContractChange
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
	txParticipants map[string]set.Set[string],
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
	txHashesToInclude := set.NewSet[string]()
	for txHash, participants := range txParticipants {
		if hasRegisteredParticipant(participants, registeredAccounts) {
			txHashesToInclude.Add(txHash)
		}
	}

	filteredTxs := make([]*types.Transaction, 0, txHashesToInclude.Cardinality())
	filteredTxParticipants := make(map[string]set.Set[string])
	for _, tx := range txs {
		if txHashesToInclude.Contains(tx.Hash) {
			filteredTxs = append(filteredTxs, tx)
			filteredTxParticipants[tx.Hash] = txParticipants[tx.Hash]
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
	trustlineChanges := indexerBuffer.GetTrustlineChanges()
	contractChanges := indexerBuffer.GetContractChanges()

	// When filtering is enabled, only store data for registered accounts
	if m.enableParticipantFiltering {
		filtered, err := m.filterByRegisteredAccounts(
			ctx, dbTx, txs, txParticipants, ops, opParticipants, stateChanges,
			indexerBuffer.GetAllParticipants(),
		)
		if err != nil {
			return nil, fmt.Errorf("filtering by registered accounts: %w", err)
		}
		filtered.trustlineChanges = trustlineChanges
		filtered.contractTokenChanges = contractChanges
		return filtered, nil
	}

	return &filteredIngestionData{
		txs:                  txs,
		txParticipants:       txParticipants,
		ops:                  ops,
		opParticipants:       opParticipants,
		stateChanges:         stateChanges,
		trustlineChanges:     trustlineChanges,
		contractTokenChanges: contractChanges,
	}, nil
}

// ingestProcessedData persists the processed data to the database.
// When processLiveChanges is true, it also unlocks channel accounts and processes token changes.
func (m *ingestService) ingestProcessedData(ctx context.Context, pgxTx pgx.Tx, data *filteredIngestionData, processLiveChanges bool) error {
	if err := m.insertTransactions(ctx, pgxTx, data.txs, data.txParticipants); err != nil {
		return err
	}
	if err := m.insertOperations(ctx, pgxTx, data.ops, data.opParticipants); err != nil {
		return err
	}
	if err := m.insertStateChanges(ctx, pgxTx, data.stateChanges); err != nil {
		return err
	}

	// Unlock channel accounts and process token changes only during live ingestion
	// This is done within the same pgxTx for atomicity - all inserts and unlocks succeed or fail together
	if processLiveChanges {
		if err := m.unlockChannelAccounts(ctx, pgxTx, data.txs); err != nil {
			return err
		}
		return m.processLiveIngestionTokenChanges(ctx, data.trustlineChanges, data.contractTokenChanges)
	}

	return nil
}

// insertTransactions batch inserts transactions with their participants into the database.
func (m *ingestService) insertTransactions(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction, stellarAddressesByTxHash map[string]set.Set[string]) error {
	if len(txs) == 0 {
		return nil
	}
	insertedCount, err := m.models.Transactions.BatchCopy(ctx, pgxTx, txs, stellarAddressesByTxHash)
	if err != nil {
		return fmt.Errorf("batch inserting transactions: %w", err)
	}
	log.Ctx(ctx).Infof("inserted %d transactions", insertedCount)
	return nil
}

// insertOperations batch inserts operations with their participants into the database.
func (m *ingestService) insertOperations(ctx context.Context, pgxTx pgx.Tx, ops []*types.Operation, stellarAddressesByOpID map[int64]set.Set[string]) error {
	if len(ops) == 0 {
		return nil
	}
	insertedCount, err := m.models.Operations.BatchCopy(ctx, pgxTx, ops, stellarAddressesByOpID)
	if err != nil {
		return fmt.Errorf("batch inserting operations: %w", err)
	}
	log.Ctx(ctx).Infof("inserted %d operations", insertedCount)
	return nil
}

// insertStateChanges batch inserts state changes and records metrics.
func (m *ingestService) insertStateChanges(ctx context.Context, pgxTx pgx.Tx, stateChanges []types.StateChange) error {
	if len(stateChanges) == 0 {
		return nil
	}
	insertedCount, err := m.models.StateChanges.BatchCopy(ctx, pgxTx, stateChanges)
	if err != nil {
		return fmt.Errorf("batch inserting state changes: %w", err)
	}
	m.recordStateChangeMetrics(stateChanges)
	log.Ctx(ctx).Infof("inserted %d state changes", insertedCount)
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

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	innerTxHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		innerTxHashes = append(innerTxHashes, tx.InnerTransactionHash)
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, pgxTx, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("🔓 unlocked %d channel accounts", affectedRows)
	}

	return nil
}

// processLiveIngestionTokenChanges processes trustline and contract changes for live ingestion.
// This updates the Redis cache and fetches metadata for new SAC/SEP-41 contracts.
func (m *ingestService) processLiveIngestionTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	// Sort trustline changes by operation ID in ascending order
	sort.Slice(trustlineChanges, func(i, j int) bool {
		return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
	})

	// Process all trustline and contract changes in a single batch using Redis pipelining
	if err := m.accountTokenService.ProcessTokenChanges(ctx, trustlineChanges, contractChanges); err != nil {
		log.Ctx(ctx).Errorf("processing trustline changes batch: %v", err)
		return fmt.Errorf("processing trustline changes batch: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(trustlineChanges), len(contractChanges))

	// Fetch and store metadata for new SAC/SEP-41 contracts
	if m.contractMetadataService != nil {
		newContractTypesByID := m.filterNewContractTokens(ctx, contractChanges)
		if len(newContractTypesByID) > 0 {
			log.Ctx(ctx).Infof("Fetching metadata for %d new contract tokens", len(newContractTypesByID))
			if err := m.contractMetadataService.FetchAndStoreMetadata(ctx, newContractTypesByID); err != nil {
				log.Ctx(ctx).Warnf("fetching new contract metadata: %v", err)
				// Don't return error - we don't want to block ingestion for metadata fetch failures
			}
		}
	}
	return nil
}

// filterNewContractTokens extracts unique SAC/SEP-41 contract IDs from contract changes,
// checks which contracts already exist in the database, and returns a map of only new contracts.
func (m *ingestService) filterNewContractTokens(ctx context.Context, contractChanges []types.ContractChange) map[string]types.ContractType {
	if len(contractChanges) == 0 {
		return nil
	}

	// Extract unique SAC and SEP-41 contract IDs and build type map
	seen := set.NewSet[string]()
	contractTypeMap := make(map[string]types.ContractType)
	var contractIDs []string

	for _, change := range contractChanges {
		// Only process SAC and SEP-41 contracts
		if change.ContractType != types.ContractTypeSAC && change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		if change.ContractID == "" {
			continue
		}
		if seen.Contains(change.ContractID) {
			continue
		}
		seen.Add(change.ContractID)
		contractIDs = append(contractIDs, change.ContractID)
		contractTypeMap[change.ContractID] = change.ContractType
	}

	if len(contractIDs) == 0 {
		return nil
	}

	// Check which contracts already exist in the database
	existingContracts, err := m.models.Contract.BatchGetByIDs(ctx, contractIDs)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to check existing contracts: %v", err)
		return nil
	}

	// Remove existing contracts from the map
	for _, contract := range existingContracts {
		delete(contractTypeMap, contract.ID)
	}

	return contractTypeMap
}
