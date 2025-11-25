package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrAlreadyInSync = errors.New("ingestion is already in sync")

const (
	ingestHealthCheckMaxWaitTime = 90 * time.Second
)

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
	models                  *data.Models
	ledgerCursorName        string
	accountTokensCursorName string
	advisoryLockID          int
	appTracker              apptracker.AppTracker
	rpcService              RPCService
	ledgerBackend           ledgerbackend.LedgerBackend
	chAccStore              store.ChannelAccountStore
	accountTokenService     AccountTokenService
	contractMetadataService ContractMetadataService
	metricsService          metrics.MetricsService
	networkPassphrase       string
	getLedgersLimit         int
	ledgerIndexer           *indexer.Indexer
}

func NewIngestService(
	models *data.Models,
	ledgerCursorName string,
	accountTokensCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
	ledgerBackend ledgerbackend.LedgerBackend,
	chAccStore store.ChannelAccountStore,
	accountTokenService AccountTokenService,
	contractMetadataService ContractMetadataService,
	metricsService metrics.MetricsService,
	getLedgersLimit int,
	network string,
	networkPassphrase string,
) (*ingestService, error) {
	if models == nil {
		return nil, errors.New("models cannot be nil")
	}
	if ledgerCursorName == "" {
		return nil, errors.New("ledgerCursorName cannot be nil")
	}
	if accountTokensCursorName == "" {
		return nil, errors.New("accountTokensCursorName cannot be nil")
	}
	if appTracker == nil {
		return nil, errors.New("appTracker cannot be nil")
	}
	if rpcService == nil {
		return nil, errors.New("rpcService cannot be nil")
	}
	if ledgerBackend == nil {
		return nil, errors.New("ledgerBackend cannot be nil")
	}
	if chAccStore == nil {
		return nil, errors.New("chAccStore cannot be nil")
	}
	if metricsService == nil {
		return nil, errors.New("metricsService cannot be nil")
	}
	if getLedgersLimit <= 0 {
		return nil, errors.New("getLedgersLimit must be greater than 0")
	}

	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)
	metricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)

	return &ingestService{
		models:                  models,
		ledgerCursorName:        ledgerCursorName,
		accountTokensCursorName: accountTokensCursorName,
		advisoryLockID:          generateAdvisoryLockID(network),
		appTracker:              appTracker,
		rpcService:              rpcService,
		ledgerBackend:           ledgerBackend,
		chAccStore:              chAccStore,
		accountTokenService:     accountTokenService,
		contractMetadataService: contractMetadataService,
		metricsService:          metricsService,
		networkPassphrase:       networkPassphrase,
		getLedgersLimit:         getLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(networkPassphrase, ledgerIndexerPool, metricsService),
	}, nil
}

// determineStartLedger determines the appropriate starting ledger sequence based on the backend
// type and configuration parameters. For RPC backend with startLedger=0, it queries RPC health
// to ensure we start from a ledger within the RPC's retention window.
func (m *ingestService) determineStartLedger(ctx context.Context, configuredStartLedger uint32) (uint32, error) {
	// If startLedger is explicitly provided and non-zero, use it
	if configuredStartLedger > 0 {
		return configuredStartLedger, nil
	}

	// Get the latest synced ledger from database as baseline
	dbLedger, err := m.models.IngestStore.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger synced: %w", err)
	}

	// For RPC backend, check RPC health to ensure we don't start
	// from a ledger older than RPC's retention window
	health, err := m.rpcService.GetHealth()
	if err != nil {
		return 0, fmt.Errorf("getting RPC health: %w", err)
	}

	// For empty db, we start with latest ledger
	if dbLedger == 0 {
		return health.LatestLedger, nil
	}

	// If DB ledger is behind RPC's oldest, start from RPC's oldest
	if dbLedger < health.OldestLedger {
		return 0, fmt.Errorf("wallet backend has fallen behind RPC's retention window, run a backfill to catchup to the tip")
	}

	// Start from next ledger after DB cursor (if DB has data)
	return dbLedger + 1, nil
}

// prepareBackendRange prepares the ledger backend with the appropriate range type.
// Returns the operating mode (live streaming vs backfill).
func (m *ingestService) prepareBackendRange(ctx context.Context, startLedger, endLedger uint32) error {
	var ledgerRange ledgerbackend.Range
	if endLedger == 0 {
		// Live streaming mode with unbounded range
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
		if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
			return fmt.Errorf("preparing datastore backend unbounded range from %d: %w", startLedger, err)
		}
		log.Ctx(ctx).Infof("Prepared backend with unbounded range starting from ledger %d", startLedger)
	} else {
		ledgerRange := ledgerbackend.BoundedRange(startLedger, endLedger)
		if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
			return fmt.Errorf("preparing datastore backend bounded range [%d, %d]: %w", startLedger, endLedger, err)
		}
		log.Ctx(ctx).Infof("Prepared backend with bounded range [%d, %d]", startLedger, endLedger)
	}
	return nil
}

func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently
	if lockAcquired, err := db.AcquireAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	} else if !lockAcquired {
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(ctx, m.models.DB, m.advisoryLockID); err != nil {
			err = fmt.Errorf("releasing advisory lock: %w", err)
			log.Ctx(ctx).Error(err)
		}
	}()

	// Determine the actual starting ledger based on backend type and configuration
	actualStartLedger, err := m.determineStartLedger(ctx, startLedger)
	if err != nil {
		return fmt.Errorf("determining start ledger: %w", err)
	}

	// Get account tokens cursor if starting fresh (when original startLedger was 0)
	var latestTrustlinesLedger uint32
	if startLedger == 0 {
		latestTrustlinesLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.accountTokensCursorName)
		if err != nil {
			return fmt.Errorf("getting latest account-tokens ledger cursor synced: %w", err)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(signalChan)

	// Populate account tokens cache if needed
	if latestTrustlinesLedger == 0 {
		err := m.accountTokenService.PopulateAccountTokens(ctx)
		if err != nil {
			return fmt.Errorf("populating account tokens cache: %w", err)
		}
		checkpointLedger := m.accountTokenService.GetCheckpointLedger()
		err = db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
			if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.accountTokensCursorName, checkpointLedger); updateErr != nil {
				return fmt.Errorf("updating latest synced account-tokens ledger: %w", updateErr)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("updating latest synced account-tokens ledger: %w", err)
		}
	}

	// Prepare backend range
	err = m.prepareBackendRange(ctx, actualStartLedger, endLedger)
	if err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	currentLedger := actualStartLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger: %d", currentLedger)
	for {
		select {
		case sig := <-signalChan:
			log.Ctx(ctx).Infof("Received signal %q, shutting down gracefully", sig)
			return nil
		case <-ctx.Done():
			return fmt.Errorf("ingestor stopped due to context cancellation: %w", ctx.Err())
		default:
			ledgerMeta, ledgerErr := m.ledgerBackend.GetLedger(ctx, currentLedger)
			if ledgerErr != nil {
				if endLedger > 0 && currentLedger > endLedger {
					log.Ctx(ctx).Infof("Backfill complete: processed ledgers %d to %d", actualStartLedger, endLedger)
					return nil
				}
				log.Ctx(ctx).Warnf("Error fetching ledger %d: %v, retrying...", currentLedger, ledgerErr)
				time.Sleep(time.Second)
				continue
			}

			totalStart := time.Now()
			if processErr := m.processSingleLedger(ctx, ledgerMeta); processErr != nil {
				return fmt.Errorf("processing ledger %d: %w", currentLedger, processErr)
			}

			// Update cursors
			cursorStart := time.Now()
			err = db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
				if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.ledgerCursorName, currentLedger); updateErr != nil {
					return fmt.Errorf("updating latest synced ledger: %w", updateErr)
				}
				m.metricsService.SetLatestLedgerIngested(float64(currentLedger))

				if currentLedger > m.accountTokenService.GetCheckpointLedger() {
					if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.accountTokensCursorName, currentLedger); updateErr != nil {
						return fmt.Errorf("updating account-tokens ledger: %w", updateErr)
					}
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("updating cursors: %w", err)
			}
			m.metricsService.ObserveIngestionPhaseDuration("cursor_update", time.Since(cursorStart).Seconds())
			m.metricsService.ObserveIngestionDuration(time.Since(totalStart).Seconds())
			m.metricsService.IncIngestionLedgersProcessed(1)

			log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, time.Since(totalStart))
			currentLedger++

			if endLedger > 0 && currentLedger > endLedger {
				log.Ctx(ctx).Infof("Backfill complete: processed ledgers %d to %d", actualStartLedger, endLedger)
				return nil
			}
		}
	}
}

// processSingleLedger processes a single ledger through all ingestion phases.
// Phase 1: Get transactions and collect data using Indexer (parallel within ledger)
// Phase 2: Fetch existing accounts for participants (single DB call)
// Phase 3: Process transactions and populate buffer using Indexer (parallel within ledger)
// Phase 4: Insert all data into DB
func (m *ingestService) processSingleLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta) error {
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Phase 1: Get transactions from ledger
	start := time.Now()
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	// Phase 1b: Collect all transaction data using Indexer (parallel within ledger)
	precomputedData, allParticipants, err := m.ledgerIndexer.CollectAllTransactionData(ctx, transactions)
	if err != nil {
		return fmt.Errorf("collecting transaction data for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("collect_transaction_data", time.Since(start).Seconds())

	// Phase 2: Fetch existing accounts for participants (single DB call)
	start = time.Now()
	existingAccounts, err := m.models.Account.BatchGetByIDs(ctx, allParticipants.ToSlice())
	if err != nil {
		return fmt.Errorf("batch checking participants for ledger %d: %w", ledgerSeq, err)
	}
	existingAccountsSet := set.NewSet(existingAccounts...)
	m.metricsService.ObserveIngestionParticipantsCount(allParticipants.Cardinality())
	m.metricsService.ObserveIngestionPhaseDuration("fetch_existing_accounts", time.Since(start).Seconds())

	// Phase 3: Process transactions using Indexer (parallel within ledger)
	start = time.Now()
	buffer := indexer.NewIndexerBuffer()
	if err := m.ledgerIndexer.ProcessTransactions(ctx, precomputedData, existingAccountsSet, buffer); err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("process_and_buffer", time.Since(start).Seconds())

	// Phase 4: Insert all data into DB
	start = time.Now()
	if err := m.ingestProcessedData(ctx, buffer); err != nil {
		return fmt.Errorf("ingesting processed data for ledger %d: %w", ledgerSeq, err)
	}
	m.metricsService.ObserveIngestionPhaseDuration("db_insertion", time.Since(start).Seconds())

	// Metrics
	m.metricsService.IncIngestionTransactionsProcessed(buffer.GetNumberOfTransactions())
	m.metricsService.IncIngestionOperationsProcessed(buffer.GetNumberOfOperations())

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

func (m *ingestService) ingestProcessedData(ctx context.Context, indexerBuffer indexer.IndexerBufferInterface) error {
	dbTxErr := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		// 2. Insert queries
		// 2.1. Insert transactions
		txs := indexerBuffer.GetTransactions()
		stellarAddressesByTxHash := indexerBuffer.GetTransactionsParticipants()
		if len(txs) > 0 {
			insertedHashes, err := m.models.Transactions.BatchInsert(ctx, dbTx, txs, stellarAddressesByTxHash)
			if err != nil {
				return fmt.Errorf("batch inserting transactions: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d transactions with hashes %v", len(insertedHashes), insertedHashes)
		}

		// 2.2. Insert operations
		ops := indexerBuffer.GetOperations()
		stellarAddressesByOpID := indexerBuffer.GetOperationsParticipants()
		if len(ops) > 0 {
			insertedOpIDs, err := m.models.Operations.BatchInsert(ctx, dbTx, ops, stellarAddressesByOpID)
			if err != nil {
				return fmt.Errorf("batch inserting operations: %w", err)
			}
			log.Ctx(ctx).Infof("✅ inserted %d operations with IDs %v", len(insertedOpIDs), insertedOpIDs)
		}

		// 2.3. Insert state changes
		stateChanges := indexerBuffer.GetStateChanges()
		if len(stateChanges) > 0 {
			insertedStateChangeIDs, err := m.models.StateChanges.BatchInsert(ctx, dbTx, stateChanges)
			if err != nil {
				return fmt.Errorf("batch inserting state changes: %w", err)
			}

			// Count state changes by type and category
			typeCategoryCount := make(map[string]map[string]int)
			for _, sc := range stateChanges {
				category := string(sc.StateChangeCategory)
				scType := ""
				if sc.StateChangeReason != nil {
					scType = string(*sc.StateChangeReason)
				}

				if typeCategoryCount[scType] == nil {
					typeCategoryCount[scType] = make(map[string]int)
				}
				typeCategoryCount[scType][category]++
			}

			for scType, categories := range typeCategoryCount {
				for category, count := range categories {
					m.metricsService.IncStateChanges(scType, category, count)
				}
			}

			log.Ctx(ctx).Infof("✅ inserted %d state changes with IDs %v", len(insertedStateChangeIDs), insertedStateChangeIDs)
		}

		// 3. Unlock channel accounts.
		err := m.unlockChannelAccounts(ctx, txs)
		if err != nil {
			return fmt.Errorf("unlocking channel accounts: %w", err)
		}

		return nil
	})
	if dbTxErr != nil {
		return fmt.Errorf("ingesting processed data: %w", dbTxErr)
	}

	trustlineChanges := indexerBuffer.GetTrustlineChanges()
	filteredTrustlineChanges := make([]types.TrustlineChange, 0, len(trustlineChanges))
	if len(trustlineChanges) > 0 {
		// Insert trustline changes in the ascending order of operation IDs using batch processing
		sort.Slice(trustlineChanges, func(i, j int) bool {
			return trustlineChanges[i].OperationID < trustlineChanges[j].OperationID
		})

		// Filter out changes that are older than the checkpoint ledger
		// This check is required since we initialize trustlines and contracts using the latest checkpoint ledger which could be ahead of wallet backend's latest ledger synced.
		// We will skip changes that are older than the latest checkpoint ledger as the wallet backend catches up to the tip. We only need to ingest changes that are newer than the latest checkpoint ledger.
		for _, change := range trustlineChanges {
			if change.LedgerNumber > m.accountTokenService.GetCheckpointLedger() {
				filteredTrustlineChanges = append(filteredTrustlineChanges, change)
			}
		}
	}

	contractChanges := indexerBuffer.GetContractChanges()
	filteredContractChanges := make([]types.ContractChange, 0, len(contractChanges))
	if len(contractChanges) > 0 {
		for _, change := range contractChanges {
			if change.LedgerNumber > m.accountTokenService.GetCheckpointLedger() {
				filteredContractChanges = append(filteredContractChanges, change)
			}
		}
	}

	// Process all trustline and contract changes in a single batch using Redis pipelining
	if err := m.accountTokenService.ProcessTokenChanges(ctx, filteredTrustlineChanges, filteredContractChanges); err != nil {
		log.Ctx(ctx).Errorf("processing trustline changes batch: %v", err)
		return fmt.Errorf("processing trustline changes batch: %w", err)
	}
	log.Ctx(ctx).Infof("✅ inserted %d trustline and %d contract changes", len(filteredTrustlineChanges), len(filteredContractChanges))

	// Fetch and store metadata for new SAC/SEP-41 contracts discovered during live ingestion
	if m.contractMetadataService != nil {
		newContractTypesByID := m.filterNewContractTokens(ctx, filteredContractChanges)
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

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, txs []types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	innerTxHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		innerTxHash, err := m.extractInnerTxHash(tx.EnvelopeXDR)
		if err != nil {
			return fmt.Errorf("extracting inner tx hash: %w", err)
		}
		innerTxHashes = append(innerTxHashes, innerTxHash)
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, nil, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("🔓 unlocked %d channel accounts", affectedRows)
	}

	return nil
}

func (m *ingestService) GetLedgerTransactions(ledger int64) ([]entities.Transaction, error) {
	var ledgerTransactions []entities.Transaction
	var cursor string
	lastLedgerSeen := ledger
	for lastLedgerSeen == ledger {
		getTxnsResp, err := m.rpcService.GetTransactions(ledger, cursor, 50)
		if err != nil {
			return []entities.Transaction{}, fmt.Errorf("getTransactions: %w", err)
		}
		cursor = getTxnsResp.Cursor
		for _, tx := range getTxnsResp.Transactions {
			if tx.Ledger == ledger {
				ledgerTransactions = append(ledgerTransactions, tx)
				lastLedgerSeen = tx.Ledger
			} else {
				lastLedgerSeen = tx.Ledger
				break
			}
		}
	}
	return ledgerTransactions, nil
}

// extractInnerTxHash takes a transaction XDR string and returns the hash of its inner transaction.
// For fee bump transactions, it returns the hash of the inner transaction.
// For regular transactions, it returns the hash of the transaction itself.
func (m *ingestService) extractInnerTxHash(txXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return "", fmt.Errorf("deserializing envelope xdr %q: %w", txXDR, err)
	}

	var innerTx *txnbuild.Transaction
	feeBumpTx, ok := genericTx.FeeBump()
	if ok {
		innerTx = feeBumpTx.InnerTransaction()
	} else {
		innerTx, ok = genericTx.Transaction()
		if !ok {
			return "", errors.New("transaction is neither fee bump nor inner transaction")
		}
	}

	innerTxHash, err := innerTx.HashHex(m.rpcService.NetworkPassphrase())
	if err != nil {
		return "", fmt.Errorf("generating hash hex: %w", err)
	}

	return innerTxHash, nil
}

func trackIngestServiceHealth(ctx context.Context, heartbeat chan any, tracker apptracker.AppTracker) {
	ticker := time.NewTicker(ingestHealthCheckMaxWaitTime)
	defer func() {
		ticker.Stop()
		close(heartbeat)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			warn := fmt.Sprintf("🐌 ingestion service stale for over %s 🐢", ingestHealthCheckMaxWaitTime)
			log.Ctx(ctx).Warn(warn)
			if tracker != nil {
				tracker.CaptureMessage(warn)
			} else {
				log.Ctx(ctx).Warnf("[NIL TRACKER] %s", warn)
			}
			ticker.Reset(ingestHealthCheckMaxWaitTime)
		case <-heartbeat:
			ticker.Reset(ingestHealthCheckMaxWaitTime)
		}
	}
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
