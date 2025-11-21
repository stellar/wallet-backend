package services

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-rpc/protocol"

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
	chAccStore              store.ChannelAccountStore
	accountTokenService     AccountTokenService
	metricsService          metrics.MetricsService
	networkPassphrase       string
	getLedgersLimit         int
	ledgerIndexer           *indexer.Indexer
	pool                    pond.Pool
}

func NewIngestService(
	models *data.Models,
	ledgerCursorName string,
	accountTokensCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
	chAccStore store.ChannelAccountStore,
	accountTokenService AccountTokenService,
	metricsService metrics.MetricsService,
	getLedgersLimit int,
	network string,
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
	if chAccStore == nil {
		return nil, errors.New("chAccStore cannot be nil")
	}
	if metricsService == nil {
		return nil, errors.New("metricsService cannot be nil")
	}
	if getLedgersLimit <= 0 {
		return nil, errors.New("getLedgersLimit must be greater than 0")
	}

	// Create worker pools
	ledgerIndexerPool := pond.NewPool(0)
	ingestPool := pond.NewPool(0)

	// Register pools with metrics service
	metricsService.RegisterPoolMetrics("ledger_indexer", ledgerIndexerPool)
	metricsService.RegisterPoolMetrics("ingest", ingestPool)

	return &ingestService{
		models:                  models,
		ledgerCursorName:        ledgerCursorName,
		accountTokensCursorName: accountTokensCursorName,
		advisoryLockID:          generateAdvisoryLockID(network),
		appTracker:              appTracker,
		rpcService:              rpcService,
		chAccStore:              chAccStore,
		accountTokenService:     accountTokenService,
		metricsService:          metricsService,
		networkPassphrase:       rpcService.NetworkPassphrase(),
		getLedgersLimit:         getLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(rpcService.NetworkPassphrase(), ledgerIndexerPool, metricsService),
		pool:                    ingestPool,
	}, nil
}

func (m *ingestService) DeprecatedRun(ctx context.Context, startLedger uint32, endLedger uint32) error {
	manualTriggerChannel := make(chan any, 1)
	go m.rpcService.TrackRPCServiceHealth(ctx, manualTriggerChannel)
	ingestHeartbeatChannel := make(chan any, 1)
	rpcHeartbeatChannel := m.rpcService.GetHeartbeatChannel()
	go trackIngestServiceHealth(ctx, ingestHeartbeatChannel, m.appTracker)

	if startLedger == 0 {
		var err error
		startLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("getting start ledger: %w", err)
		}
	}

	ingestLedger := startLedger
	for endLedger == 0 || ingestLedger <= endLedger {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case resp := <-rpcHeartbeatChannel:
			switch {
			// Case-1: wallet-backend is running behind rpc's oldest ledger. In this case, we start
			// ingestion from rpc's oldest ledger.
			case ingestLedger < resp.OldestLedger:
				ingestLedger = resp.OldestLedger
			// Case-2: rpc is running behind wallet-backend's latest synced ledger. We wait for rpc to
			// catch back up to wallet-backend.
			case ingestLedger > resp.LatestLedger:
				log.Debugf("waiting for RPC to catchup to ledger %d (latest: %d)",
					ingestLedger, resp.LatestLedger)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Ctx(ctx).Infof("ingesting ledger: %d, oldest: %d, latest: %d", ingestLedger, resp.OldestLedger, resp.LatestLedger)

			start := time.Now()
			ledgerTransactions, err := m.GetLedgerTransactions(int64(ingestLedger))
			if err != nil {
				log.Error("getTransactions: %w", err)
				continue
			}
			ingestHeartbeatChannel <- true

			// eagerly unlock channel accounts from txs
			err = m.unlockChannelAccountsDeprecated(ctx, ledgerTransactions)
			if err != nil {
				return fmt.Errorf("unlocking channel account from tx: %w", err)
			}

			// update cursor
			err = db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
				if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.ledgerCursorName, ingestLedger); updateErr != nil {
					return fmt.Errorf("updating latest synced ledger: %w", updateErr)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("updating latest synced ledger: %w", err)
			}
			m.metricsService.SetLatestLedgerIngested(float64(ingestLedger))
			m.metricsService.ObserveIngestionDuration(time.Since(start).Seconds())

			// immediately trigger the next ingestion the wallet-backend is behind the RPC's latest ledger
			if resp.LatestLedger-ingestLedger > 1 {
				select {
				case manualTriggerChannel <- true:
				default:
					// do nothing if the channel is full
				}
			}

			ingestLedger++
		}
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
		m.pool.Stop()
	}()

	// get latest ledger synced, to use as a cursor
	var latestTrustlinesLedger uint32
	if startLedger == 0 {
		var err error
		startLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("getting latest ledger synced: %w", err)
		}

		latestTrustlinesLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.accountTokensCursorName)
		if err != nil {
			return fmt.Errorf("getting latest account-tokens ledger cursor synced: %w", err)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(signalChan)

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

	// Prepare the health check:
	manualTriggerChan := make(chan any, 1)
	go m.rpcService.TrackRPCServiceHealth(ctx, manualTriggerChan)
	ingestHeartbeatChannel := make(chan any, 1)
	rpcHeartbeatChannel := m.rpcService.GetHeartbeatChannel()
	go trackIngestServiceHealth(ctx, ingestHeartbeatChannel, m.appTracker)
	var rpcHealth entities.RPCGetHealthResult

	log.Ctx(ctx).Info("Starting ingestion loop")
	for {
		select {
		case sig := <-signalChan:
			return fmt.Errorf("ingestor stopped due to signal %q", sig)
		case <-ctx.Done():
			return fmt.Errorf("ingestor stopped due to context cancellation: %w", ctx.Err())
		case rpcHealth = <-rpcHeartbeatChannel:
			ingestHeartbeatChannel <- true // ⬅️ indicate that it's still running
			// this will fallthrough to execute the code below ⬇️
		}

		// fetch ledgers
		totalIngestionStart := time.Now()
		getLedgersResponse, err := m.fetchNextLedgersBatch(ctx, rpcHealth, startLedger)
		if err != nil {
			if errors.Is(err, ErrAlreadyInSync) {
				log.Ctx(ctx).Info("Ingestion is already in sync, will retry in a few moments...")
				continue
			}
			log.Ctx(ctx).Warnf("fetching next ledgers batch. will retry in a few moments...: %v", err)
			continue
		}
		m.metricsService.ObserveIngestionBatchSize(len(getLedgersResponse.Ledgers))

		// process ledgers
		err = m.processLedgerResponse(ctx, getLedgersResponse)
		if err != nil {
			return fmt.Errorf("processing ledger response: %w", err)
		}

		// update cursors
		cursorStart := time.Now()
		err = db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
			lastLedger := getLedgersResponse.Ledgers[len(getLedgersResponse.Ledgers)-1].Sequence
			if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.ledgerCursorName, lastLedger); updateErr != nil {
				return fmt.Errorf("updating latest synced ledger: %w", updateErr)
			}
			m.metricsService.SetLatestLedgerIngested(float64(lastLedger))
			checkpointLedger := m.accountTokenService.GetCheckpointLedger()
			if lastLedger > checkpointLedger {
				if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.accountTokensCursorName, lastLedger); updateErr != nil {
					return fmt.Errorf("updating latest synced account-tokens ledger: %w", updateErr)
				}
			}
			startLedger = lastLedger
			return nil
		})
		if err != nil {
			return fmt.Errorf("updating latest synced ledgers: %w", err)
		}
		cursorDuration := time.Since(cursorStart)
		m.metricsService.ObserveIngestionPhaseDuration("cursor_update", cursorDuration.Seconds())

		totalDuration := time.Since(totalIngestionStart)
		m.metricsService.ObserveIngestionDuration(totalDuration.Seconds())
		m.metricsService.IncIngestionLedgersProcessed(len(getLedgersResponse.Ledgers))

		if len(getLedgersResponse.Ledgers) == m.getLedgersLimit {
			select {
			case manualTriggerChan <- true:
			default:
				// do nothing if the channel is full
			}
		}
	}
}

// fetchNextLedgersBatch fetches the next batch of ledgers from the RPC service.
func (m *ingestService) fetchNextLedgersBatch(ctx context.Context, rpcHealth entities.RPCGetHealthResult, startLedger uint32) (GetLedgersResponse, error) {
	ledgerSeqRange, inSync := m.getLedgerSeqRange(rpcHealth.OldestLedger, rpcHealth.LatestLedger, startLedger)
	log.Ctx(ctx).Debugf("ledgerSeqRange: %+v", ledgerSeqRange)
	if inSync {
		return GetLedgersResponse{}, ErrAlreadyInSync
	}

	fetchStart := time.Now()
	getLedgersResponse, err := m.rpcService.GetLedgers(ledgerSeqRange.StartLedger, ledgerSeqRange.Limit)
	if err != nil {
		return GetLedgersResponse{}, fmt.Errorf("getting ledgers: %w", err)
	}
	fetchDuration := time.Since(fetchStart)
	m.metricsService.ObserveIngestionPhaseDuration("fetch_ledgers", fetchDuration.Seconds())

	return getLedgersResponse, nil
}

type LedgerSeqRange struct {
	StartLedger uint32
	Limit       uint32
}

// getLedgerSeqRange returns a ledger sequence range to ingest. It takes into account:
// - the ledgers available in the RPC,
// - the latest ledger synced by the ingestion service,
// - the max ledger window to ingest.
//
// The returned ledger sequence range is inclusive of the start and end ledgers.
func (m *ingestService) getLedgerSeqRange(rpcOldestLedger, rpcNewestLedger, latestLedgerSynced uint32) (ledgerRange LedgerSeqRange, inSync bool) {
	if latestLedgerSynced >= rpcNewestLedger {
		return LedgerSeqRange{}, true
	}
	ledgerRange.StartLedger = max(latestLedgerSynced+1, rpcOldestLedger)
	ledgerRange.Limit = uint32(m.getLedgersLimit)

	return ledgerRange, false
}

// ledgerData holds collected data for a single ledger including transactions and participants
type ledgerData struct {
	ledgerInfo      protocol.LedgerInfo
	precomputedData []indexer.PrecomputedTransactionData
	allParticipants set.Set[string]
}

// collectLedgerTransactionData collects all transaction data and participants from all ledgers in parallel.
// This is Phase 1 of ledger processing.
func (m *ingestService) collectLedgerTransactionData(ctx context.Context, getLedgersResponse GetLedgersResponse) ([]ledgerData, error) {
	ledgerDataList := make([]ledgerData, len(getLedgersResponse.Ledgers))
	group := m.pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}
	for idx, ledger := range getLedgersResponse.Ledgers {
		index := idx
		ledgerInfo := ledger
		group.Submit(func() {
			// Unmarshal and get transactions
			var xdrLedgerCloseMeta xdr.LedgerCloseMeta
			if err := xdr.SafeUnmarshalBase64(ledger.LedgerMetadata, &xdrLedgerCloseMeta); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("unmarshalling ledger close meta for ledger %d: %w", ledgerInfo.Sequence, err))
				errMu.Unlock()
				return
			}

			transactions, err := m.getLedgerTransactions(ctx, xdrLedgerCloseMeta)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("getting transactions for ledger %d: %w", ledgerInfo.Sequence, err))
				errMu.Unlock()
				return
			}

			// Create temporary indexer for data collection
			precomputedData, allParticipants, err := m.ledgerIndexer.CollectAllTransactionData(ctx, transactions)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("collecting data for ledger %d: %w", ledgerInfo.Sequence, err))
				errMu.Unlock()
				return
			}

			ledgerDataList[index] = ledgerData{
				ledgerInfo:      ledgerInfo,
				precomputedData: precomputedData,
				allParticipants: allParticipants,
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for ledger data collection: %w", err)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("collecting ledger data: %w", errors.Join(errs...))
	}
	return ledgerDataList, nil
}

// fetchExistingAccountsForParticipants fetches all existing accounts for participants across all ledgers.
// This is Phase 2 of ledger processing and makes a single DB call to get all existing accounts.
func (m *ingestService) fetchExistingAccountsForParticipants(ctx context.Context, ledgerDataList []ledgerData) (set.Set[string], error) {
	// Collect all unique participants across all ledgers
	allParticipants := set.NewSet[string]()
	for _, ld := range ledgerDataList {
		allParticipants = allParticipants.Union(ld.allParticipants)
	}

	// Batch fetch all existing accounts in a single DB query
	existingAccounts, err := m.models.Account.BatchGetByIDs(ctx, allParticipants.ToSlice())
	if err != nil {
		return nil, fmt.Errorf("batch checking participants: %w", err)
	}

	existingAccountsSet := set.NewSet[string]()
	if len(existingAccounts) >= 0 {
		existingAccountsSet = set.NewSet(existingAccounts...)
	}
	m.metricsService.ObserveIngestionParticipantsCount(allParticipants.Cardinality())
	return existingAccountsSet, nil
}

// processAndBufferTransactions processes transactions and populates per-ledger buffers in parallel.
// This is Phase 3 of ledger processing. Each ledger gets its own buffer to avoid lock contention.
func (m *ingestService) processAndBufferTransactions(ctx context.Context, ledgerDataList []ledgerData, existingAccountsSet set.Set[string]) ([]*indexer.IndexerBuffer, error) {
	ledgerBuffers := make([]*indexer.IndexerBuffer, len(ledgerDataList))
	group := m.pool.NewGroupContext(ctx)

	var errs []error
	errMu := sync.Mutex{}

	for idx, ld := range ledgerDataList {
		index := idx
		ledgerData := ld
		group.Submit(func() {
			// Create per-ledger indexer to avoid lock contention
			buffer := indexer.NewIndexerBuffer()
			if processErr := m.ledgerIndexer.ProcessTransactions(ctx, ledgerData.precomputedData, existingAccountsSet, buffer); processErr != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("processing ledger %d: %w", ledgerData.ledgerInfo.Sequence, processErr))
				errMu.Unlock()
				return
			}
			ledgerBuffers[index] = buffer
		})
	}
	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for ledger processing: %w", err)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("processing ledgers: %w", errors.Join(errs...))
	}
	return ledgerBuffers, nil
}

// mergeLedgerBuffers merges all per-ledger buffers into a single buffer for batch DB insertion.
// This is Phase 4 of ledger processing.
func (m *ingestService) mergeLedgerBuffers(ledgerBuffers []*indexer.IndexerBuffer) *indexer.IndexerBuffer {
	mergedBuffer := indexer.NewIndexerBuffer()
	for _, buffer := range ledgerBuffers {
		mergedBuffer.MergeBuffer(buffer)
	}
	return mergedBuffer
}

func (m *ingestService) processLedgerResponse(ctx context.Context, getLedgersResponse GetLedgersResponse) error {
	// Phase 1: Collect all transaction data and participants from all ledgers in parallel
	start := time.Now()
	ledgerDataList, err := m.collectLedgerTransactionData(ctx, getLedgersResponse)
	if err != nil {
		return fmt.Errorf("collecting ledger transaction data: %w", err)
	}
	collectDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("collect_transaction_data", collectDuration.Seconds())

	// Phase 2: Single DB call to get all existing accounts across all ledgers
	start = time.Now()
	existingAccountsSet, err := m.fetchExistingAccountsForParticipants(ctx, ledgerDataList)
	if err != nil {
		return fmt.Errorf("fetching existing accounts: %w", err)
	}
	fetchAccountsDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("fetch_existing_accounts", fetchAccountsDuration.Seconds())

	// Phase 3: Process transactions and populate per-ledger buffers in parallel
	start = time.Now()
	ledgerBuffers, err := m.processAndBufferTransactions(ctx, ledgerDataList, existingAccountsSet)
	if err != nil {
		return fmt.Errorf("processing and buffering transactions: %w", err)
	}
	processBufferDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("process_and_buffer", processBufferDuration.Seconds())

	// Phase 4: Merge all per-ledger buffers into a single buffer
	start = time.Now()
	mergedBuffer := m.mergeLedgerBuffers(ledgerBuffers)
	mergeDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("merge_buffers", mergeDuration.Seconds())

	// Phase 5: Insert all data into DB
	start = time.Now()
	if err := m.ingestProcessedData(ctx, mergedBuffer); err != nil {
		return fmt.Errorf("ingesting processed data: %w", err)
	}
	dbInsertDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("db_insertion", dbInsertDuration.Seconds())

	// Log summary of processing
	memStats := new(runtime.MemStats)
	runtime.ReadMemStats(memStats)
	numberOfTransactions := mergedBuffer.GetNumberOfTransactions()
	numberOfOperations := mergedBuffer.GetNumberOfOperations()
	m.metricsService.IncIngestionTransactionsProcessed(numberOfTransactions)
	m.metricsService.IncIngestionOperationsProcessed(numberOfOperations)

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

// unlockChannelAccountsDeprecated unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccountsDeprecated(ctx context.Context, ledgerTransactions []entities.Transaction) error {
	if len(ledgerTransactions) == 0 {
		log.Ctx(ctx).Debug("no transactions to unlock channel accounts from")
		return nil
	}

	innerTxHashes := make([]string, 0, len(ledgerTransactions))
	for _, tx := range ledgerTransactions {
		if innerTxHash, err := m.extractInnerTxHash(tx.EnvelopeXDR); err != nil {
			return fmt.Errorf("extracting inner tx hash: %w", err)
		} else {
			innerTxHashes = append(innerTxHashes, innerTxHash)
		}
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, nil, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("unlocked %d channel accounts", affectedRows)
	}

	return nil
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
