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

// ingestionMode represents the operating mode for ledger ingestion.
type ingestionMode string

const (
	ingestionModeLive ingestionMode = "live"
	ingestionModeBackfill ingestionMode = "backfill"
)

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
	pool                    pond.Pool
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
		ledgerBackend:           ledgerBackend,
		chAccStore:              chAccStore,
		accountTokenService:     accountTokenService,
		contractMetadataService: contractMetadataService,
		metricsService:          metricsService,
		networkPassphrase:       networkPassphrase,
		getLedgersLimit:         getLedgersLimit,
		ledgerIndexer:           indexer.NewIndexer(networkPassphrase, ledgerIndexerPool, metricsService),
		pool:                    ingestPool,
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

	switch m.ledgerBackend.(type) {
	case *ledgerbackend.RPCLedgerBackend:
		// For RPC backend, check RPC health to ensure we don't start
		// from a ledger older than RPC's retention window
		health, err := m.rpcService.GetHealth()
		if err != nil {
			return 0, fmt.Errorf("getting RPC health: %w", err)
		}

		// If DB ledger is behind RPC's oldest, start from RPC's oldest
		if dbLedger < health.OldestLedger {
			log.Ctx(ctx).Infof("DB ledger %d is behind RPC oldest %d, starting from RPC oldest", dbLedger, health.OldestLedger)
			return health.OldestLedger, nil
		}

		// Start from next ledger after DB cursor (if DB has data)
		if dbLedger > 0 {
			return dbLedger + 1, nil
		}

		// DB is empty, start from RPC's oldest
		return health.OldestLedger, nil

	case *ledgerbackend.BufferedStorageBackend:
		// For BufferedStorageBackend, use DB cursor + 1 (or 1 if empty)
		if dbLedger > 0 {
			return dbLedger + 1, nil
		}
		return 1, nil

	default:
		return 0, fmt.Errorf("unsupported ledger backend type: %T", m.ledgerBackend)
	}
}

// prepareBackendRange prepares the ledger backend with the appropriate range type.
// Returns the operating mode (live streaming vs backfill).
func (m *ingestService) prepareBackendRange(ctx context.Context, startLedger, endLedger uint32) (ingestionMode, error) {
	switch m.ledgerBackend.(type) {
	case *ledgerbackend.RPCLedgerBackend:
		// RPC backend always uses unbounded range for live streaming
		ledgerRange := ledgerbackend.UnboundedRange(startLedger)
		if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
			return "", fmt.Errorf("preparing RPC backend range from %d: %w", startLedger, err)
		}
		log.Ctx(ctx).Infof("Prepared RPC backend with unbounded range starting from ledger %d", startLedger)
		return ingestionModeLive, nil

	case *ledgerbackend.BufferedStorageBackend:
		if endLedger == 0 {
			// Live streaming mode with unbounded range
			ledgerRange := ledgerbackend.UnboundedRange(startLedger)
			if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
				return "", fmt.Errorf("preparing datastore backend unbounded range from %d: %w", startLedger, err)
			}
			log.Ctx(ctx).Infof("Prepared datastore backend with unbounded range starting from ledger %d", startLedger)
			return ingestionModeLive, nil
		}

		// Backfill mode with bounded range
		ledgerRange := ledgerbackend.BoundedRange(startLedger, endLedger)
		if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
			return "", fmt.Errorf("preparing datastore backend bounded range [%d, %d]: %w", startLedger, endLedger, err)
		}
		log.Ctx(ctx).Infof("Prepared datastore backend with bounded range [%d, %d]", startLedger, endLedger)
		return ingestionModeBackfill, nil

	default:
		return "", fmt.Errorf("unsupported ledger backend type: %T", m.ledgerBackend)
	}
}

// fetchNextLedgersBatch fetches the next batch of ledgers from the backend.
// Both backends handle blocking/waiting internally via GetLedger.
// For bounded ranges, it stops at endLedger.
func (m *ingestService) fetchNextLedgersBatch(ctx context.Context, currentLedger, endLedger uint32) ([]xdr.LedgerCloseMeta, error) {
	ledgers := make([]xdr.LedgerCloseMeta, 0, m.getLedgersLimit)

	for i := 0; i < m.getLedgersLimit; i++ {
		ledgerSeq := currentLedger + uint32(i)

		// Check if we've reached the end of a bounded range
		if endLedger > 0 && ledgerSeq > endLedger {
			break
		}

		// GetLedger blocks until the ledger is available
		// - RPCBackend: Retries every 2 seconds if beyond latest
		// - BufferedStorageBackend: Blocks until data is available from queue
		ledgerMeta, err := m.ledgerBackend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			// If we already have some ledgers, return them
			// The caller will process what we have and request more
			if len(ledgers) > 0 {
				log.Ctx(ctx).Warnf("Error fetching ledger %d after getting %d ledgers: %v", ledgerSeq, len(ledgers), err)
				return ledgers, nil
			}
			return nil, fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
		}

		ledgers = append(ledgers, ledgerMeta)
		log.Ctx(ctx).Debugf("Fetched ledger %d (%d/%d in batch)", ledgerSeq, len(ledgers), m.getLedgersLimit)
	}

	if len(ledgers) == 0 {
		return nil, ErrAlreadyInSync
	}

	return ledgers, nil
}

func (m *ingestService) DeprecatedRun(ctx context.Context, startLedger uint32, endLedger uint32) error {
	manualTriggerChannel := make(chan any, 1)
	go func() {
		if err := m.rpcService.TrackRPCServiceHealth(ctx, manualTriggerChannel); err != nil {
			log.Ctx(ctx).Warnf("RPC health tracking stopped: %v", err)
		}
	}()
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
	mode, err := m.prepareBackendRange(ctx, actualStartLedger, endLedger)
	if err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	currentLedger := actualStartLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger: %d", currentLedger)
	// for {
	// 	select {
	// 	case sig := <-signalChan:
	// 		return fmt.Errorf("ingestor stopped due to signal %q", sig)
	// 	case <-ctx.Done():
	// 		return fmt.Errorf("ingestor stopped due to context cancellation: %w", ctx.Err())
	// 	default:
			// // Fetch next batch of ledgers
			// startTime := time.Now()
			// ledgers, err := m.fetchNextLedgersBatch(ctx, currentLedger, endLedger)
			// if err != nil {
			// 	if errors.Is(err, ErrAlreadyInSync) {
			// 		// In bounded mode, this means we've completed the range
			// 		if endLedger > 0 {
			// 			log.Ctx(ctx).Infof("Backfill complete: processed all ledgers up to %d", endLedger)
			// 			return nil
			// 		}
			// 		// In live mode, this shouldn't happen with proper backend behavior
			// 		log.Ctx(ctx).Debug("Temporarily in sync, waiting for new ledgers...")
			// 		time.Sleep(time.Second)
			// 		continue
			// 	}
			// 	log.Ctx(ctx).Warnf("Error fetching ledgers batch starting at %d: %v, retrying...", currentLedger, err)
			// 	time.Sleep(time.Second)
			// 	continue
			// }
			// fetchDuration := time.Since(startTime).Seconds()
			// m.metricsService.ObserveIngestionPhaseDuration("fetch_ledgers", fetchDuration)
			// m.metricsService.ObserveIngestionBatchSize(len(ledgers))

			// Process the ledgers
	// 		totalIngestionStart := time.Now()
	// 		err = m.processLedgerResponse(ctx, ledgers)
	// 		if err != nil {
	// 			return fmt.Errorf("processing ledger response: %w", err)
	// 		}

	// 		// Update cursors
	// 		cursorStart := time.Now()
	// 		lastLedger := ledgers[len(ledgers)-1].LedgerSequence()
	// 		err = db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
	// 			if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.ledgerCursorName, lastLedger); updateErr != nil {
	// 				return fmt.Errorf("updating latest synced ledger: %w", updateErr)
	// 			}
	// 			m.metricsService.SetLatestLedgerIngested(float64(lastLedger))

	// 			checkpointLedger := m.accountTokenService.GetCheckpointLedger()
	// 			if lastLedger > checkpointLedger {
	// 				if updateErr := m.models.IngestStore.UpdateLatestLedgerSynced(ctx, dbTx, m.accountTokensCursorName, lastLedger); updateErr != nil {
	// 					return fmt.Errorf("updating latest synced account-tokens ledger: %w", updateErr)
	// 				}
	// 			}
	// 			return nil
	// 		})
	// 		if err != nil {
	// 			return fmt.Errorf("updating latest synced ledgers: %w", err)
	// 		}
	// 		cursorDuration := time.Since(cursorStart)
	// 		m.metricsService.ObserveIngestionPhaseDuration("cursor_update", cursorDuration.Seconds())

	// 		totalDuration := time.Since(totalIngestionStart)
	// 		m.metricsService.ObserveIngestionDuration(totalDuration.Seconds())
	// 		m.metricsService.IncIngestionLedgersProcessed(len(ledgers))

	// 		// Update current ledger for next iteration
	// 		currentLedger = lastLedger + 1

	// 		// Check if backfill is complete
	// 		if endLedger > 0 && currentLedger > endLedger {
	// 			log.Ctx(ctx).Infof("Backfill complete: processed all ledgers from %d to %d", actualStartLedger, endLedger)
	// 			return nil
	// 		}
	// 	}
	// }
	switch mode {
	case ingestionModeLive:
		for {
			_, err := m.ledgerBackend.GetLedger(ctx, currentLedger)
			if err != nil {
				break
			}
		}
	case ingestionModeBackfill:
		for ledgerSeq := currentLedger; ledgerSeq <= endLedger; ledgerSeq++ {
			_, err := m.ledgerBackend.GetLedger(ctx, ledgerSeq)
			if err != nil {
				break
			}
		}
	default:
		return fmt.Errorf("unknown ingestion mode")
	}
	return nil
}

// ledgerData holds collected data for a single ledger including transactions and participants
type ledgerData struct {
	ledgerCloseMeta xdr.LedgerCloseMeta
	precomputedData []indexer.PrecomputedTransactionData
	allParticipants set.Set[string]
}

// collectLedgerTransactionData collects all transaction data and participants from all ledgers in parallel.
// This is Phase 1 of ledger processing.
func (m *ingestService) collectLedgerTransactionData(ctx context.Context, ledgers []xdr.LedgerCloseMeta) ([]ledgerData, error) {
	phaseStart := time.Now()

	ledgerDataList := make([]ledgerData, len(ledgers))
	group := m.pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}
	for idx, ledgerMeta := range ledgers {
		index := idx
		ledgerCloseMeta := ledgerMeta
		group.Submit(func() {
			ledgerSeq := ledgerCloseMeta.LedgerSequence()

			transactions, err := m.getLedgerTransactions(ctx, ledgerCloseMeta)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err))
				errMu.Unlock()
				return
			}

			// Create temporary indexer for data collection
			precomputedData, allParticipants, err := m.ledgerIndexer.CollectAllTransactionData(ctx, transactions)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("collecting data for ledger %d: %w", ledgerSeq, err))
				errMu.Unlock()
				return
			}

			ledgerDataList[index] = ledgerData{
				ledgerCloseMeta: ledgerCloseMeta,
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
	phaseDuration := time.Since(phaseStart).Seconds()
	m.metricsService.ObserveIngestionPhaseDuration("collect_transaction_data", phaseDuration)

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
				errs = append(errs, fmt.Errorf("processing ledger %d: %w", ledgerData.ledgerCloseMeta.LedgerSequence(), processErr))
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

func (m *ingestService) processLedgerResponse(ctx context.Context, ledgers []xdr.LedgerCloseMeta) error {
	// Phase 1: Collect all transaction data and participants from all ledgers in parallel
	start := time.Now()
	ledgerDataList, err := m.collectLedgerTransactionData(ctx, ledgers)
	if err != nil {
		return fmt.Errorf("collecting ledger transaction data: %w", err)
	}

	collectDuration := time.Since(start)
	m.metricsService.ObserveIngestionPhaseDuration("collect_transaction_data", collectDuration.Seconds())
	// Phase 2: Single DB call to get all existing accounts across all ledgers
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
