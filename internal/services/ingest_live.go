package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// startLiveIngestion begins continuous ingestion from the last checkpoint ledger,
// acquiring an advisory lock to prevent concurrent ingestion instances.
func (m *ingestService) startLiveIngestion(ctx context.Context) error {
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

	// Get latest ingested ledger to determine DB state
	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	startLedger := latestIngestedLedger + 1
	if latestIngestedLedger == 0 {
		startLedger, err = m.archive.GetLatestLedgerSequence()
		if err != nil {
			return fmt.Errorf("getting latest ledger sequence: %w", err)
		}

		err = m.accountTokenService.PopulateAccountTokens(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("populating account tokens cache: %w", err)
		}

		err := m.initializeCursors(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("initializing cursors: %w", err)
		}
	} else {
		// If we already have data in the DB, we will do an optimized catchup by parallely backfilling the ledgers.
		health, err := m.rpcService.GetHealth()
		if err != nil {
			return fmt.Errorf("getting health check result from RPC: %w", err)
		}

		networkLatestLedger := health.LatestLedger
		if networkLatestLedger > startLedger && (networkLatestLedger-startLedger) >= m.catchupThreshold {
			log.Ctx(ctx).Infof("Wallet backend has fallen behind network tip by %d ledgers. Doing optimized catchup to the tip: %d", networkLatestLedger-startLedger, networkLatestLedger)
			err := m.startBackfilling(ctx, startLedger, networkLatestLedger, BackfillModeCatchup)
			if err != nil {
				return fmt.Errorf("catching up to network tip: %w", err)
			}
			// Update startLedger to continue from where catchup ended
			startLedger = networkLatestLedger + 1
		}
	}

	// Start unbounded ingestion from latest ledger ingested onwards
	ledgerRange := ledgerbackend.UnboundedRange(startLedger)
	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing unbounded ledger backend range from %d: %w", startLedger, err)
	}
	return m.ingestLiveLedgers(ctx, startLedger)
}

// initializeCursors initializes both latest and oldest cursors to the same starting ledger.
func (m *ingestService) initializeCursors(ctx context.Context, ledger uint32) error {
	err := db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		if err := m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, ledger); err != nil {
			return fmt.Errorf("initializing latest cursor: %w", err)
		}
		if err := m.models.IngestStore.Update(ctx, dbTx, m.oldestLedgerCursorName, ledger); err != nil {
			return fmt.Errorf("initializing oldest cursor: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("initializing cursors: %w", err)
	}
	return nil
}

// ingestLiveLedgers continuously processes ledgers starting from startLedger,
// updating cursors and metrics after each successful ledger.
func (m *ingestService) ingestLiveLedgers(ctx context.Context, startLedger uint32) error {
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion from ledger: %d", currentLedger)
	for {
		totalStart := time.Now()
		ledgerMeta, ledgerErr := m.getLedgerWithRetry(ctx, m.ledgerBackend, currentLedger)
		if ledgerErr != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, ledgerErr)
		}
		m.metricsService.ObserveIngestionPhaseDuration("get_ledger", time.Since(totalStart).Seconds())

		start := time.Now()
		buffer := indexer.NewIndexerBuffer()
		err := m.processLedger(ctx, ledgerMeta, buffer)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		m.metricsService.ObserveIngestionPhaseDuration("process_ledger", time.Since(start).Seconds())

		dbStart := time.Now()
		numTransactionProcessed := 0
		numOperationProcessed := 0
		err = db.RunInPgxTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			filteredData, innerErr := m.filterParticipantData(ctx, dbTx, buffer)
			if innerErr != nil {
				return fmt.Errorf("filtering participant data for ledger %d: %w", currentLedger, innerErr)
			}

			innerErr = m.ingestProcessedData(ctx, dbTx, filteredData, true)
			if innerErr != nil {
				return fmt.Errorf("ingesting processed data for ledger %d: %w", currentLedger, innerErr)
			}

			innerErr = m.models.IngestStore.Update(ctx, dbTx, m.latestLedgerCursorName, currentLedger)
			if innerErr != nil {
				return fmt.Errorf("updating cursor for ledger %d: %w", currentLedger, innerErr)
			}
			numTransactionProcessed = len(filteredData.txs)
			numOperationProcessed = len(filteredData.ops)
			return nil
		})
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}

		// Record processing metrics
		m.metricsService.ObserveIngestionPhaseDuration("db_insertion", time.Since(dbStart).Seconds())
		totalIngestionDuration := time.Since(totalStart).Seconds()
		m.metricsService.ObserveIngestionDuration(totalIngestionDuration)
		m.metricsService.IncIngestionTransactionsProcessed(numTransactionProcessed)
		m.metricsService.IncIngestionOperationsProcessed(numOperationProcessed)
		m.metricsService.IncIngestionLedgersProcessed(1)
		m.metricsService.SetLatestLedgerIngested(float64(currentLedger))

		log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, totalIngestionDuration)
		currentLedger++
	}
}
