// Package loadtest provides synthetic ledger generation and ingestion for load testing.
// This file contains the runner that ingests synthetic ledgers from a file.
package loadtest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	goloadtest "github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	// loadtestLatestCursor is the cursor name for tracking the latest ingested ledger in loadtest mode.
	loadtestLatestCursor = "loadtest_latest_ledger"
	// serverShutdownTimeout is the timeout for graceful server shutdown.
	serverShutdownTimeout = 10 * time.Second
)

// RunConfig holds configuration for the loadtest runner.
type RunConfig struct {
	LedgersFilePath     string
	LedgerCloseDuration time.Duration
	DatabaseURL         string
	NetworkPassphrase   string
	ServerPort          int
	SkipTxMeta          bool
	SkipTxEnvelope      bool
	StartLedger         uint32
}

// Run executes ingestion from synthetic ledgers file.
func Run(ctx context.Context, cfg RunConfig) error {
	// Setup context with signal handling
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Info("Received shutdown signal, cleaning up...")
		cancel()
	}()

	// Setup dependencies
	dbPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	sqlxDB, err := dbPool.SqlxDB(ctx)
	if err != nil {
		return fmt.Errorf("getting sqlx db: %w", err)
	}

	metricsService := metrics.NewMetricsService(sqlxDB)
	models, err := data.NewModels(dbPool, metricsService)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	// Create ledger backend
	backend := ingest.NewLoadtestLedgerBackend(ingest.LoadtestBackendConfig{
		NetworkPassphrase:   cfg.NetworkPassphrase,
		LedgersFilePath:     cfg.LedgersFilePath,
		LedgerCloseDuration: cfg.LedgerCloseDuration,
		DatastoreConfigPath: "config/datastore-pubnet.toml",
	})
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Warnf("Error closing ledger backend: %v", closeErr)
		}
	}()

	// Create indexer with worker pool
	indexerPool := pond.NewPool(0)
	metricsService.RegisterPoolMetrics("loadtest_indexer", indexerPool)
	ledgerIndexer := indexer.NewIndexer(cfg.NetworkPassphrase, indexerPool, metricsService, cfg.SkipTxMeta, cfg.SkipTxEnvelope)

	// Start metrics server
	servers := startServers(cfg, metricsService)
	defer shutdownServers(servers)

	// Initialize cursor
	if err := initializeCursor(ctx, models, loadtestLatestCursor); err != nil {
		return fmt.Errorf("initializing cursor: %w", err)
	}

	// Run ingestion loop
	return runIngestionLoop(ctx, cfg, backend, ledgerIndexer, models, metricsService)
}

// initializeCursor ensures the loadtest cursor exists with value 0.
func initializeCursor(ctx context.Context, models *data.Models, cursorName string) error {
	_, err := models.IngestStore.Get(ctx, cursorName)
	if err != nil {
		// Cursor doesn't exist, create it
		txErr := db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
			return models.IngestStore.Update(ctx, dbTx, cursorName, 0)
		})
		if txErr != nil {
			return fmt.Errorf("creating cursor %s: %w", cursorName, txErr)
		}
	}
	return nil
}

// runIngestionLoop processes ledgers until the loadtest backend signals completion.
func runIngestionLoop(
	ctx context.Context,
	cfg RunConfig,
	backend ledgerbackend.LedgerBackend,
	ledgerIndexer *indexer.Indexer,
	models *data.Models,
	metricsService metrics.MetricsService,
) error {
	// Prepare unbounded range - the backend will signal completion when file is exhausted
	ledgerRange := ledgerbackend.UnboundedRange(cfg.StartLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing ledger range: %w", err)
	}

	currentLedger := cfg.StartLedger
	totalStart := time.Now()
	ledgersProcessed := 0
	txsProcessed := 0
	opsProcessed := 0

	log.Infof("Starting loadtest ingestion from ledger %d", cfg.StartLedger)

	for {
		select {
		case <-ctx.Done():
			return printSummary(ledgersProcessed, txsProcessed, opsProcessed, totalStart)
		default:
		}

		// Get ledger from backend
		ledgerStart := time.Now()
		ledgerMeta, err := backend.GetLedger(ctx, currentLedger)
		if errors.Is(err, goloadtest.ErrLoadTestDone) {
			log.Info("Loadtest complete - all ledgers processed")
			return printSummary(ledgersProcessed, txsProcessed, opsProcessed, totalStart)
		}
		if err != nil {
			return fmt.Errorf("getting ledger %d: %w", currentLedger, err)
		}
		metricsService.ObserveIngestionPhaseDuration("get_ledger", time.Since(ledgerStart).Seconds())

		// Process ledger
		ingestStart := time.Now()
		processStart := time.Now()
		buffer := indexer.NewIndexerBuffer()
		if _, err := indexer.ProcessLedger(ctx, cfg.NetworkPassphrase, ledgerMeta, ledgerIndexer, buffer); err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		metricsService.ObserveIngestionPhaseDuration("process_ledger", time.Since(processStart).Seconds())

		// Write to database
		dbStart := time.Now()
		numTxs, numOps, err := persistLedgerData(ctx, models, buffer, currentLedger)
		if err != nil {
			return fmt.Errorf("persisting ledger %d: %w", currentLedger, err)
		}
		metricsService.ObserveIngestionPhaseDuration("db_insertion", time.Since(dbStart).Seconds())

		// Record metrics
		ingestionDuration := time.Since(ingestStart).Seconds()
		metricsService.ObserveIngestionDuration(ingestionDuration)
		metricsService.IncIngestionLedgersProcessed(1)
		metricsService.IncIngestionTransactionsProcessed(numTxs)
		metricsService.IncIngestionOperationsProcessed(numOps)
		metricsService.SetLatestLedgerIngested(float64(currentLedger))

		ledgersProcessed++
		txsProcessed += numTxs
		opsProcessed += numOps

		log.Infof("Ingested ledger %d (%d txs, %d ops) in %.3fs",
			currentLedger, numTxs, numOps, ingestionDuration)

		currentLedger++
	}
}

// persistLedgerData writes the processed data to the database.
func persistLedgerData(ctx context.Context, models *data.Models, buffer *indexer.IndexerBuffer, ledgerSeq uint32) (int, int, error) {
	txs := buffer.GetTransactions()
	ops := buffer.GetOperations()
	stateChanges := buffer.GetStateChanges()

	err := db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
		// Insert transactions
		if len(txs) > 0 {
			if _, err := models.Transactions.BatchCopy(ctx, dbTx, txs, buffer.GetTransactionsParticipants()); err != nil {
				return fmt.Errorf("inserting transactions: %w", err)
			}
		}

		// Insert operations
		if len(ops) > 0 {
			if _, err := models.Operations.BatchCopy(ctx, dbTx, ops, buffer.GetOperationsParticipants()); err != nil {
				return fmt.Errorf("inserting operations: %w", err)
			}
		}

		// Insert state changes
		if len(stateChanges) > 0 {
			if _, err := models.StateChanges.BatchCopy(ctx, dbTx, stateChanges); err != nil {
				return fmt.Errorf("inserting state changes: %w", err)
			}
		}

		// Update cursor
		if err := models.IngestStore.Update(ctx, dbTx, loadtestLatestCursor, ledgerSeq); err != nil {
			return fmt.Errorf("updating cursor: %w", err)
		}

		return nil
	})

	return len(txs), len(ops), err
}

// printSummary logs final statistics and returns nil.
func printSummary(ledgers, txs, ops int, start time.Time) error {
	duration := time.Since(start)
	log.Info("=== Loadtest Summary ===")
	log.Infof("Total ledgers processed: %d", ledgers)
	log.Infof("Total transactions: %d", txs)
	log.Infof("Total operations: %d", ops)
	log.Infof("Total duration: %v", duration)
	if ledgers > 0 {
		log.Infof("Average time per ledger: %v", duration/time.Duration(ledgers))
		log.Infof("Ledgers per second: %.2f", float64(ledgers)/duration.Seconds())
	}
	return nil
}

// startServers starts the metrics and admin HTTP servers.
func startServers(cfg RunConfig, metricsService metrics.MetricsService) []*http.Server {
	servers := make([]*http.Server, 0, 2)

	// Metrics server
	mux := http.NewServeMux()
	mux.Handle("/ingest-metrics", promhttp.HandlerFor(metricsService.GetRegistry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.ServerPort),
		Handler: mux,
	}

	go func() {
		log.Infof("Starting metrics server on port %d", cfg.ServerPort)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Errorf("Metrics server error: %v", err)
		}
	}()
	servers = append(servers, server)
	return servers
}

// shutdownServers gracefully shuts down all HTTP servers.
func shutdownServers(servers []*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()

	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			log.Errorf("Error shutting down server: %v", err)
		}
	}
}
