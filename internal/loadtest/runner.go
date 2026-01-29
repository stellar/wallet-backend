// Package loadtest provides synthetic ledger generation and ingestion for load testing.
// This file contains the runner that ingests synthetic ledgers from a file.
package loadtest

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
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
	SeedDataPath        string // Optional path to SQL file containing seed data
}

// Run executes ingestion from synthetic ledgers file.
func Run(ctx context.Context, cfg RunConfig) error {
	// Setup dependencies
	dbPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbPool.Close() // nolint:errcheck

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
	backend, err := ingest.NewLoadtestLedgerBackend(ctx, ingest.LoadtestBackendConfig{
		NetworkPassphrase:   cfg.NetworkPassphrase,
		LedgersFilePath:     cfg.LedgersFilePath,
		LedgerCloseDuration: cfg.LedgerCloseDuration,
		DatastoreConfigPath: "config/datastore-pubnet.toml",
	})
	if err != nil {
		return fmt.Errorf("creating load test ledger backend: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Warnf("Error closing ledger backend: %v", closeErr)
		}
	}()

	// Create indexer with worker pool
	indexerPool := pond.NewPool(0)
	defer indexerPool.StopAndWait()

	metricsService.RegisterPoolMetrics("loadtest_indexer", indexerPool)
	ledgerIndexer := indexer.NewIndexer(cfg.NetworkPassphrase, indexerPool, metricsService, cfg.SkipTxMeta, cfg.SkipTxEnvelope)

	// Create TokenIngestionService for token change processing
	tokenIngestionService := services.NewTokenIngestionServiceForLoadtest(
		dbPool,
		cfg.NetworkPassphrase,
		models.TrustlineBalance,
		models.NativeBalance,
		models.SACBalance,
		models.AccountContractTokens,
	)

	// Create ingest service for shared persistence logic
	ingestSvc, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:              "loadtest",
		Models:                     models,
		MetricsService:             metricsService,
		NetworkPassphrase:          cfg.NetworkPassphrase,
		TokenIngestionService:      tokenIngestionService,
		ChannelAccountStore:        store.NewChannelAccountModel(dbPool),
		EnableParticipantFiltering: false,
	})
	if err != nil {
		return fmt.Errorf("creating ingest service: %w", err)
	}

	// Start metrics server
	servers := startServers(cfg, metricsService)
	defer shutdownServers(servers)

	// Load seed data, this uses the mainnet tokens
	if err := loadSeedData(ctx, cfg.DatabaseURL, cfg.SeedDataPath); err != nil {
		return fmt.Errorf("loading seed data: %w", err)
	}

	// Initialize cursor
	if err := initializeCursor(ctx, models, loadtestLatestCursor); err != nil {
		return fmt.Errorf("initializing cursor: %w", err)
	}

	// Run ingestion loop
	return runIngestionLoop(ctx, cfg, backend, ledgerIndexer, metricsService, ingestSvc)
}

// initializeCursor ensures the loadtest cursor exists with value 0.
func initializeCursor(ctx context.Context, models *data.Models, cursorName string) error {
	// Cursor doesn't exist, create it
	txErr := db.RunInPgxTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
		return models.IngestStore.Update(ctx, dbTx, cursorName, 0)
	})
	if txErr != nil {
		return fmt.Errorf("creating cursor %s: %w", cursorName, txErr)
	}
	return nil
}

// loadSeedData loads SQL seed data from the specified file path using psql.
// The file should contain pg_dump COPY format data. If seedDataPath is empty, this is a no-op.
func loadSeedData(ctx context.Context, databaseURL string, seedDataPath string) error {
	if seedDataPath == "" {
		return nil
	}

	cmd := exec.CommandContext(ctx, "psql", databaseURL, "-f", seedDataPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("executing psql: %w, output: %s", err, string(output))
	}

	log.Infof("Loaded seed data from %s", seedDataPath)
	return nil
}

// runIngestionLoop processes ledgers until the last available ledger.
func runIngestionLoop(
	ctx context.Context,
	cfg RunConfig,
	backend ledgerbackend.LedgerBackend,
	ledgerIndexer *indexer.Indexer,
	metricsService metrics.MetricsService,
	ingestSvc services.IngestService,
) error {
	// Prepare unbounded range - backend will read all ledgers from file
	ledgerRange := ledgerbackend.UnboundedRange(cfg.StartLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing ledger range: %w", err)
	}

	// Query the actual latest ledger sequence from the backend
	latestSeq, err := backend.GetLatestLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("getting latest ledger sequence: %w", err)
	}

	currentLedger := cfg.StartLedger
	totalStart := time.Now()
	ledgersProcessed := 0
	txsProcessed := 0
	opsProcessed := 0
	var totalIngestionDuration time.Duration

	log.Infof("Starting loadtest ingestion from ledger %d to %d", cfg.StartLedger, latestSeq)

	// Bounded loop - process all ledgers up to and including latestSeq
	for currentLedger <= latestSeq {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		default:
			// fall through and continue normally
		}

		// Get ledger from backend
		ledgerMeta, err := backend.GetLedger(ctx, currentLedger)
		if err != nil {
			return fmt.Errorf("getting ledger %d: %w", currentLedger, err)
		}

		// Process ledger
		ingestStart := time.Now()
		processStart := time.Now()
		buffer := indexer.NewIndexerBuffer()
		_, err = indexer.ProcessLedger(ctx, cfg.NetworkPassphrase, ledgerMeta, ledgerIndexer, buffer)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}
		metricsService.ObserveIngestionPhaseDuration("process_ledger", time.Since(processStart).Seconds())

		// Write to database using shared persistence logic
		dbStart := time.Now()
		numTxs, numOps, err := ingestSvc.PersistLedgerData(ctx, currentLedger, buffer, loadtestLatestCursor)
		if err != nil {
			return fmt.Errorf("persisting ledger %d: %w", currentLedger, err)
		}
		metricsService.ObserveIngestionPhaseDuration("insert_into_db", time.Since(dbStart).Seconds())

		// Record metrics
		ingestionDuration := time.Since(ingestStart)
		totalIngestionDuration += ingestionDuration
		metricsService.ObserveIngestionDuration(ingestionDuration.Seconds())
		metricsService.IncIngestionLedgersProcessed(1)
		metricsService.IncIngestionTransactionsProcessed(numTxs)
		metricsService.IncIngestionOperationsProcessed(numOps)
		metricsService.SetLatestLedgerIngested(float64(currentLedger))

		ledgersProcessed++
		txsProcessed += numTxs
		opsProcessed += numOps

		log.Infof("Ingested ledger %d in %.3fs", currentLedger, ingestionDuration.Seconds())
		currentLedger++
	}

	log.Info("Loadtest complete - all ledgers processed")
	return printSummary(ledgersProcessed, txsProcessed, opsProcessed, totalStart, totalIngestionDuration)
}

// printSummary logs final statistics and returns nil.
func printSummary(ledgers, txs, ops int, start time.Time, totalIngestionDuration time.Duration) error {
	duration := time.Since(start)
	log.Info("=== Loadtest Summary ===")
	log.Infof("Total ledgers processed: %d", ledgers)
	log.Infof("Total transactions: %d", txs)
	log.Infof("Total operations: %d", ops)
	log.Infof("Total duration: %v", duration)
	if ledgers > 0 {
		log.Infof("Total ingestion duration: %v", totalIngestionDuration)
		log.Infof("Average ingestion duration per ledger: %v", totalIngestionDuration/time.Duration(ledgers))
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
