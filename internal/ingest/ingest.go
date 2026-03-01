package ingest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	httphandler "github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

const (
	ServerShutdownTimeout = 10 * time.Second
)

// LedgerBackendType represents the type of ledger backend to use
type LedgerBackendType string

const (
	// LedgerBackendTypeRPC uses RPC to fetch ledgers
	LedgerBackendTypeRPC LedgerBackendType = "rpc"
	// LedgerBackendTypeDatastore uses cloud storage (S3/GCS) to fetch ledgers
	LedgerBackendTypeDatastore LedgerBackendType = "datastore"
)

// StorageBackendConfig holds configuration for the datastore-based ledger backend
type StorageBackendConfig struct {
	DataStoreConfig              datastore.DataStoreConfig                  `toml:"datastore_config"`
	BufferedStorageBackendConfig ledgerbackend.BufferedStorageBackendConfig `toml:"buffered_storage_backend_config"`
}

type Configs struct {
	IngestionMode          string
	LatestLedgerCursorName string
	OldestLedgerCursorName string
	DatabaseURL            string
	ServerPort             int
	StartLedger            int
	EndLedger              int
	LogLevel               logrus.Level
	AppTracker             apptracker.AppTracker
	RPCURL                 string
	Network                string
	NetworkPassphrase      string
	GetLedgersLimit        int
	AdminPort              int
	ArchiveURL             string
	CheckpointFrequency    int
	// LedgerBackendType specifies which backend to use for fetching ledgers
	LedgerBackendType LedgerBackendType
	// DatastoreConfigPath is the path to the TOML config file for datastore backend
	DatastoreConfigPath string
	// SkipTxMeta skips storing transaction metadata (meta_xdr) to reduce storage space
	SkipTxMeta bool
	// SkipTxEnvelope skips storing transaction envelope (envelope_xdr) to reduce storage space
	SkipTxEnvelope bool
	// BackfillWorkers limits concurrent batch processing during backfill.
	// Defaults to runtime.NumCPU(). Lower values reduce RAM usage.
	BackfillWorkers int
	// BackfillBatchSize is the number of ledgers processed per batch during backfill.
	// Defaults to 250. Lower values reduce RAM usage at cost of more DB transactions.
	BackfillBatchSize int
	// BackfillDBInsertBatchSize is the number of ledgers to process before flushing to DB.
	// Defaults to 50. Lower values reduce RAM usage at cost of more DB transactions.
	BackfillDBInsertBatchSize int
	// CatchupThreshold is the number of ledgers behind network tip that triggers fast catchup.
	// Defaults to 100.
	CatchupThreshold int
	// ChunkInterval sets the TimescaleDB chunk time interval for hypertables.
	// Only affects future chunks. Uses PostgreSQL INTERVAL syntax (e.g., "1 day", "7 days").
	ChunkInterval string
	// RetentionPeriod configures automatic data retention. Chunks older than this are dropped.
	// Empty string disables retention. Uses PostgreSQL INTERVAL syntax (e.g., "30 days", "6 months").
	RetentionPeriod string
	// CompressionScheduleInterval controls how frequently the compression policy job runs.
	// Uses PostgreSQL INTERVAL syntax (e.g., "4 hours", "12 hours"). Empty string skips configuration.
	CompressionScheduleInterval string
	// CompressAfter controls how long after a chunk is closed before it becomes eligible for compression.
	// Uses PostgreSQL INTERVAL syntax (e.g., "1 hour", "12 hours"). Empty string skips configuration.
	CompressAfter string
}

func Ingest(cfg Configs) error {
	ctx := context.Background()

	ingestService, err := setupDeps(cfg)
	if err != nil {
		log.Ctx(ctx).Fatalf("Error setting up dependencies for ingest: %v", err)
	}

	if err = ingestService.Run(ctx, uint32(cfg.StartLedger), uint32(cfg.EndLedger)); err != nil {
		log.Ctx(ctx).Fatalf("running 'ingest' from %d to %d: %v", cfg.StartLedger, cfg.EndLedger, err)
	}

	return nil
}

func setupDeps(cfg Configs) (services.IngestService, error) {
	ctx := context.Background()

	var dbConnectionPool *pgxpool.Pool
	var err error
	switch cfg.IngestionMode {
	// Use optimized connection pool for backfill mode with async commit and increased work_mem
	case services.IngestionModeBackfill:
		dbConnectionPool, err = db.OpenDBConnectionPoolForBackfill(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("connecting to the database (backfill mode): %w", err)
		}

		// Disable FK constraint checking for faster inserts (requires elevated privileges)
		if fkErr := db.ConfigureBackfillSession(ctx, dbConnectionPool); fkErr != nil {
			log.Ctx(ctx).Warnf("Could not disable FK checks (may require superuser privileges): %v", fkErr)
			// Continue anyway - other optimizations (async commit, work_mem) still apply
		} else {
			log.Ctx(ctx).Info("Backfill session configured: FK checks disabled, async commit enabled")
		}
	default:
		dbConnectionPool, err = db.OpenDBConnectionPool(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("connecting to the database: %w", err)
		}
	}

	if cfg.IngestionMode == services.IngestionModeLive {
		if err := configureHypertableSettings(ctx, dbConnectionPool, cfg.ChunkInterval, cfg.RetentionPeriod, cfg.OldestLedgerCursorName, cfg.CompressionScheduleInterval, cfg.CompressAfter); err != nil {
			return nil, fmt.Errorf("configuring hypertable settings: %w", err)
		}
	}

	metricsService := metrics.NewMetricsService()
	models, err := data.NewModels(dbConnectionPool, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating models: %w", err)
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, httpClient, metricsService)
	if err != nil {
		return nil, fmt.Errorf("instantiating rpc service: %w", err)
	}

	ledgerBackend, err := NewLedgerBackend(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("creating ledger backend: %w", err)
	}

	chAccStore := store.NewChannelAccountModel(dbConnectionPool)

	contractValidator := services.NewContractValidator()

	// Create pond pool for contract metadata fetching
	contractMetadataPool := pond.NewPool(0)
	metricsService.RegisterPoolMetrics("contract_metadata", contractMetadataPool)

	// Create ContractMetadataService for fetching and storing token metadata
	contractMetadataService, err := services.NewContractMetadataService(rpcService, models.Contract, contractMetadataPool)
	if err != nil {
		return nil, fmt.Errorf("instantiating contract metadata service: %w", err)
	}

	// Initialize history archive once for use by both TokenIngestionService and IngestService
	archive, err := historyarchive.Connect(
		cfg.ArchiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase:   cfg.NetworkPassphrase,
			CheckpointFrequency: uint32(cfg.CheckpointFrequency),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to history archive: %w", err)
	}

	tokenIngestionService := services.NewTokenIngestionService(models.DB, cfg.NetworkPassphrase, archive, contractValidator, contractMetadataService, models.TrustlineAsset, models.TrustlineBalance, models.NativeBalance, models.SACBalance, models.AccountContractTokens, models.Contract)

	// Create a factory function for parallel backfill (each batch needs its own backend)
	ledgerBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		return NewLedgerBackend(ctx, cfg)
	}

	ingestService, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:             cfg.IngestionMode,
		Models:                    models,
		LatestLedgerCursorName:    cfg.LatestLedgerCursorName,
		OldestLedgerCursorName:    cfg.OldestLedgerCursorName,
		AppTracker:                cfg.AppTracker,
		RPCService:                rpcService,
		LedgerBackend:             ledgerBackend,
		LedgerBackendFactory:      ledgerBackendFactory,
		ChannelAccountStore:       chAccStore,
		TokenIngestionService:     tokenIngestionService,
		ContractMetadataService:   contractMetadataService,
		MetricsService:            metricsService,
		GetLedgersLimit:           cfg.GetLedgersLimit,
		Network:                   cfg.Network,
		NetworkPassphrase:         cfg.NetworkPassphrase,
		Archive:                   archive,
		SkipTxMeta:                cfg.SkipTxMeta,
		SkipTxEnvelope:            cfg.SkipTxEnvelope,
		BackfillWorkers:           cfg.BackfillWorkers,
		BackfillBatchSize:         cfg.BackfillBatchSize,
		BackfillDBInsertBatchSize: cfg.BackfillDBInsertBatchSize,
		CatchupThreshold:          cfg.CatchupThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("instantiating ingest service: %w", err)
	}

	// Start ingest server which serves metrics and health check endpoints.
	servers := startServers(cfg, models, rpcService, metricsService)

	// Wait for termination signal to gracefully shut down the servers.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Info("Shutting down servers...")

		ctx, cancel := context.WithTimeout(context.Background(), ServerShutdownTimeout)
		defer cancel()

		for _, server := range servers {
			if err := server.Shutdown(ctx); err != nil {
				log.Errorf("Server forced to shutdown: %v", err)
			}
		}
		log.Info("Servers gracefully stopped")
	}()

	return ingestService, nil
}

// startServers initializes and starts the ingest server which serves metrics and health check endpoints.
// If AdminEndpoint port is configured, also starts a separate admin server for pprof endpoints.
func startServers(cfg Configs, models *data.Models, rpcService services.RPCService, metricsSvc metrics.MetricsService) []*http.Server {
	servers := make([]*http.Server, 0, 2)

	// Start main ingest server with health and metrics endpoints
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.ServerPort),
		Handler: mux,
	}

	healthHandler := httphandler.HealthHandler{
		Models:     models,
		RPCService: rpcService,
		AppTracker: cfg.AppTracker,
	}
	mux.Handle("/ingest-metrics", promhttp.HandlerFor(metricsSvc.GetRegistry(), promhttp.HandlerOpts{}))
	mux.Handle("/health", http.HandlerFunc(healthHandler.GetHealth))

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Ctx(context.Background()).Fatalf("starting ingest server on %s: %v", server.Addr, err)
		}
	}()

	servers = append(servers, server)

	// Start separate admin server for pprof endpoints if configured
	if cfg.AdminPort > 0 {
		adminMux := http.NewServeMux()
		adminServer := &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.AdminPort),
			Handler: adminMux,
		}

		registerAdminHandlers(adminMux)

		go func() {
			log.Ctx(context.Background()).Infof("Starting admin server with pprof endpoints on port %d", cfg.AdminPort)
			if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
				log.Ctx(context.Background()).Fatalf("starting admin server on %s: %v", adminServer.Addr, err)
			}
		}()

		servers = append(servers, adminServer)
	}

	return servers
}

// registerAdminHandlers exposes pprof endpoints at /debug/pprof for profiling.
func registerAdminHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
