package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	httphandler "github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers SEP-41 validator + processor via init()
)

const (
	ServerShutdownTimeout = 10 * time.Second
	// wasmExtractorCloseTimeout bounds releasing the wazero runtime on shutdown.
	// It runs on a fresh context (not the cancelled root ctx) since the close
	// itself has nothing to do with the signal that triggered shutdown.
	wasmExtractorCloseTimeout = 5 * time.Second
)

// LedgerBackendType represents the type of ledger backend to use
type LedgerBackendType string

const (
	// LedgerBackendTypeRPC uses RPC to fetch ledgers
	LedgerBackendTypeRPC LedgerBackendType = "rpc"
	// LedgerBackendTypeDatastore uses cloud storage (S3/GCS) to fetch ledgers
	LedgerBackendTypeDatastore LedgerBackendType = "datastore"
)

type Configs struct {
	IngestionMode          string
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
	// Datastore holds the datastore ledger backend configuration (flag/env driven).
	Datastore DatastoreConfig
	// BackfillWorkers limits concurrent batch processing during backfill.
	// Defaults to runtime.NumCPU(). Lower values reduce RAM usage.
	BackfillWorkers int
	// BackfillBatchSize is the number of ledgers processed per batch during backfill.
	// Defaults to 250. Lower values reduce RAM usage at cost of more DB transactions.
	BackfillBatchSize int
	// BackfillDBInsertBatchSize is the number of ledgers to process before flushing to DB.
	// Defaults to 50. Lower values reduce RAM usage at cost of more DB transactions.
	BackfillDBInsertBatchSize int
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
	// MaxChunksToCompress limits how many chunks each compression policy job run processes.
	// 0 means unlimited (TimescaleDB default). Set to a small value (e.g. 10) during
	// backfill to prevent long-running jobs from blocking their next scheduled start.
	MaxChunksToCompress int
	// DB pool tuning — all default to db.Default* constants when zero.
	DBMaxConns        int
	DBMinConns        int
	DBMaxConnLifetime time.Duration
	DBMaxConnIdleTime time.Duration
}

func (c Configs) BuildPoolConfig() db.PoolConfig {
	cfg := db.DefaultPoolConfig()
	if c.DBMaxConns > 0 {
		cfg.MaxConns = int32(c.DBMaxConns)
	}
	if c.DBMinConns > 0 {
		cfg.MinConns = int32(c.DBMinConns)
	}
	if c.DBMaxConnLifetime > 0 {
		cfg.MaxConnLifetime = c.DBMaxConnLifetime
	}
	if c.DBMaxConnIdleTime > 0 {
		cfg.MaxConnIdleTime = c.DBMaxConnIdleTime
	}
	return cfg
}

func Ingest(cfg Configs) error {
	// A SIGINT/SIGTERM cancels this root context, which propagates into the ingest
	// loop and the in-flight ledger's transaction, so that ledger is rolled back
	// rather than committed. Ingestion is idempotent and gap-driven: the rolled-back
	// ledger is simply re-fetched and re-ingested on the next startup, so no partial
	// state is ever persisted. Cleanup (deferred below) then drains the servers and
	// tears down the remaining resources in order.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ingestService, cleanup, err := setupDeps(ctx, cfg)
	if err != nil {
		return fmt.Errorf("setting up dependencies for ingest: %w", err)
	}
	defer cleanup()

	runErr := ingestService.Run(ctx, uint32(cfg.StartLedger), uint32(cfg.EndLedger))
	if runErr != nil {
		if isShutdownRequested(ctx, runErr) {
			log.Ctx(ctx).Infof("shutdown requested; exiting cleanly: %v", runErr)
			return nil
		}
		return fmt.Errorf("running 'ingest' from %d to %d: %w", cfg.StartLedger, cfg.EndLedger, runErr)
	}

	return nil
}

// isShutdownRequested classifies a Run error as a clean-exit shutdown (root
// ctx cancelled by SIGINT/SIGTERM) versus a genuine failure. ctx is checked
// directly rather than just err, since a cancellation can surface through
// several different wrapped errors depending on where in the ingest loop it
// was observed.
func isShutdownRequested(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// setupDeps builds the ingest service and its dependencies. The returned
// cleanup func releases resources owned at this layer (ledger backend, wasm
// spec extractor, DB pool) and must be called by the caller after Run
// returns.
func setupDeps(ctx context.Context, cfg Configs) (services.IngestService, func(), error) {
	poolCfg := cfg.BuildPoolConfig()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, cfg.DatabaseURL, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to the database: %w", err)
	}

	// Retention and compression policies live in shared database state, so only
	// the live deployment configures them. A backfill runs with ad-hoc flags and
	// must not mutate or remove the policies the live pods rely on; and an active
	// retention policy would drop the very history a backfill is writing.
	if cfg.IngestionMode == services.IngestionModeLive {
		if err := configureHypertableSettings(ctx, dbConnectionPool, cfg.ChunkInterval, cfg.RetentionPeriod, cfg.OldestLedgerCursorName, cfg.CompressionScheduleInterval, cfg.CompressAfter, cfg.MaxChunksToCompress); err != nil {
			return nil, nil, fmt.Errorf("configuring hypertable settings: %w", err)
		}
	}

	m := metrics.NewMetrics(prometheus.NewRegistry())
	metrics.RegisterDBPoolMetrics(m.Registry(), dbConnectionPool)
	models, err := data.NewModels(dbConnectionPool, m.DB)
	if err != nil {
		return nil, nil, fmt.Errorf("creating models: %w", err)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, httpClient, m.RPC)
	if err != nil {
		return nil, nil, fmt.Errorf("instantiating rpc service: %w", err)
	}

	ledgerBackend, err := NewLedgerBackend(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating ledger backend: %w", err)
	}

	// Create pond pool for contract metadata fetching, bounded to the batch size
	// ContractMetadataService itself submits per round-trip (pond.NewPool(0) is unbounded).
	contractMetadataPool := pond.NewPool(services.SimulateTransactionBatchSize)
	metrics.RegisterPoolMetrics(m.Registry(), "contract_metadata", contractMetadataPool)

	// Create ContractMetadataService for fetching and storing token metadata
	contractMetadataService, err := services.NewContractMetadataService(rpcService, models.Contract, contractMetadataPool)
	if err != nil {
		return nil, nil, fmt.Errorf("instantiating contract metadata service: %w", err)
	}

	// Build a single ProtocolDeps to pass through both the validator and
	// processor registries. cmd/ingest knows nothing about specific
	// protocols — adding a new one is a blank import elsewhere plus a SQL
	// migration.
	protocolDeps := services.ProtocolDeps{
		NetworkPassphrase:       cfg.NetworkPassphrase,
		Models:                  models,
		RPCService:              rpcService,
		ContractMetadataService: contractMetadataService,
		MetricsService:          m,
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
		return nil, nil, fmt.Errorf("connecting to history archive: %w", err)
	}

	tokenIngestionService := services.NewTokenIngestionService(services.TokenIngestionServiceConfig{
		TrustlineBalanceModel:     models.TrustlineBalance,
		NativeBalanceModel:        models.NativeBalance,
		SACBalanceModel:           models.SACBalance,
		LiquidityPoolModel:        models.LiquidityPool,
		LiquidityPoolBalanceModel: models.LiquidityPoolBalance,
		NetworkPassphrase:         cfg.NetworkPassphrase,
	})

	checkpointService := services.NewCheckpointService(services.CheckpointServiceConfig{
		DB:                        models.DB,
		Archive:                   archive,
		ContractMetadataService:   contractMetadataService,
		TrustlineAssetModel:       models.TrustlineAsset,
		TrustlineBalanceModel:     models.TrustlineBalance,
		NativeBalanceModel:        models.NativeBalance,
		SACBalanceModel:           models.SACBalance,
		LiquidityPoolModel:        models.LiquidityPool,
		LiquidityPoolBalanceModel: models.LiquidityPoolBalance,
		ContractModel:             models.Contract,
		ProtocolWasmsModel:        models.ProtocolWasms,
		ProtocolContractsModel:    models.ProtocolContracts,
		NetworkPassphrase:         cfg.NetworkPassphrase,
	})

	// Create a factory function for parallel backfill (each batch needs its own backend)
	ledgerBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		return NewLedgerBackend(ctx, cfg)
	}

	// Resolve protocol processors and validators from the registries.
	// Both share ProtocolDeps so the cmd layer never names a specific
	// protocol or its dependencies.
	allProtocolIDs := services.GetAllProcessorIDs()
	protocolProcessors, err := services.BuildProcessors(protocolDeps, allProtocolIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("building protocol processors: %w", err)
	}

	allValidatorIDs := services.GetAllValidatorIDs()
	protocolValidators, err := services.BuildValidators(protocolDeps, allValidatorIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("building protocol validators: %w", err)
	}

	// Spec extractor is owned by the ingest service so it lives for the
	// process lifetime; it is closed by the returned cleanup func, after
	// Run has returned and no extraction can be in flight.
	wasmExtractor := services.NewWasmSpecExtractor()

	ingestService, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:          cfg.IngestionMode,
		Models:                 models,
		OldestLedgerCursorName: cfg.OldestLedgerCursorName,
		AppTracker:             cfg.AppTracker,
		RPCService:             rpcService,
		LedgerBackend:          ledgerBackend,
		LedgerBackendFactory:   ledgerBackendFactory,
		// Never matches for the RPC backend (it never wraps ErrBufferDead), so this is safe
		// to wire unconditionally rather than branching on cfg.LedgerBackendType.
		IsPermanentFetchError:     func(err error) bool { return errors.Is(err, ErrBufferDead) },
		TokenIngestionService:     tokenIngestionService,
		CheckpointService:         checkpointService,
		Metrics:                   m,
		GetLedgersLimit:           cfg.GetLedgersLimit,
		Network:                   cfg.Network,
		NetworkPassphrase:         cfg.NetworkPassphrase,
		Archive:                   archive,
		BackfillWorkers:           cfg.BackfillWorkers,
		BackfillBatchSize:         cfg.BackfillBatchSize,
		BackfillDBInsertBatchSize: cfg.BackfillDBInsertBatchSize,
		ProtocolProcessors:        protocolProcessors,
		ProtocolValidators:        protocolValidators,
		WasmSpecExtractor:         wasmExtractor,
		ContractMetadataService:   contractMetadataService,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("instantiating ingest service: %w", err)
	}

	// Start ingest server which serves metrics and health check endpoints.
	servers := startServers(cfg, models, rpcService, m)

	// cleanup tears down every resource owned at this layer, in dependency order,
	// once Run has returned. Servers are drained first — their /health handler
	// reads the DB pool — and the drain is awaited here rather than left to a
	// fire-and-forget goroutine, so it cannot race the pool close below. The
	// worker pools are stopped next (a draining task may still issue DB work),
	// and only then is the shared DB pool closed.
	cleanup := func() {
		log.Info("Shutting down servers...")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), ServerShutdownTimeout)
		defer cancelShutdown()
		for _, server := range servers {
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Errorf("Server forced to shutdown: %v", err)
			}
		}
		log.Info("Servers gracefully stopped")

		// Stop the ingest service's worker pools and the contract-metadata pool
		// before the DB pool, so no pooled task outlives the connection pool.
		ingestService.Close()
		contractMetadataPool.StopAndWait()

		if err := ledgerBackend.Close(); err != nil {
			log.Ctx(ctx).Warnf("closing ledger backend: %v", err)
		}

		wasmCloseCtx, cancelWasm := context.WithTimeout(context.Background(), wasmExtractorCloseTimeout)
		defer cancelWasm()
		if err := wasmExtractor.Close(wasmCloseCtx); err != nil {
			log.Ctx(ctx).Warnf("closing wasm spec extractor: %v", err)
		}

		dbConnectionPool.Close()
	}

	return ingestService, cleanup, nil
}

// startServers initializes and starts the ingest server which serves metrics and health check endpoints.
// If AdminEndpoint port is configured, also starts a separate admin server for pprof endpoints.
func startServers(cfg Configs, models *data.Models, rpcService services.RPCService, m *metrics.Metrics) []*http.Server {
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
	mux.Handle("/ingest-metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
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
