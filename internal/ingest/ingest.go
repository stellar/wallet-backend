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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	httphandler "github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
	cache "github.com/stellar/wallet-backend/internal/store"
)

const (
	ServerShutdownTimeout = 10 * time.Second
)

type Configs struct {
	DatabaseURL       string
	ServerPort        int
	LedgerCursorName  string
	TrustlinesCursorName string
	StartLedger       int
	EndLedger         int
	LogLevel          logrus.Level
	AppTracker        apptracker.AppTracker
	RPCURL            string
	Network           string
	NetworkPassphrase string
	GetLedgersLimit   int
	AdminPort         int
	RedisHost         string
	RedisPort         int
	RedisPassword     string
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
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to the database: %w", err)
	}
	db, err := dbConnectionPool.SqlxDB(context.Background())
	if err != nil {
		return nil, fmt.Errorf("getting sqlx db: %w", err)
	}
	metricsService := metrics.NewMetricsService(db)
	models, err := data.NewModels(dbConnectionPool, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating models: %w", err)
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, httpClient, metricsService)
	if err != nil {
		return nil, fmt.Errorf("instantiating rpc service: %w", err)
	}
	chAccStore := store.NewChannelAccountModel(dbConnectionPool)
	contractStore, err := cache.NewTokenContractStore(models.Contract)
	if err != nil {
		return nil, fmt.Errorf("instantiating contract store: %w", err)
	}

	redisStore := cache.NewRedisStore(cfg.RedisHost, cfg.RedisPort, cfg.RedisPassword)
	trustlinesService, err := services.NewTrustlinesService(cfg.NetworkPassphrase, redisStore)
	if err != nil {
		return nil, fmt.Errorf("instantiating trustlines service: %w", err)
	}

	ingestService, err := services.NewIngestService(
		models, cfg.LedgerCursorName, cfg.TrustlinesCursorName, cfg.AppTracker, rpcService, chAccStore, contractStore, metricsService, trustlinesService, cfg.GetLedgersLimit, cfg.Network)
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
