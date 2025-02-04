package ingest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
)

type Configs struct {
	DatabaseURL                   string
	LedgerCursorName              string
	StartLedger                   int
	EndLedger                     int
	LogLevel                      logrus.Level
	AppTracker                    apptracker.AppTracker
	RPCURL                        string
	WebhookChannelMaxBufferSize   int
	WebhookChannelMaxWorkers      int
	WebhookChannelMaxRetries      int
	WebhookChannelWaitBtwnTriesMS int
	WebhookChannel                tss.Channel
}

func Ingest(cfg Configs) error {
	ctx := context.Background()

	ingestService, err := setupDeps(cfg)
	if err != nil {
		log.Ctx(ctx).Fatalf("Error setting up dependencies for ingest: %v", err)
	}

	if err = ingestService.Run(ctx, uint32(cfg.StartLedger), uint32(cfg.EndLedger)); err != nil {
		log.Ctx(ctx).Fatalf("Running ingest from %d to %d: %v", cfg.StartLedger, cfg.EndLedger, err)
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
	models, err := data.NewModels(dbConnectionPool)
	if err != nil {
		return nil, fmt.Errorf("creating models: %w", err)
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, httpClient, metricsService)
	if err != nil {
		return nil, fmt.Errorf("instantiating rpc service: %w", err)
	}
	go rpcService.TrackRPCServiceHealth(context.Background())
	tssStore, err := tssstore.NewStore(dbConnectionPool)
	if err != nil {
		return nil, fmt.Errorf("instantiating tss store: %w", err)
	}
	tssRouterConfig := tssrouter.RouterConfigs{
		WebhookChannel: cfg.WebhookChannel,
	}

	router := tssrouter.NewRouter(tssRouterConfig)

	ingestService, err := services.NewIngestService(
		models, cfg.LedgerCursorName, cfg.AppTracker, rpcService, router, tssStore, metricsService)
	if err != nil {
		return nil, fmt.Errorf("instantiating ingest service: %w", err)
	}

	http.Handle("/ingest-metrics", promhttp.HandlerFor(metricsService.GetRegistry(), promhttp.HandlerOpts{}))
	go func() {
		err := http.ListenAndServe(":8002", nil)
		if err != nil {
			log.Ctx(context.Background()).Fatalf("starting ingest metrics server: %v", err)
		}
	}()

	return ingestService, nil
}
