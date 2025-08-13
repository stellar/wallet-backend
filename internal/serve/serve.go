package serve

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
)

// blockedOperationTypes is now empty but we're keeping it here in case we want to block specific operations again.
var blockedOperationTypes = []xdr.OperationType{}

type Configs struct {
	Port                    int
	DatabaseURL             string
	ServerBaseURL           string
	ClientAuthPublicKeys    []string
	LogLevel                logrus.Level
	EncryptionPassphrase    string
	NumberOfChannelAccounts int64

	// Horizon
	SupportedAssets                    []entities.Asset
	NetworkPassphrase                  string
	MaxSponsoredBaseReserves           int
	BaseFee                            int
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	// RPC
	RPCURL string

	// Error Tracker
	AppTracker apptracker.AppTracker
}

func Serve(cfg Configs) error {
	ctx := context.Background()
	deps, err := buildHandlerDependencies(ctx, cfg)
	if err != nil {
		return fmt.Errorf("setting up handler dependencies: %w", err)
	}

	// Start RPC health tracking first (channel account service depends on this)
	go deps.ServiceContainer.GetRPCService().TrackRPCServiceHealth(ctx, nil)

	// Ensure channel accounts exist (this must succeed for the server to start)
	err = ensureChannelAccounts(ctx, deps.ServiceContainer.GetChannelAccountService(), cfg.NumberOfChannelAccounts)
	if err != nil {
		return fmt.Errorf("ensuring channel accounts: %w", err)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    NewHandler(deps),
		OnStarting: func() {
			log.Infof("🌐 Starting Wallet Backend server on port %d", cfg.Port)
		},
		OnStopping: func() {
			log.Info("Stopping Wallet Backend server")
		},
	})

	return nil
}

func buildHandlerDependencies(ctx context.Context, cfg Configs) (HandlerDependencies, error) {
	// Create metrics service first
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return HandlerDependencies{}, fmt.Errorf("connecting to the database: %w", err)
	}
	dbInstance, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		return HandlerDependencies{}, fmt.Errorf("getting sqlx db: %w", err)
	}
	metricsService := metrics.NewMetricsService(dbInstance)

	// Create providers
	databaseProvider, err := NewDatabaseProvider(cfg.DatabaseURL, metricsService)
	if err != nil {
		return HandlerDependencies{}, fmt.Errorf("creating database provider: %w", err)
	}

	httpClientProvider := NewHTTPClientProvider()

	var authProvider AuthProvider
	if len(cfg.ClientAuthPublicKeys) > 0 {
		authProvider, err = NewAuthProvider(cfg.ClientAuthPublicKeys)
		if err != nil {
			return HandlerDependencies{}, fmt.Errorf("creating auth provider: %w", err)
		}
	}

	// Build service dependencies
	serviceDeps := ServiceDependencies{
		DatabaseProvider:                   databaseProvider,
		HTTPClientProvider:                 httpClientProvider,
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      cfg.ChannelAccountSignatureClient,
		NetworkPassphrase:                  cfg.NetworkPassphrase,
		BaseFee:                            int64(cfg.BaseFee),
		MaxSponsoredBaseReserves:           cfg.MaxSponsoredBaseReserves,
		EncryptionPassphrase:               cfg.EncryptionPassphrase,
		RPCURL:                             cfg.RPCURL,
		ServerBaseURL:                      cfg.ServerBaseURL,
		SupportedAssets:                    cfg.SupportedAssets,
		AppTracker:                         cfg.AppTracker,
	}

	// Create service container
	serviceContainer, err := NewServiceContainer(ctx, serviceDeps)
	if err != nil {
		return HandlerDependencies{}, fmt.Errorf("creating service container: %w", err)
	}

	return HandlerDependencies{
		ServiceContainer: serviceContainer,
		AuthProvider:     authProvider,
	}, nil
}

func ensureChannelAccounts(ctx context.Context, channelAccountService services.ChannelAccountService, numberOfChannelAccounts int64) error {
	log.Ctx(ctx).Info("Ensuring the number of channel accounts in the database...")
	err := channelAccountService.EnsureChannelAccounts(ctx, numberOfChannelAccounts)
	if err != nil {
		return fmt.Errorf("error ensuring the number of channel accounts: %w", err)
	}
	log.Ctx(ctx).Infof("Ensured that at least %d channel accounts exist in the database", numberOfChannelAccounts)
	return nil
}
