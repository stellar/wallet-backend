package serve

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	resolvers "github.com/stellar/wallet-backend/internal/serve/graphql/resolvers"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	txservices "github.com/stellar/wallet-backend/internal/transactions/services"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/ast"
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
	NumberOfChannelAccounts int

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

type handlerDeps struct {
	Models              *data.Models
	Port                int
	DatabaseURL         string
	RequestAuthVerifier auth.HTTPRequestVerifier
	SupportedAssets     []entities.Asset
	NetworkPassphrase   string

	// Services

	AccountService     services.AccountService
	FeeBumpService     services.FeeBumpService
	MetricsService     metrics.MetricsService
	TransactionService txservices.TransactionService
	RPCService         services.RPCService
	// Error Tracker
	AppTracker apptracker.AppTracker
}

func Serve(cfg Configs) error {
	ctx := context.Background()
	deps, err := initHandlerDeps(ctx, cfg)
	if err != nil {
		return fmt.Errorf("setting up handler dependencies: %w", err)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    handler(deps),
		OnStarting: func() {
			log.Infof("🌐 Starting Wallet Backend server on port %d", cfg.Port)
		},
		OnStopping: func() {
			log.Info("Stopping Wallet Backend server")
		},
	})

	return nil
}

func initHandlerDeps(ctx context.Context, cfg Configs) (handlerDeps, error) {
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("connecting to the database: %w", err)
	}
	db, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("getting sqlx db: %w", err)
	}
	metricsService := metrics.NewMetricsService(db)
	models, err := data.NewModels(dbConnectionPool, metricsService)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("creating models for Serve: %w", err)
	}

	jwtTokenParser, err := auth.NewMultiJWTTokenParser(time.Second*5, cfg.ClientAuthPublicKeys...)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating multi JWT token parser: %w", err)
	}
	requestAuthVerifier := auth.NewHTTPRequestVerifier(jwtTokenParser, auth.DefaultMaxBodySize)

	httpClient := http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, &httpClient, metricsService)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating rpc service: %w", err)
	}
	go rpcService.TrackRPCServiceHealth(ctx, nil)

	channelAccountStore := store.NewChannelAccountModel(dbConnectionPool)

	accountService, err := services.NewAccountService(models, metricsService)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating account service: %w", err)
	}

	feeBumpService, err := services.NewFeeBumpService(services.FeeBumpServiceOptions{
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		BaseFee:                            int64(cfg.BaseFee),
		Models:                             models,
		BlockedOperationsTypes:             blockedOperationTypes,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating fee bump service: %w", err)
	}

	txService, err := txservices.NewTransactionService(txservices.TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      cfg.ChannelAccountSignatureClient,
		ChannelAccountStore:                channelAccountStore,
		RPCService:                         rpcService,
		BaseFee:                            int64(cfg.BaseFee),
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating transaction service: %w", err)
	}

	httpClient = http.Client{Timeout: 30 * time.Second}
	channelAccountService, err := services.NewChannelAccountService(ctx, services.ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         rpcService,
		BaseFee:                            int64(cfg.BaseFee),
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountStore:                store.NewChannelAccountModel(dbConnectionPool),
		PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
		EncryptionPassphrase:               cfg.EncryptionPassphrase,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating channel account service: %w", err)
	}
	go ensureChannelAccounts(ctx, channelAccountService, int64(cfg.NumberOfChannelAccounts))

	return handlerDeps{
		Models:              models,
		RequestAuthVerifier: requestAuthVerifier,
		SupportedAssets:     cfg.SupportedAssets,
		AccountService:      accountService,
		FeeBumpService:      feeBumpService,
		MetricsService:      metricsService,
		RPCService:          rpcService,
		AppTracker:          cfg.AppTracker,
		NetworkPassphrase:   cfg.NetworkPassphrase,
		TransactionService:  txService,
	}, nil
}

func ensureChannelAccounts(ctx context.Context, channelAccountService services.ChannelAccountService, numberOfChannelAccounts int64) {
	log.Ctx(ctx).Info("Ensuring the number of channel accounts in the database...")
	err := channelAccountService.EnsureChannelAccounts(ctx, numberOfChannelAccounts)
	if err != nil {
		log.Ctx(ctx).Errorf("error ensuring the number of channel accounts: %s", err.Error())
		return
	}
	log.Ctx(ctx).Infof("Ensured that at least %d channel accounts exist in the database", numberOfChannelAccounts)
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(log.DefaultLogger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)

	// Add metrics middleware first to capture all requests
	mux.Use(middleware.MetricsMiddleware(deps.MetricsService))
	mux.Use(middleware.RecoverHandler(deps.AppTracker))

	mux.Get("/health", httphandler.HealthHandler{
		Models:     deps.Models,
		RPCService: deps.RPCService,
		AppTracker: deps.AppTracker,
	}.GetHealth)
	mux.Get("/api-metrics", promhttp.HandlerFor(deps.MetricsService.GetRegistry(), promhttp.HandlerOpts{}).ServeHTTP)

	// Authenticated routes
	mux.Group(func(r chi.Router) {
		r.Use(middleware.AuthenticationMiddleware(deps.RequestAuthVerifier, deps.AppTracker, deps.MetricsService))

		r.Route("/graphql", func(r chi.Router) {
			r.Use(middleware.DataloaderMiddleware(deps.Models))

			resolver := resolvers.NewResolver(deps.Models, deps.AccountService, deps.TransactionService)

			srv := gqlhandler.New(
				generated.NewExecutableSchema(
					generated.Config{
						Resolvers: resolver,
					},
				),
			)
			srv.AddTransport(transport.Options{})
			srv.AddTransport(transport.GET{})
			srv.AddTransport(transport.POST{})
			srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
			srv.Use(extension.Introspection{})
			srv.Use(extension.AutomaticPersistedQuery{
				Cache: lru.New[string](100),
			})
			r.Handle("/query", srv)
		})

		r.Route("/tx", func(r chi.Router) {
			accountHandler := &httphandler.AccountHandler{
				FeeBumpService: deps.FeeBumpService,
				AppTracker:     deps.AppTracker,
			}

			r.Post("/create-fee-bump", accountHandler.CreateFeeBumpTransaction)
		})
	})

	return mux
}
