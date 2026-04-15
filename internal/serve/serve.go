package serve

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	supporthttp "github.com/stellar/go-stellar-sdk/support/http"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphqlutils "github.com/stellar/wallet-backend/internal/serve/graphql"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	resolvers "github.com/stellar/wallet-backend/internal/serve/graphql/resolvers"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	complexityreporter "github.com/basemachina/gqlgen-complexity-reporter"
	"github.com/vektah/gqlparser/v2/ast"
)

type Configs struct {
	Port                        int
	DatabaseURL                 string
	ServerBaseURL               string
	ClientAuthPublicKeys        []string
	ClientAuthMaxTimeoutSeconds int
	ClientAuthMaxBodySizeBytes  int
	LogLevel                    logrus.Level
	EncryptionPassphrase        string
	NumberOfChannelAccounts     int
	// Horizon
	SupportedAssets                    []entities.Asset
	NetworkPassphrase                  string
	MaxSponsoredBaseReserves           int
	BaseFee                            int
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient

	// RPC
	RPCURL string

	// GraphQL
	GraphQLComplexityLimit   int
	MaxGraphQLWorkerPoolSize int

	// Error Tracker
	AppTracker apptracker.AppTracker

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
	// Use Exec mode to avoid server-side prepared statement caching, which
	// conflicts with PgBouncer in transaction pooling mode (SQLSTATE 42P05).
	cfg.QueryExecMode = pgx.QueryExecModeExec
	return cfg
}

type handlerDeps struct {
	Models              *data.Models
	Port                int
	DatabaseURL         string
	RequestAuthVerifier auth.HTTPRequestVerifier
	SupportedAssets     []entities.Asset
	NetworkPassphrase   string

	// Services
	FeeBumpService             services.FeeBumpService
	Metrics                    *metrics.Metrics
	TransactionService         services.TransactionService
	RPCService                 services.RPCService
	TrustlineBalanceModel      data.TrustlineBalanceModelInterface
	NativeBalanceModel         data.NativeBalanceModelInterface
	SACBalanceModel            data.SACBalanceModelInterface
	AccountContractTokensModel data.AccountContractTokensModelInterface
	ContractMetadataService    services.ContractMetadataService

	// GraphQL
	GraphQLComplexityLimit   int
	MaxGraphQLWorkerPoolSize int

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
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, cfg.DatabaseURL, cfg.BuildPoolConfig())
	if err != nil {
		return handlerDeps{}, fmt.Errorf("connecting to the database: %w", err)
	}
	m := metrics.NewMetrics(prometheus.NewRegistry())
	metrics.RegisterDBPoolMetrics(m.Registry(), dbConnectionPool)
	models, err := data.NewModels(dbConnectionPool, m.DB)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("creating models for Serve: %w", err)
	}

	jwtTokenParser, err := auth.NewMultiJWTTokenParser(time.Duration(cfg.ClientAuthMaxTimeoutSeconds)*time.Second, cfg.ClientAuthPublicKeys...)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating multi JWT token parser: %w", err)
	}
	requestAuthVerifier := auth.NewHTTPRequestVerifier(jwtTokenParser, int64(cfg.ClientAuthMaxBodySizeBytes))

	httpClient := http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, &httpClient, m.RPC)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating rpc service: %w", err)
	}

	channelAccountStore := store.NewChannelAccountModel(dbConnectionPool)

	feeBumpService, err := services.NewFeeBumpService(services.FeeBumpServiceOptions{
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		BaseFee:                            int64(cfg.BaseFee),
		Models:                             models,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating fee bump service: %w", err)
	}

	// AccountTokens model used directly for reading trustlines and contracts

	contractMetadataService, err := services.NewContractMetadataService(rpcService, models.Contract, pond.NewPool(0))
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating contract metadata service: %w", err)
	}

	txService, err := services.NewTransactionService(services.TransactionServiceOptions{
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
		ChannelAccountSignatureClient:      cfg.ChannelAccountSignatureClient,
		ChannelAccountStore:                store.NewChannelAccountModel(dbConnectionPool),
		PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
		EncryptionPassphrase:               cfg.EncryptionPassphrase,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating channel account service: %w", err)
	}
	go ensureChannelAccounts(ctx, channelAccountService, int64(cfg.NumberOfChannelAccounts))

	return handlerDeps{
		Models:                     models,
		RequestAuthVerifier:        requestAuthVerifier,
		SupportedAssets:            cfg.SupportedAssets,
		FeeBumpService:             feeBumpService,
		Metrics:                    m,
		RPCService:                 rpcService,
		TrustlineBalanceModel:      models.TrustlineBalance,
		NativeBalanceModel:         models.NativeBalance,
		SACBalanceModel:            models.SACBalance,
		AccountContractTokensModel: models.AccountContractTokens,
		ContractMetadataService:    contractMetadataService,
		AppTracker:                 cfg.AppTracker,
		NetworkPassphrase:          cfg.NetworkPassphrase,
		TransactionService:         txService,
		GraphQLComplexityLimit:     cfg.GraphQLComplexityLimit,
		MaxGraphQLWorkerPoolSize:   cfg.MaxGraphQLWorkerPoolSize,
	}, nil
}

func ensureChannelAccounts(ctx context.Context, channelAccountService services.ChannelAccountService, numberOfChannelAccounts int64) {
	log.Ctx(ctx).Info("Ensuring the number of channel accounts in the database...")
	err := channelAccountService.EnsureChannelAccounts(ctx, numberOfChannelAccounts)
	if err != nil {
		log.Ctx(ctx).Errorf("error ensuring the number of channel accounts: %s", err.Error())
		return
	}
	log.Ctx(ctx).Infof("Ensured that exactly %d channel accounts exist in the database", numberOfChannelAccounts)
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(log.DefaultLogger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)

	// Add metrics middleware first to capture all requests
	mux.Use(middleware.MetricsMiddleware(deps.Metrics.HTTP))
	mux.Use(middleware.RecoverHandler(deps.AppTracker))

	mux.Get("/health", httphandler.HealthHandler{
		Models:     deps.Models,
		RPCService: deps.RPCService,
		AppTracker: deps.AppTracker,
	}.GetHealth)
	mux.Get("/api-metrics", promhttp.HandlerFor(deps.Metrics.Registry(), promhttp.HandlerOpts{}).ServeHTTP)

	// API routes (conditionally authenticated)
	mux.Group(func(r chi.Router) {
		// Apply authentication middleware only if auth verifier is configured
		if deps.RequestAuthVerifier != nil {
			r.Use(middleware.AuthenticationMiddleware(deps.RequestAuthVerifier, deps.AppTracker, deps.Metrics.Auth))
		}

		r.Route("/graphql", func(r chi.Router) {
			r.Use(middleware.DataloaderMiddleware(deps.Models))

			resolver := resolvers.NewResolver(
				deps.Models,
				deps.TransactionService,
				deps.FeeBumpService,
				deps.RPCService,
				resolvers.NewBalanceReader(deps.TrustlineBalanceModel, deps.NativeBalanceModel, deps.SACBalanceModel),
				deps.AccountContractTokensModel,
				deps.ContractMetadataService,
				deps.Metrics,
				resolvers.ResolverConfig{
					MaxWorkerPoolSize: deps.MaxGraphQLWorkerPoolSize,
				},
			)

			config := generated.Config{
				Resolvers: resolver,
			}
			addComplexityCalculation(&config)
			srv := gqlhandler.New(
				generated.NewExecutableSchema(
					config,
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
			srv.SetErrorPresenter(graphqlutils.CustomErrorPresenter)
			srv.Use(extension.FixedComplexityLimit(deps.GraphQLComplexityLimit))

			// Add complexity logging - reports all queries with their complexity values
			reporter := middleware.NewComplexityLogger(deps.Metrics.GraphQL)
			srv.Use(complexityreporter.NewExtension(reporter))

			// Add operation-level metrics (duration, in-flight, throughput, errors, response size)
			opMetrics := middleware.NewGraphQLOperationMetrics(deps.Metrics.GraphQL)
			srv.AroundOperations(opMetrics.Middleware)

			// Add field-level deprecated field tracking
			fieldMetrics := middleware.NewGraphQLFieldMetrics(deps.Metrics.GraphQL)
			srv.AroundFields(fieldMetrics.Middleware)

			r.Handle("/query", srv)
		})
	})

	return mux
}

func addComplexityCalculation(config *generated.Config) {
	/*
		Complexity Calculation
		--------------------------------
		Complexity is a measure of the computational cost of a query.
		It is used to determine the performance of a query and to prevent
		queries that are too complex from being executed.

		By default, graphql assigns a complexity of 1 to each field. This means that a query with 10 fields will have a complexity of 10.
		However, we also want to take into account the number of items requested for paginated queries. So we use the first/last parameters
		to calculate the final complexity.

		For example, for the following query, the complexity is calculated as follows:
		--------------------------------
		transactions(first: 10) {
				edges {
					node {
						hash
						operations(first: 2) {
							edges {
								node {
									id
									stateChanges(first: 5) {
										edges {
											node {
												stateChangeCategory
												stateChangeReason
											}
										}
									}
								}
							}
						}
					}
				}
			}
		--------------------------------
		Complexity = 10*(1+1+1+2*(1+1+1+5*(1+1+1+1))) = 490
		--------------------------------
	*/
	calculatePaginatedComplexity := func(childComplexity int, first *int32, last *int32) int {
		// Use the same default page size as resolver execution when pagination args are omitted.
		limit := int(graphqlutils.DefaultPageLimit)
		if first != nil {
			limit = int(*first)
		} else if last != nil {
			limit = int(*last)
		}
		return childComplexity * limit
	}
	paginatedQueryComplexityFunc := func(childComplexity int, first *int32, _ *string, last *int32, _ *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Query.Transactions = paginatedQueryComplexityFunc
	config.Complexity.Query.Operations = paginatedQueryComplexityFunc
	config.Complexity.Query.StateChanges = paginatedQueryComplexityFunc
	config.Complexity.Account.Transactions = func(childComplexity int, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Account.Operations = func(childComplexity int, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Account.StateChanges = func(childComplexity int, filter *generated.AccountStateChangeFilterInput, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Transaction.Operations = paginatedQueryComplexityFunc
	config.Complexity.Transaction.StateChanges = paginatedQueryComplexityFunc
	config.Complexity.Operation.StateChanges = paginatedQueryComplexityFunc
}
