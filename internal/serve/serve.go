package serve

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	supporthttp "github.com/stellar/go-stellar-sdk/support/http"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
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
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	complexityreporter "github.com/basemachina/gqlgen-complexity-reporter"
	"github.com/vektah/gqlparser/v2/ast"
)

type Configs struct {
	Port int
	// AdminPort, when > 0, serves pprof endpoints at /debug/pprof on a separate
	// listener. Mirrors the ingest admin server.
	AdminPort                   int
	DatabaseURL                 string
	ServerBaseURL               string
	ClientAuthPublicKeys        []string
	ClientAuthMaxTimeoutSeconds int
	ClientAuthMaxBodySizeBytes  int
	LogLevel                    logrus.Level
	SupportedAssets             []entities.Asset
	NetworkPassphrase           string

	// RPC
	RPCURL string

	// GraphQL
	GraphQLComplexityLimit int

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
	Metrics                   *metrics.Metrics
	RPCService                services.RPCService
	TrustlineBalanceModel     data.TrustlineBalanceModelInterface
	NativeBalanceModel        data.NativeBalanceModelInterface
	SACBalanceModel           data.SACBalanceModelInterface
	LiquidityPoolBalanceModel data.LiquidityPoolBalanceModelInterface
	SEP41BalanceModel         sep41data.BalanceModelInterface
	SEP41AllowanceModel       sep41data.AllowanceModelInterface

	// GraphQL
	GraphQLComplexityLimit int

	// Error Tracker
	AppTracker apptracker.AppTracker
}

func Serve(cfg Configs) error {
	ctx := context.Background()
	deps, err := initHandlerDeps(ctx, cfg)
	if err != nil {
		return fmt.Errorf("setting up handler dependencies: %w", err)
	}

	// Start separate admin server for pprof endpoints if configured. It is best-effort profiling
	// infra: supporthttp.Run blocks below, and a bind failure here is logged without taking down
	// the main API server.
	if cfg.AdminPort > 0 {
		adminMux := http.NewServeMux()
		registerAdminHandlers(adminMux)
		adminServer := &http.Server{
			Addr:              fmt.Sprintf(":%d", cfg.AdminPort),
			Handler:           adminMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Infof("Starting admin server with pprof endpoints on port %d", cfg.AdminPort)
			if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
				log.Errorf("admin server on %s stopped: %v (pprof disabled; main server unaffected)", adminServer.Addr, err)
			}
		}()
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

// registerAdminHandlers exposes pprof endpoints at /debug/pprof for profiling.
// Mirrors the ingest admin server (internal/ingest/ingest.go).
func registerAdminHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
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

	return handlerDeps{
		Models:                    models,
		RequestAuthVerifier:       requestAuthVerifier,
		SupportedAssets:           cfg.SupportedAssets,
		Metrics:                   m,
		RPCService:                rpcService,
		TrustlineBalanceModel:     models.TrustlineBalance,
		NativeBalanceModel:        models.NativeBalance,
		SACBalanceModel:           models.SACBalance,
		LiquidityPoolBalanceModel: models.LiquidityPoolBalance,
		SEP41BalanceModel:         models.SEP41.Balances,
		SEP41AllowanceModel:       models.SEP41.Allowances,
		AppTracker:                cfg.AppTracker,
		NetworkPassphrase:         cfg.NetworkPassphrase,
		GraphQLComplexityLimit:    cfg.GraphQLComplexityLimit,
	}, nil
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
				deps.RPCService,
				resolvers.NewBalanceReader(deps.TrustlineBalanceModel, deps.NativeBalanceModel, deps.SACBalanceModel, deps.LiquidityPoolBalanceModel, deps.SEP41BalanceModel, deps.SEP41AllowanceModel),
				deps.Metrics,
				resolvers.ResolverConfig{},
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

		By default, graphql assigns a complexity of 1 to each field.
		For paginated connections, the child complexity is multiplied by the
		page size: the explicit first/last argument, or DefaultPageLimit (50)
		when omitted.

		Example — explicit pagination arguments:
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

		Without explicit args the same shape uses DefaultPageLimit (50):
		Complexity = 50*(1+1+1+50*(1+1+1+50*(1+1+1+1))) = 507,650

		Clients should provide explicit first/last arguments to keep
		query complexity within the configured limit (default 5000).
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
	config.Complexity.Account.Balances = paginatedQueryComplexityFunc
	config.Complexity.Account.Transactions = func(childComplexity int, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Account.Operations = func(childComplexity int, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Account.StateChanges = func(childComplexity int, filter *generated.AccountStateChangeFilterInput, since *time.Time, until *time.Time, first *int32, after *string, last *int32, before *string) int {
		return calculatePaginatedComplexity(childComplexity, first, last)
	}
	config.Complexity.Account.Sep41Allowances = paginatedQueryComplexityFunc
	config.Complexity.Transaction.Operations = paginatedQueryComplexityFunc
	config.Complexity.Transaction.StateChanges = paginatedQueryComplexityFunc
	config.Complexity.Operation.StateChanges = paginatedQueryComplexityFunc

	// accounts are unpaginated resolver lists that fan out into per-account balance lookups. Price
	// them at a default page's worth (must stay >=~20 so a deep accounts->balances traversal stays
	// over the limit). The account-edge operations/stateChanges lists are deliberately left at the
	// default cost: the full-detail account-history query selects ~34 fields per edge, so any
	// multiplier there would push a first=100 page past the limit.
	accountsListComplexityFunc := func(childComplexity int) int {
		return childComplexity * int(graphqlutils.DefaultPageLimit)
	}
	config.Complexity.Transaction.Accounts = accountsListComplexityFunc
	config.Complexity.Operation.Accounts = accountsListComplexityFunc

	// Blend fields are unpaginated but bounded by on-chain limits rather than client
	// first/last args: Query.blendPools/blendEarnOptions fan out over a bounded catalog
	// (pools ≈ dozens; each pool ≤ MAX_RESERVES=30 reserves on-chain), the same
	// "unpaginated but bounded" shape as the accounts fan-out above, so they reuse
	// accountsListComplexityFunc's ×50 multiplier. Account.blendPositions is bounded
	// per-pool by that pool's max_positions (well under a full page), so it gets a
	// smaller ×10 multiplier.
	//
	// Worst case (every field selected, derived from the actual schema + the complexity
	// math above: default field complexity = 1 + sum(child complexities)):
	//   blendPools:       BlendPool = 11 scalars + reserves(1 + 17 BlendReserve scalars = 18)
	//                     => childComplexity 29 => 29*50 = 1450
	//   blendEarnOptions: BlendEarnOption = 4 scalars + pools(1 + 4 BlendEarnPoolOption
	//                     scalars = 5) => childComplexity 9 => 9*50 = 450
	//   blendPositions:   BlendAccountPositions = pools(1 + [7 scalars + reserves(1+16=17)]
	//                     = 25) + backstop(1 + [7 scalars + q4w(1+2=3)] = 11)
	//                     + backstopClaimedBlnd(1) => childComplexity 37 => 37*10 = 370
	// All comfortably under GRAPHQL_COMPLEXITY_LIMIT=6000. None of this touches
	// AccountTransactionEdge.operations/stateChanges or any other existing entry above —
	// those stay exactly as budgeted for the freighter full-detail query.
	config.Complexity.Query.BlendPools = accountsListComplexityFunc
	config.Complexity.Query.BlendEarnOptions = accountsListComplexityFunc
	// Query.blendPool (single pool by address) is left at the default (1 + childComplexity):
	// same treatment as the other single-item lookups (accountByAddress, operationByID,
	// transactionByHash) below, which also have no custom entry. It returns one object, not
	// a list, so no page-size-style multiplier is needed.
	config.Complexity.Account.BlendPositions = func(childComplexity int) int {
		return childComplexity * 10
	}
}
