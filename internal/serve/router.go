package serve

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"

	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	resolvers "github.com/stellar/wallet-backend/internal/serve/graphql/resolvers"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/serve/middleware"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/ast"
)

// NewHandler creates the main HTTP handler with all routes configured
func NewHandler(deps HandlerDependencies) http.Handler {
	container := deps.ServiceContainer
	authProvider := deps.AuthProvider

	mux := supporthttp.NewAPIMux(log.DefaultLogger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)

	setupMiddleware(mux, container)
	setupPublicRoutes(mux, container)
	setupPotentiallyAuthenticatedRoutes(mux, container, authProvider)

	return mux
}

func setupMiddleware(mux *chi.Mux, container ServiceContainer) {
	mux.Use(middleware.MetricsMiddleware(container.GetMetricsService()))
	mux.Use(middleware.RecoverHandler(container.GetAppTracker()))
}

func setupPublicRoutes(mux *chi.Mux, container ServiceContainer) {
	mux.Get("/health", httphandler.HealthHandler{
		Models:     container.GetModels(),
		RPCService: container.GetRPCService(),
		AppTracker: container.GetAppTracker(),
	}.GetHealth)

	mux.Get("/api-metrics", promhttp.HandlerFor(
		container.GetMetricsService().GetRegistry(),
		promhttp.HandlerOpts{},
	).ServeHTTP)
}

func setupPotentiallyAuthenticatedRoutes(mux *chi.Mux, container ServiceContainer, authProvider AuthProvider) {
	mux.Group(func(r chi.Router) {
		if authProvider != nil {
			r.Use(middleware.AuthenticationMiddleware(
				container.GetServerHostname(),
				authProvider.GetRequestVerifier(),
				container.GetAppTracker(),
				container.GetMetricsService(),
			))
		}

		setupGraphQLRoutes(r, container)
		setupPaymentRoutes(r, container)
		setupTransactionRoutes(r, container)
	})
}

func setupGraphQLRoutes(r chi.Router, container ServiceContainer) {
	r.Route("/graphql", func(r chi.Router) {
		r.Use(middleware.DataloaderMiddleware(container.GetModels()))

		resolver := resolvers.NewResolver(
			container.GetModels(),
			container.GetAccountService(),
			container.GetTransactionService(),
		)

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
}

func setupPaymentRoutes(r chi.Router, container ServiceContainer) {
	r.Route("/payments", func(r chi.Router) {
		handler := &httphandler.PaymentHandler{
			PaymentService: container.GetPaymentService(),
			AppTracker:     container.GetAppTracker(),
		}

		r.Get("/", handler.GetPayments)
	})
}

func setupTransactionRoutes(r chi.Router, container ServiceContainer) {
	r.Route("/tx", func(r chi.Router) {
		accountHandler := &httphandler.AccountHandler{
			AccountService:            container.GetAccountService(),
			AccountSponsorshipService: container.GetAccountSponsorshipService(),
			SupportedAssets:           container.GetSupportedAssets(),
			AppTracker:                container.GetAppTracker(),
		}

		r.Post("/create-sponsored-account", accountHandler.SponsorAccountCreation)
		r.Post("/create-fee-bump", accountHandler.CreateFeeBumpTransaction)
	})
}
