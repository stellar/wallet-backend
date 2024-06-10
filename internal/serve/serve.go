package serve

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/health"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/serve/auth"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
)

type Configs struct {
	Port             int
	DatabaseURL      string
	ServerBaseURL    string
	WalletSigningKey string
	LogLevel         logrus.Level

	// Horizon
	SupportedAssets       []entities.Asset
	MaxSponsoredThreshold int
	BaseFee               int
	HorizonClient         horizonclient.ClientInterface
	SignatureClient       signing.SignatureClient
}

type handlerDeps struct {
	Models            *data.Models
	Port              int
	DatabaseURL       string
	SignatureVerifier auth.SignatureVerifier
	SupportedAssets   []entities.Asset

	// Services
	AccountSponsorshipService services.AccountSponsorshipService
}

func Serve(cfg Configs) error {
	deps, err := getHandlerDeps(cfg)
	if err != nil {
		return fmt.Errorf("setting up handler dependencies: %w", err)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    handler(deps),
		OnStarting: func() {
			log.Infof("Starting Wallet Backend server on port %d", cfg.Port)
		},
		OnStopping: func() {
			log.Info("Stopping Wallet Backend server")
		},
	})

	return nil
}

func getHandlerDeps(cfg Configs) (handlerDeps, error) {
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("connecting to the database: %w", err)
	}
	models, err := data.NewModels(dbConnectionPool)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("creating models for Serve: %w", err)
	}

	signatureVerifier, err := auth.NewStellarSignatureVerifier(cfg.ServerBaseURL, cfg.WalletSigningKey)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating stellar signature verifier: %w", err)
	}

	accountSponsorshipService, err := services.NewAccountSponsorshipService(cfg.SignatureClient, cfg.HorizonClient, cfg.MaxSponsoredThreshold, int64(cfg.BaseFee))
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating account sponsorship service: %w", err)
	}

	return handlerDeps{
		Models:                    models,
		SignatureVerifier:         signatureVerifier,
		SupportedAssets:           cfg.SupportedAssets,
		AccountSponsorshipService: accountSponsorshipService,
	}, nil
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(log.DefaultLogger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)

	mux.Get("/health", health.PassHandler{}.ServeHTTP)

	// Authenticated routes
	mux.Group(func(r chi.Router) {
		r.Use(middleware.SignatureMiddleware(deps.SignatureVerifier))

		r.Route("/payments", func(r chi.Router) {
			handler := &httphandler.PaymentsHandler{
				PaymentModel: deps.Models.Payments,
			}

			r.Post("/subscribe", handler.SubscribeAddress)
			r.Post("/unsubscribe", handler.UnsubscribeAddress)
		})

		r.Route("/tx", func(r chi.Router) {
			handler := &httphandler.AccountHandler{
				AccountSponsorshipService: deps.AccountSponsorshipService,
				SupportedAssets:           deps.SupportedAssets,
			}

			r.Post("/create-sponsored-account", handler.SponsorAccountCreation)
		})
	})

	return mux
}
