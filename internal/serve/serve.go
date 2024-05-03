package serve

import (
	"fmt"
	"net/http"

	"wallet-backend/internal/data"
	"wallet-backend/internal/db"

	"github.com/pkg/errors"
	supporthttp "github.com/stellar/go/support/http"
	supportlog "github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/health"
)

type Configs struct {
	Logger      *supportlog.Entry
	Port        int
	DatabaseURL string
}

type handlerDeps struct {
	Logger *supportlog.Entry
	Models *data.Models
}

func Serve(cfg Configs) error {
	deps, err := getHandlerDeps(cfg)
	if err != nil {
		return errors.Wrap(err, "setting up handler depedencies")
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    handler(deps),
		OnStarting: func() {
			deps.Logger.Infof("Starting Wallet Backend server on %s", addr)
		},
		OnStopping: func() {
			deps.Logger.Info("Stopping Wallet Backend server")
		},
	})

	return nil
}

func getHandlerDeps(cfg Configs) (handlerDeps, error) {
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return handlerDeps{}, errors.Wrap(err, "error connecting to the database")
	}
	models, err := data.NewModels(dbConnectionPool)
	if err != nil {
		return handlerDeps{}, errors.Wrap(err, "error creating models for Serve")
	}

	return handlerDeps{
		Logger: cfg.Logger,
		Models: models,
	}, nil
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(deps.Logger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)

	mux.Get("/health", health.PassHandler{}.ServeHTTP)

	// Authenticated routes
	mux.Group(func(r chi.Router) {
		// r.Use(...authMiddleware...)

		r.Route("/payments", func(r chi.Router) {
			handler := &httphandler.PaymentsHandler{}

			r.Post("/subscribe", handler.SubscribeAddress)
		})
	})

	return mux
}
