package serve

import (
	"fmt"
	"net/http"

	supporthttp "github.com/stellar/go/support/http"
	supportlog "github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/health"
)

type Configs struct {
	Logger *supportlog.Entry
	Port   int
}

type handlerDeps struct {
	Logger *supportlog.Entry
}

func Serve(cfg Configs) {
	deps := getHandlerDeps(cfg)

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    handler(deps),
		OnStarting: func() {
			deps.Logger.Infof("Starting Wallet Backend server on %s", addr)
		},
	})
}

func getHandlerDeps(cfg Configs) handlerDeps {
	return handlerDeps{
		Logger: cfg.Logger,
	}
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(deps.Logger)
	mux.NotFound(errorHandler{Error: notFound}.ServeHTTP)
	mux.MethodNotAllowed(errorHandler{Error: methodNotAllowed}.ServeHTTP)

	mux.Get("/health", health.PassHandler{}.ServeHTTP)

	return mux
}
