package middleware

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

const APIKeyHeader = "X-API-Key"

func AuthenticationMiddleware(
	serverHostname string,
	requestAuthVerifier auth.HTTPRequestVerifier,
	appTracker apptracker.AppTracker,
	metricsService metrics.MetricsService,
	wantedAPIKey string,
) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			if apiKeyAuth(req, wantedAPIKey) {
				log.Ctx(ctx).Debug("🔓 Authenticated with API key")
				next.ServeHTTP(rw, req)
				return
			}

			err := requestAuthVerifier.VerifyHTTPRequest(req, serverHostname)
			if err == nil {
				next.ServeHTTP(rw, req)
				return
			}

			log.Ctx(ctx).Errorf("verifying request authentication: %v", err)

			if !errors.Is(err, auth.ErrUnauthorized) {
				httperror.InternalServerError(ctx, "", err, nil, appTracker).Render(rw)
				return
			}

			var expiredTokenErr *auth.ExpiredTokenError
			if errors.As(err, &expiredTokenErr) {
				metricsService.IncSignatureVerificationExpired(expiredTokenErr.ExpiredBy.Seconds())
			}

			httperror.Unauthorized("", nil).Render(rw)
		})
	}
}

// BearerTokenAuthMiddleware is a middleware that extracts and validates Bearer tokens.
// It expects the Authorization header in the format: "Bearer <token>"
// The token is stored in the request context under the key "token"
func apiKeyAuth(req *http.Request, wantedAPIKey string) bool {
	wantedAPIKey = strings.TrimSpace(wantedAPIKey)
	if wantedAPIKey == "" {
		return false
	}

	gotAPIKey := req.Header.Get(APIKeyHeader)
	return gotAPIKey == wantedAPIKey
}

// RecoverHandler is a middleware that recovers from panics and logs the error.
func RecoverHandler(appTracker apptracker.AppTracker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			defer func() {
				r := recover()
				if r == nil {
					return
				}
				err, ok := r.(error)
				if !ok {
					err = fmt.Errorf("panic: %v", r)
				}

				// No need to recover when the client has disconnected:
				if errors.Is(err, http.ErrAbortHandler) {
					panic(err)
				}

				ctx := req.Context()
				log.Ctx(ctx).WithStack(err).Error(err)
				httperror.InternalServerError(ctx, "", err, nil, appTracker).Render(rw)
			}()

			next.ServeHTTP(rw, req)
		})
	}
}
