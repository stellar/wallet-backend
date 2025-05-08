package middleware

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

const MaxBodySize int64 = 10_240 // 10kb

func AuthenticationMiddleware(
	serverHostname string,
	requestAuthVerifier auth.HTTPRequestVerifier,
	appTracker apptracker.AppTracker,
	metricsService metrics.MetricsService,
) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

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

			if expirationDuration, ok := auth.ParseExpirationDuration(err); ok {
				metricsService.IncSignatureVerificationExpired(expirationDuration.Seconds())
			}
			httperror.Unauthorized("", nil).Render(rw)
		})
	}
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
