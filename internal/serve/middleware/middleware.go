package middleware

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

const MaxBodySize int64 = 10_240 // 10kb

func AuthenticationMiddleware(
	serverHostname string,
	jwtTokenParser auth.JWTTokenParser,
	appTracker apptracker.AppTracker,
	metricsService metrics.MetricsService,
) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			authHeader := req.Header.Get("Authorization")
			if authHeader == "" {
				httperror.Unauthorized("", nil).Render(rw)
				return
			}

			// check if the Authorization header has two parts ['Bearer', token]
			if !strings.HasPrefix(authHeader, "Bearer ") {
				log.Ctx(ctx).Error("Authorization header is invalid, expected 'Bearer <token>'")
				httperror.Unauthorized("", nil).Render(rw)
				return
			}

			var reqBody []byte
			if req.Body != nil {
				var err error
				reqBody, err = io.ReadAll(io.LimitReader(req.Body, MaxBodySize))
				if err != nil {
					err = fmt.Errorf("reading request body: %w", err)
					httperror.InternalServerError(ctx, err.Error(), err, nil, appTracker).Render(rw)
					return
				}
			}

			tokenStr := strings.Replace(authHeader, "Bearer ", "", 1)
			if _, _, err := jwtTokenParser.ParseJWT(tokenStr, serverHostname, reqBody); err != nil {
				if expirationDuration, ok := auth.ParseExpirationDuration(err); ok {
					metricsService.IncSignatureVerificationExpired(expirationDuration.Seconds())
				}
				log.Ctx(ctx).Error(fmt.Errorf("parsing JWT token: %w", err))
				httperror.Unauthorized("", nil).Render(rw)
				return
			}

			req.Body = io.NopCloser(bytes.NewReader(reqBody))
			next.ServeHTTP(rw, req)
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
