package middleware

import (
	"context"
	"net/http"

	"github.com/alitto/pond/v2"
)

type contextKey string

const requestPoolKey contextKey = "requestPool"

// RequestPoolFromContext returns the per-request worker pool from the context.
// Returns nil if no pool was set (e.g., in tests without the middleware).
func RequestPoolFromContext(ctx context.Context) pond.Pool {
	pool, _ := ctx.Value(requestPoolKey).(pond.Pool)
	return pool
}

// RequestPoolMiddleware creates a bounded worker pool for each HTTP request and
// injects it into the request context. All GraphQL field resolutions within the
// same request share this pool, which caps the total number of concurrent worker
// goroutines a single request can consume. The pool is stopped and drained when
// the HTTP handler returns.
func RequestPoolMiddleware(maxConcurrency int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool := pond.NewPool(maxConcurrency)
			ctx := context.WithValue(r.Context(), requestPoolKey, pool)
			next.ServeHTTP(w, r.WithContext(ctx))
			pool.StopAndWait()
		})
	}
}
