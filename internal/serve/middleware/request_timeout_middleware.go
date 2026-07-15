package middleware

import (
	"context"
	"net/http"
	"time"
)

// RequestTimeoutMiddleware bounds each request's context to timeout, so that resolvers and DB
// calls further down the stack observe cancellation instead of running unbounded. Without this, a
// slow or stuck query can hold a pooled DB connection indefinitely (see also the WriteTimeout /
// IdleTimeout set on the HTTP server, which bound the connection itself rather than the request
// context).
func RequestTimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
