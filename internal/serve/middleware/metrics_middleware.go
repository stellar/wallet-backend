package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi"

	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	// unmatchedEndpoint labels requests chi routed to no pattern (404s, and 405s for
	// unknown method tokens). The raw path is client-controlled and never becomes a label.
	unmatchedEndpoint = "unmatched"
	// otherMethod labels any method token outside knownMethods. net/http accepts any
	// RFC 7230 token as a method, so the raw value is unbounded.
	otherMethod = "OTHER"
)

var knownMethods = map[string]struct{}{
	http.MethodGet: {}, http.MethodHead: {}, http.MethodPost: {}, http.MethodPut: {},
	http.MethodPatch: {}, http.MethodDelete: {}, http.MethodOptions: {},
}

// MetricsMiddleware records request count and duration per (route pattern, method).
// It runs on the root mux above authentication, so both label values must be bounded
// by construction: each unique label pair is retained for the life of the process.
func MetricsMiddleware(httpMetrics *metrics.HTTPMetrics) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()
			rw := &responseWriter{ResponseWriter: w}
			next.ServeHTTP(rw, r)

			// The route pattern is only known after routing, i.e. after next.ServeHTTP.
			endpoint := unmatchedEndpoint
			if rctx := chi.RouteContext(r.Context()); rctx != nil {
				if pattern := rctx.RoutePattern(); pattern != "" {
					endpoint = pattern
				}
			}
			method := otherMethod
			if _, known := knownMethods[r.Method]; known {
				method = r.Method
			}

			duration := time.Since(startTime).Seconds()
			httpMetrics.RequestsDuration.WithLabelValues(endpoint, method).Observe(duration)
			httpMetrics.RequestsTotal.WithLabelValues(endpoint, method, strconv.Itoa(rw.statusCode)).Inc()
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

//nolint:wrapcheck // This is a thin wrapper around the ResponseWriter
func (rw *responseWriter) Write(b []byte) (int, error) {
	// If WriteHeader hasn't been called yet, we assume it's a 200
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}
