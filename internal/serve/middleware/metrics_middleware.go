package middleware

import (
	"net/http"
	"time"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// MetricsMiddleware creates a middleware that tracks HTTP request metrics
func MetricsMiddleware(metricsService metrics.MetricsService) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()

			// Get the endpoint from the request path
			endpoint := r.URL.Path
			if endpoint == "" {
				endpoint = "/"
			}

			// Create a response wrapper to capture the status code
			rw := &responseWriter{ResponseWriter: w}

			// Call the next handler
			next.ServeHTTP(rw, r)

			duration := time.Since(startTime).Seconds()
			metricsService.ObserveRequestDuration(endpoint, r.Method, duration)
			metricsService.IncNumRequests(endpoint, r.Method, rw.statusCode)
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

func (rw *responseWriter) Write(b []byte) (int, error) {
	// If WriteHeader hasn't been called yet, we assume it's a 200
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}
