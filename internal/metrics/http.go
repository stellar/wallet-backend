package metrics

import "github.com/prometheus/client_golang/prometheus"

// HTTPMetrics holds Prometheus collectors for HTTP request tracking.
type HTTPMetrics struct {
	RequestsTotal    *prometheus.CounterVec
	RequestsDuration *prometheus.HistogramVec
}

func newHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	m := &HTTPMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_http_requests_total",
			Help: "Total number of HTTP requests.",
		}, []string{"endpoint", "method", "status_code"}),
		// A histogram costs ~200 B per label set against ~68 KB for a summary with
		// quantile objectives, and its buckets aggregate across replicas. The top bucket
		// matches the 30s request context timeout in serve, so a slow request that still
		// completes lands in a finite bucket rather than only in +Inf.
		RequestsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0},
		}, []string{"endpoint", "method"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestsDuration)
	return m
}
