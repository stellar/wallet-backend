package metrics

import "github.com/prometheus/client_golang/prometheus"

// HTTPMetrics holds Prometheus collectors for HTTP request tracking.
type HTTPMetrics struct {
	RequestsTotal    *prometheus.CounterVec
	RequestsDuration *prometheus.SummaryVec
}

func newHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	m := &HTTPMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_http_requests_total",
			Help: "Total number of HTTP requests.",
		}, []string{"endpoint", "method", "status_code"}),
		RequestsDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_http_request_duration_seconds",
			Help:       "Duration of HTTP requests in seconds.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"endpoint", "method"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestsDuration)
	return m
}
