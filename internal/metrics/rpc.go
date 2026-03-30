package metrics

import "github.com/prometheus/client_golang/prometheus"

// RPCMetrics holds Prometheus collectors for RPC service operations.
type RPCMetrics struct {
	// Transport-level metrics
	RequestsTotal     *prometheus.CounterVec
	RequestsDuration  *prometheus.SummaryVec
	EndpointFailures  *prometheus.CounterVec
	EndpointSuccesses *prometheus.CounterVec
	ServiceHealth     prometheus.Gauge
	LatestLedger      prometheus.Gauge

	// Application-level method metrics
	MethodCallsTotal  *prometheus.CounterVec
	MethodDuration    *prometheus.SummaryVec
	MethodErrorsTotal *prometheus.CounterVec
}

func newRPCMetrics(reg prometheus.Registerer) *RPCMetrics {
	m := &RPCMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_requests_total",
			Help: "Total number of RPC requests.",
		}, []string{"endpoint"}),
		RequestsDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_rpc_requests_duration_seconds",
			Help:       "Duration of RPC requests in seconds.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"endpoint"}),
		EndpointFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_endpoint_failures_total",
			Help: "Total number of RPC endpoint failures.",
		}, []string{"endpoint"}),
		EndpointSuccesses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_endpoint_successes_total",
			Help: "Total number of successful RPC requests.",
		}, []string{"endpoint"}),
		ServiceHealth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_rpc_service_health",
			Help: "RPC service health status (1 for healthy, 0 for unhealthy).",
		}),
		LatestLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_rpc_latest_ledger",
			Help: "Latest ledger number reported by the RPC service.",
		}),
		MethodCallsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_method_calls_total",
			Help: "Total number of RPC method calls at the application level.",
		}, []string{"method"}),
		MethodDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_rpc_method_duration_seconds",
			Help:       "Duration of RPC method execution including parsing and validation.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"method"}),
		MethodErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_method_errors_total",
			Help: "Total number of RPC method errors by error type.",
		}, []string{"method", "error_type"}),
	}
	reg.MustRegister(
		m.RequestsTotal,
		m.RequestsDuration,
		m.EndpointFailures,
		m.EndpointSuccesses,
		m.ServiceHealth,
		m.LatestLedger,
		m.MethodCallsTotal,
		m.MethodDuration,
		m.MethodErrorsTotal,
	)
	return m
}
