package metrics

import "github.com/prometheus/client_golang/prometheus"

// rpcDurationBuckets covers the expected latency range for Stellar RPC calls:
// fast (getHealth ~10ms), normal (getTransaction ~100-500ms), slow (getTransactions ~1-3s), degraded (~10s).
var rpcDurationBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

// RPCMetrics holds Prometheus collectors for RPC service operations.
// Metrics are split into two layers:
//   - Transport-level: raw JSON-RPC HTTP request instrumentation
//   - Application-level: Go method instrumentation including parsing and validation
type RPCMetrics struct {
	// ── Transport-level metrics (JSON-RPC HTTP layer) ──────────────────

	// RequestDuration tracks round-trip latency of JSON-RPC HTTP calls — the primary RPC SLO metric.
	// Includes network time, serialization, and response reading.
	//
	//	histogram_quantile(0.99, rate(wallet_rpc_request_duration_seconds_bucket[5m]))
	//	histogram_quantile(0.99, rate(wallet_rpc_request_duration_seconds_bucket{method="sendTransaction"}[5m]))
	//
	// Labels: method (JSON-RPC method name, e.g. "getTransaction", "sendTransaction").
	RequestDuration *prometheus.HistogramVec

	// RequestsTotal counts completed JSON-RPC HTTP requests by method and outcome.
	// Use for throughput dashboards and transport-layer error-rate alerting.
	//
	//	rate(wallet_rpc_requests_total[5m])
	//	rate(wallet_rpc_requests_total{status="failure"}[5m]) / rate(wallet_rpc_requests_total[5m])
	//
	// Labels: method (JSON-RPC method name), status ("success" / "failure").
	RequestsTotal *prometheus.CounterVec

	// InFlightRequests tracks the number of RPC HTTP requests currently in progress.
	// A sustained high value signals connection pool exhaustion or RPC node slowness.
	//
	//	wallet_rpc_in_flight_requests > 10
	InFlightRequests prometheus.Gauge

	// ResponseSizeBytes tracks RPC response body sizes.
	// Detects unexpectedly large responses that may indicate pagination issues or RPC misbehavior.
	//
	//	histogram_quantile(0.95, rate(wallet_rpc_response_size_bytes_bucket{method="getTransactions"}[5m]))
	//
	// Labels: method (JSON-RPC method name).
	ResponseSizeBytes *prometheus.HistogramVec

	// ServiceHealth reports the last-known RPC service health status (1 = healthy, 0 = unhealthy).
	// Updated on every GetHealth call.
	//
	//	wallet_rpc_service_health == 0
	ServiceHealth prometheus.Gauge

	// LatestLedger reports the latest ledger sequence number from the RPC service.
	// Updated on every successful GetHealth call. Use to detect RPC node stalls.
	//
	//	changes(wallet_rpc_latest_ledger[5m]) == 0
	LatestLedger prometheus.Gauge

	// ── Application-level method metrics ───────────────────────────────

	// MethodCallsTotal counts application-level RPC method invocations.
	// Use with MethodErrorsTotal to compute per-method error rates.
	//
	//	rate(wallet_rpc_method_calls_total[5m])
	//
	// Labels: method (Go method name, e.g. "GetTransaction", "SendTransaction").
	MethodCallsTotal *prometheus.CounterVec

	// MethodDuration tracks end-to-end latency of application-level RPC methods.
	// Includes transport time, JSON parsing, validation, and any post-processing.
	//
	//	histogram_quantile(0.99, rate(wallet_rpc_method_duration_seconds_bucket[5m]))
	//
	// Labels: method (Go method name).
	MethodDuration *prometheus.HistogramVec

	// MethodErrorsTotal counts application-level RPC method errors classified by type.
	// Types: rpc_error, json_unmarshal_error, validation_error, not_found_error, xdr_decode_error.
	//
	//	rate(wallet_rpc_method_errors_total{error_type="rpc_error"}[5m])
	//
	// Labels: method (Go method name), error_type.
	MethodErrorsTotal *prometheus.CounterVec
}

func newRPCMetrics(reg prometheus.Registerer) *RPCMetrics {
	m := &RPCMetrics{
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_rpc_request_duration_seconds",
			Help:    "Duration of RPC HTTP requests in seconds.",
			Buckets: rpcDurationBuckets,
		}, []string{"method"}),
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_requests_total",
			Help: "Total number of RPC HTTP requests by method and outcome.",
		}, []string{"method", "status"}),
		InFlightRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_rpc_in_flight_requests",
			Help: "Number of RPC HTTP requests currently in flight.",
		}),
		ResponseSizeBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_rpc_response_size_bytes",
			Help:    "Size of RPC response bodies in bytes.",
			Buckets: prometheus.ExponentialBuckets(256, 4, 8),
		}, []string{"method"}),
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
		MethodDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_rpc_method_duration_seconds",
			Help:    "Duration of RPC method execution including parsing and validation.",
			Buckets: rpcDurationBuckets,
		}, []string{"method"}),
		MethodErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_rpc_method_errors_total",
			Help: "Total number of RPC method errors by error type.",
		}, []string{"method", "error_type"}),
	}
	reg.MustRegister(
		m.RequestDuration,
		m.RequestsTotal,
		m.InFlightRequests,
		m.ResponseSizeBytes,
		m.ServiceHealth,
		m.LatestLedger,
		m.MethodCallsTotal,
		m.MethodDuration,
		m.MethodErrorsTotal,
	)
	return m
}
