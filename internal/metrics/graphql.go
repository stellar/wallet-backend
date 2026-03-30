package metrics

import "github.com/prometheus/client_golang/prometheus"

// GraphQLMetrics holds Prometheus collectors for GraphQL operations.
type GraphQLMetrics struct {
	// OperationDuration tracks end-to-end latency of GraphQL operations — the primary SLO metric.
	// Covers parsing, validation, and all resolver execution.
	//
	//	histogram_quantile(0.99, rate(wallet_graphql_operation_duration_seconds_bucket[5m]))
	//
	// Labels: operation_name, operation_type ("query"/"mutation").
	OperationDuration *prometheus.HistogramVec

	// OperationsTotal counts completed GraphQL operations by outcome.
	// Use for throughput dashboards and error-rate alerting.
	// Labels: operation_name, operation_type ("query"/"mutation"), status ("success"/"error").
	OperationsTotal *prometheus.CounterVec

	// InFlightOperations tracks the number of GraphQL operations currently being processed.
	// A sustained high value signals server saturation; use for load-shedding alerts.
	InFlightOperations prometheus.Gauge

	// ResponseSize tracks GraphQL response body sizes in bytes.
	// Detects payload bloat from unbounded pagination or overly broad field selections.
	//
	//	histogram_quantile(0.95, rate(wallet_graphql_response_size_bytes_bucket[5m]))
	//
	// Labels: operation_name, operation_type ("query"/"mutation").
	ResponseSize *prometheus.HistogramVec

	// Complexity records the computed complexity score of each GraphQL operation.
	// Queries approaching the configured limit may indicate abusive or inefficient clients.
	// Labels: operation_name.
	Complexity *prometheus.HistogramVec

	// ErrorsTotal counts GraphQL errors classified by type at the operation level.
	// Types form a closed set: validation_error, parse_error, bad_input, auth_error,
	// forbidden, internal_error, unknown. Unrecognized extension codes map to "unknown".
	// Labels: operation_name, error_type.
	ErrorsTotal *prometheus.CounterVec

	// DeprecatedFieldsTotal counts accesses to fields marked @deprecated in the schema.
	// Produces zero series until deprecated fields exist — no cardinality overhead until then.
	// Labels: operation_name, field_name.
	DeprecatedFieldsTotal *prometheus.CounterVec
}

// NewGraphQLMetrics creates and registers all GraphQL Prometheus collectors.
func NewGraphQLMetrics(reg prometheus.Registerer) *GraphQLMetrics {
	m := &GraphQLMetrics{
		OperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_graphql_operation_duration_seconds",
			Help:    "Total duration of GraphQL operations from request start to response.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}, []string{"operation_name", "operation_type"}),
		OperationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_graphql_operations_total",
			Help: "Total number of GraphQL operations processed.",
		}, []string{"operation_name", "operation_type", "status"}),
		InFlightOperations: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_graphql_in_flight_operations",
			Help: "Number of GraphQL operations currently being processed.",
		}),
		ResponseSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_graphql_response_size_bytes",
			Help:    "Size of GraphQL response bodies in bytes.",
			Buckets: prometheus.ExponentialBuckets(256, 4, 8),
		}, []string{"operation_name", "operation_type"}),
		Complexity: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_graphql_complexity",
			Help:    "GraphQL query complexity values.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}, []string{"operation_name"}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_graphql_errors_total",
			Help: "Total number of GraphQL errors.",
		}, []string{"operation_name", "error_type"}),
		DeprecatedFieldsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_graphql_deprecated_fields_total",
			Help: "Total number of deprecated GraphQL field accesses.",
		}, []string{"operation_name", "field_name"}),
	}
	reg.MustRegister(
		m.OperationDuration, m.OperationsTotal, m.InFlightOperations,
		m.ResponseSize, m.Complexity, m.ErrorsTotal, m.DeprecatedFieldsTotal,
	)
	return m
}
