package metrics

import "github.com/prometheus/client_golang/prometheus"

// DBMetrics holds Prometheus collectors for database operations.
type DBMetrics struct {
	// QueryDuration tracks the latency of individual database queries — the primary DB performance metric.
	// Use to detect slow queries and set SLOs on database response times.
	//
	//	histogram_quantile(0.99, rate(wallet_db_query_duration_seconds_bucket[5m]))
	//
	// Labels: query_type (e.g. "select", "insert", "upsert"), table.
	QueryDuration *prometheus.HistogramVec

	// QueriesTotal counts completed database queries.
	// Use for throughput dashboards and per-table query volume analysis.
	// Labels: query_type (e.g. "select", "insert", "upsert"), table.
	QueriesTotal *prometheus.CounterVec

	// QueryErrors counts database query failures classified by error type.
	// Use for error-rate alerting and diagnosing recurring failure patterns.
	// Labels: query_type (e.g. "select", "insert", "upsert"), table, error_type.
	QueryErrors *prometheus.CounterVec

	// BatchSize records the number of rows in batch database operations.
	// Detects unexpectedly large or small batches that may indicate upstream issues.
	//
	//	histogram_quantile(0.95, rate(wallet_db_batch_operation_size_bucket[5m]))
	//
	// Labels: operation, table.
	BatchSize *prometheus.HistogramVec
}

func newDBMetrics(reg prometheus.Registerer) *DBMetrics {
	m := &DBMetrics{
		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_db_query_duration_seconds",
			Help:    "Duration of database queries.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2.5, 10),
		}, []string{"query_type", "table"}),
		QueriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_db_queries_total",
			Help: "Total number of database queries.",
		}, []string{"query_type", "table"}),
		QueryErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_db_query_errors_total",
			Help: "Total number of database query errors.",
		}, []string{"query_type", "table", "error_type"}),
		BatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_db_batch_operation_size",
			Help:    "Size of batch database operations.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{"operation", "table"}),
	}
	reg.MustRegister(
		m.QueryDuration,
		m.QueriesTotal,
		m.QueryErrors,
		m.BatchSize,
	)
	return m
}
