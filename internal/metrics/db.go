package metrics

import "github.com/prometheus/client_golang/prometheus"

// DBMetrics holds Prometheus collectors for database operations.
type DBMetrics struct {
	QueryDuration *prometheus.HistogramVec
	QueriesTotal  *prometheus.CounterVec
	QueryErrors   *prometheus.CounterVec
	BatchSize     *prometheus.HistogramVec
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
