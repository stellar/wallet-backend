package metrics

import "github.com/prometheus/client_golang/prometheus"

// DBMetrics holds Prometheus collectors for database operations.
type DBMetrics struct {
	QueryDuration       *prometheus.SummaryVec
	QueriesTotal        *prometheus.CounterVec
	QueryErrors         *prometheus.CounterVec
	TransactionsTotal   *prometheus.CounterVec
	TransactionDuration *prometheus.SummaryVec
	BatchSize           *prometheus.HistogramVec
}

func newDBMetrics(reg prometheus.Registerer) *DBMetrics {
	m := &DBMetrics{
		QueryDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_db_query_duration_seconds",
			Help:       "Duration of database queries.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"query_type", "table"}),
		QueriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_db_queries_total",
			Help: "Total number of database queries.",
		}, []string{"query_type", "table"}),
		QueryErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_db_query_errors_total",
			Help: "Total number of database query errors.",
		}, []string{"query_type", "table", "error_type"}),
		TransactionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_db_transactions_total",
			Help: "Total number of database transactions.",
		}, []string{"status"}),
		TransactionDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_db_transaction_duration_seconds",
			Help:       "Duration of database transactions.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"status"}),
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
		m.TransactionsTotal,
		m.TransactionDuration,
		m.BatchSize,
	)
	return m
}
