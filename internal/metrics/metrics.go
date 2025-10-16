package metrics

import (
	"fmt"
	"strconv"

	"github.com/alitto/pond"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsService interface {
	RegisterPoolMetrics(channel string, pool *pond.WorkerPool)
	GetRegistry() *prometheus.Registry
	SetLatestLedgerIngested(value float64)
	ObserveIngestionDuration(ingestionType string, duration float64)
	IncActiveAccount()
	DecActiveAccount()
	IncRPCRequests(endpoint string)
	ObserveRPCRequestDuration(endpoint string, duration float64)
	IncRPCEndpointFailure(endpoint string)
	IncRPCEndpointSuccess(endpoint string)
	SetRPCServiceHealth(healthy bool)
	SetRPCLatestLedger(ledger int64)
	IncNumRequests(endpoint, method string, statusCode int)
	ObserveRequestDuration(endpoint, method string, duration float64)
	ObserveDBQueryDuration(queryType, table string, duration float64)
	IncDBQuery(queryType, table string)
	IncDBQueryError(queryType, table, errorType string)
	IncDBTransaction(status string)
	ObserveDBTransactionDuration(status string, duration float64)
	ObserveDBBatchSize(operation, table string, size int)
	IncSignatureVerificationExpired(expiredSeconds float64)
}

// MetricsService handles all metrics for the wallet-backend
type metricsService struct {
	registry *prometheus.Registry
	db       *sqlx.DB

	// Ingest Service Metrics
	latestLedgerIngested prometheus.Gauge
	ingestionDuration    *prometheus.SummaryVec

	// Account Metrics
	activeAccounts prometheus.Gauge

	// RPC Service Metrics
	rpcRequestsTotal     *prometheus.CounterVec
	rpcRequestsDuration  *prometheus.SummaryVec
	rpcEndpointFailures  *prometheus.CounterVec
	rpcEndpointSuccesses *prometheus.CounterVec
	rpcServiceHealth     prometheus.Gauge
	rpcLatestLedger      prometheus.Gauge

	// HTTP Request Metrics
	numRequestsTotal *prometheus.CounterVec
	requestsDuration *prometheus.SummaryVec

	// DB Query Metrics
	dbQueryDuration *prometheus.SummaryVec
	dbQueriesTotal  *prometheus.CounterVec
	dbQueryErrors   *prometheus.CounterVec
	dbTransactions  *prometheus.CounterVec
	dbTxnDuration   *prometheus.SummaryVec
	dbBatchSize     *prometheus.HistogramVec

	// Signature Verification Metrics
	signatureVerificationExpired *prometheus.CounterVec
}

// NewMetricsService creates a new metrics service with all metrics registered
func NewMetricsService(db *sqlx.DB) MetricsService {
	m := &metricsService{
		registry: prometheus.NewRegistry(),
		db:       db,
	}

	// Ingest Service Metrics
	m.latestLedgerIngested = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "latest_ledger_ingested",
			Help: "Latest ledger ingested",
		},
	)
	m.ingestionDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "ingestion_duration_seconds",
			Help:       "Duration of ledger ingestion",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"type"},
	)

	// Account Metrics
	m.activeAccounts = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_accounts",
			Help: "Number of currently registered active accounts",
		},
	)

	// RPC Service Metrics
	m.rpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_requests_total",
			Help: "Total number of RPC requests",
		},
		[]string{"endpoint"},
	)
	m.rpcRequestsDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "rpc_requests_duration_seconds",
			Help:       "Duration of RPC requests in seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"endpoint"},
	)
	m.rpcEndpointFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_endpoint_failures_total",
			Help: "Total number of RPC endpoint failures",
		},
		[]string{"endpoint"},
	)
	m.rpcEndpointSuccesses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_endpoint_successes_total",
			Help: "Total number of successful RPC requests",
		},
		[]string{"endpoint"},
	)
	m.rpcServiceHealth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "rpc_service_health",
			Help: "RPC service health status (1 for healthy, 0 for unhealthy)",
		},
	)
	m.rpcLatestLedger = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "rpc_latest_ledger",
			Help: "Latest ledger number reported by the RPC service",
		},
	)

	// HTTP Request Metrics
	m.numRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"endpoint", "method", "status_code"},
	)
	m.requestsDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "http_request_duration_seconds",
			Help:       "Duration of HTTP requests in seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"endpoint", "method"},
	)

	// DB Query Metrics
	m.dbQueryDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "db_query_duration_seconds",
			Help:       "Duration of database queries",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"query_type", "table"},
	)
	m.dbQueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_queries_total",
			Help: "Total number of database queries",
		},
		[]string{"query_type", "table"},
	)
	m.dbQueryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_query_errors_total",
			Help: "Total number of database query errors",
		},
		[]string{"query_type", "table", "error_type"},
	)
	m.dbTransactions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_transactions_total",
			Help: "Total number of database transactions",
		},
		[]string{"status"},
	)
	m.dbTxnDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "db_transaction_duration_seconds",
			Help:       "Duration of database transactions",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"status"},
	)
	m.dbBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_batch_operation_size",
			Help:    "Size of batch database operations",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048
		},
		[]string{"operation", "table"},
	)

	// Signature Verification Metrics
	m.signatureVerificationExpired = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "signature_verification_expired_total",
			Help: "Total number of signature verifications that failed due to expiration",
		},
		[]string{"expired_seconds"},
	)

	m.registerMetrics()
	return m
}

func (m *metricsService) registerMetrics() {
	collector := sqlstats.NewStatsCollector("wallet-backend-db", m.db)
	m.registry.MustRegister(
		collector,
		m.latestLedgerIngested,
		m.ingestionDuration,
		m.activeAccounts,
		m.rpcRequestsTotal,
		m.rpcRequestsDuration,
		m.rpcEndpointFailures,
		m.rpcEndpointSuccesses,
		m.rpcServiceHealth,
		m.rpcLatestLedger,
		m.numRequestsTotal,
		m.requestsDuration,
		m.dbQueryDuration,
		m.dbQueriesTotal,
		m.dbQueryErrors,
		m.dbTransactions,
		m.dbTxnDuration,
		m.dbBatchSize,
		m.signatureVerificationExpired,
	)
}

// RegisterPool registers a worker pool for metrics collection
func (m *metricsService) RegisterPoolMetrics(channel string, pool *pond.WorkerPool) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        fmt.Sprintf("pool_workers_running_%s", channel),
			Help:        "Number of running worker goroutines",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.RunningWorkers())
		},
	))

	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        fmt.Sprintf("pool_workers_idle_%s", channel),
			Help:        "Number of idle worker goroutines",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.IdleWorkers())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        fmt.Sprintf("pool_tasks_submitted_total_%s", channel),
			Help:        "Number of tasks submitted",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.SubmittedTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        fmt.Sprintf("pool_tasks_waiting_%s", channel),
			Help:        "Number of tasks currently waiting in the queue",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.WaitingTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        fmt.Sprintf("pool_tasks_successful_total_%s", channel),
			Help:        "Number of tasks that completed successfully",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.SuccessfulTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        fmt.Sprintf("pool_tasks_failed_total_%s", channel),
			Help:        "Number of tasks that completed with panic",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.FailedTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        fmt.Sprintf("pool_tasks_completed_total_%s", channel),
			Help:        "Number of tasks that completed either successfully or with panic",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.CompletedTasks())
		},
	))
}

// GetRegistry returns the prometheus registry
func (m *metricsService) GetRegistry() *prometheus.Registry {
	return m.registry
}

// Ingest Service Metrics

func (m *metricsService) SetLatestLedgerIngested(value float64) {
	m.latestLedgerIngested.Set(value)
}

func (m *metricsService) ObserveIngestionDuration(ingestionType string, duration float64) {
	m.ingestionDuration.WithLabelValues(ingestionType).Observe(duration)
}

// Account Service Metrics
func (m *metricsService) IncActiveAccount() {
	m.activeAccounts.Inc()
}

func (m *metricsService) DecActiveAccount() {
	m.activeAccounts.Dec()
}

// RPC Service Metrics
func (m *metricsService) IncRPCRequests(endpoint string) {
	m.rpcRequestsTotal.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) ObserveRPCRequestDuration(endpoint string, duration float64) {
	m.rpcRequestsDuration.WithLabelValues(endpoint).Observe(duration)
}

func (m *metricsService) IncRPCEndpointFailure(endpoint string) {
	m.rpcEndpointFailures.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) IncRPCEndpointSuccess(endpoint string) {
	m.rpcEndpointSuccesses.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) SetRPCServiceHealth(healthy bool) {
	if healthy {
		m.rpcServiceHealth.Set(1)
	} else {
		m.rpcServiceHealth.Set(0)
	}
}

func (m *metricsService) SetRPCLatestLedger(ledger int64) {
	m.rpcLatestLedger.Set(float64(ledger))
}

// HTTP Request Metrics
func (m *metricsService) IncNumRequests(endpoint, method string, statusCode int) {
	m.numRequestsTotal.WithLabelValues(endpoint, method, strconv.Itoa(statusCode)).Inc()
}

func (m *metricsService) ObserveRequestDuration(endpoint, method string, duration float64) {
	m.requestsDuration.WithLabelValues(endpoint, method).Observe(duration)
}

// DB Query Metrics
func (m *metricsService) ObserveDBQueryDuration(queryType, table string, duration float64) {
	m.dbQueryDuration.WithLabelValues(queryType, table).Observe(duration)
}

func (m *metricsService) IncDBQuery(queryType, table string) {
	m.dbQueriesTotal.WithLabelValues(queryType, table).Inc()
}

func (m *metricsService) IncDBQueryError(queryType, table, errorType string) {
	m.dbQueryErrors.WithLabelValues(queryType, table, errorType).Inc()
}

func (m *metricsService) IncDBTransaction(status string) {
	m.dbTransactions.WithLabelValues(status).Inc()
}

func (m *metricsService) ObserveDBTransactionDuration(status string, duration float64) {
	m.dbTxnDuration.WithLabelValues(status).Observe(duration)
}

func (m *metricsService) ObserveDBBatchSize(operation, table string, size int) {
	m.dbBatchSize.WithLabelValues(operation, table).Observe(float64(size))
}

// Signature Verification Metrics
func (m *metricsService) IncSignatureVerificationExpired(expiredSeconds float64) {
	m.signatureVerificationExpired.WithLabelValues(fmt.Sprintf("%fs", expiredSeconds)).Inc()
}
