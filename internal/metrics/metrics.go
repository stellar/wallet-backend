package metrics

import (
	"fmt"
	"strconv"

	"github.com/alitto/pond/v2"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsService interface {
	RegisterPoolMetrics(channel string, pool pond.Pool)
	GetRegistry() *prometheus.Registry
	SetLatestLedgerIngested(value float64)
	SetOldestLedgerIngested(value float64)
	ObserveIngestionDuration(duration float64)
	IncActiveAccount()
	DecActiveAccount()
	IncRPCRequests(endpoint string)
	ObserveRPCRequestDuration(endpoint string, duration float64)
	IncRPCEndpointFailure(endpoint string)
	IncRPCEndpointSuccess(endpoint string)
	SetRPCServiceHealth(healthy bool)
	SetRPCLatestLedger(ledger int64)
	// New RPC method-level metrics
	IncRPCMethodCalls(method string)
	ObserveRPCMethodDuration(method string, duration float64)
	IncRPCMethodErrors(method, errorType string)
	IncNumRequests(endpoint, method string, statusCode int)
	ObserveRequestDuration(endpoint, method string, duration float64)
	ObserveDBQueryDuration(queryType, table string, duration float64)
	IncDBQuery(queryType, table string)
	IncDBQueryError(queryType, table, errorType string)
	IncDBTransaction(status string)
	ObserveDBTransactionDuration(status string, duration float64)
	ObserveDBBatchSize(operation, table string, size int)
	IncSignatureVerificationExpired(expiredSeconds float64)
	// State Change Metrics
	ObserveStateChangeProcessingDuration(processor string, duration float64)
	IncStateChanges(stateChangeType, category string, count int)
	// Ingestion Phase Metrics
	ObserveIngestionPhaseDuration(phase string, duration float64)
	IncIngestionLedgersProcessed(count int)
	IncIngestionTransactionsProcessed(count int)
	IncIngestionOperationsProcessed(count int)
	ObserveIngestionBatchSize(size int)
	ObserveIngestionParticipantsCount(count int)
	// GraphQL Metrics
	ObserveGraphQLFieldDuration(operationName, fieldName string, duration float64)
	IncGraphQLField(operationName, fieldName string, success bool)
	ObserveGraphQLComplexity(operationName string, complexity int)
	IncGraphQLError(operationName, errorType string)
	// Backfill Metrics - all methods require instance ID
	SetBackfillStartLedger(instance string, ledger uint32)
	SetBackfillEndLedger(instance string, ledger uint32)
	SetBackfillCurrentLedger(instance string, ledger uint32)
	SetBackfillBatchesTotal(instance string, count int)
	IncBackfillBatchesCompleted(instance string)
	IncBackfillBatchesFailed(instance string)
	ObserveBackfillPhaseDuration(instance string, phase string, duration float64)
	IncBackfillLedgersProcessed(instance string, count int)
	IncBackfillTransactionsProcessed(instance string, count int)
	IncBackfillOperationsProcessed(instance string, count int)
	IncBackfillRetries(instance string)
	ObserveBackfillBatchLedgersProcessed(instance string, count int)
	ObserveBackfillDuration(instance string, duration float64)
}

// MetricsService handles all metrics for the wallet-backend
type metricsService struct {
	registry *prometheus.Registry
	db       *sqlx.DB

	// Ingest Service Metrics
	latestLedgerIngested prometheus.Gauge
	oldestLedgerIngested prometheus.Gauge
	ingestionDuration    *prometheus.HistogramVec

	// Account Metrics
	activeAccounts prometheus.Gauge

	// RPC Service Metrics (transport-level)
	rpcRequestsTotal     *prometheus.CounterVec
	rpcRequestsDuration  *prometheus.SummaryVec
	rpcEndpointFailures  *prometheus.CounterVec
	rpcEndpointSuccesses *prometheus.CounterVec
	rpcServiceHealth     prometheus.Gauge
	rpcLatestLedger      prometheus.Gauge

	// RPC Method Metrics (application-level)
	rpcMethodCallsTotal  *prometheus.CounterVec
	rpcMethodDuration    *prometheus.SummaryVec
	rpcMethodErrorsTotal *prometheus.CounterVec

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

	// State Change Metrics
	stateChangeProcessingDuration *prometheus.HistogramVec
	stateChangesTotal             *prometheus.CounterVec

	// Ingestion Phase Metrics
	ingestionPhaseDuration     *prometheus.HistogramVec
	ingestionLedgersProcessed  prometheus.Counter
	ingestionTransactionsTotal prometheus.Counter
	ingestionOperationsTotal   prometheus.Counter
	ingestionBatchSize         prometheus.Histogram
	ingestionParticipantsCount prometheus.Histogram

	// GraphQL Metrics
	graphqlFieldDuration *prometheus.SummaryVec
	graphqlFieldsTotal   *prometheus.CounterVec
	graphqlComplexity    *prometheus.SummaryVec
	graphqlErrorsTotal   *prometheus.CounterVec

	// Backfill Metrics (Progress) - all GaugeVec with instance label
	backfillStartLedger      *prometheus.GaugeVec
	backfillEndLedger        *prometheus.GaugeVec
	backfillCurrentLedger    *prometheus.GaugeVec
	backfillBatchesTotal     *prometheus.GaugeVec
	backfillBatchesCompleted *prometheus.GaugeVec
	backfillBatchesFailed    *prometheus.GaugeVec

	// Backfill Metrics (Phase Durations)
	backfillPhaseDuration *prometheus.HistogramVec

	// Backfill Metrics (Counters) - all CounterVec with instance label
	backfillLedgersProcessed      *prometheus.CounterVec
	backfillTransactionsProcessed *prometheus.CounterVec
	backfillOperationsProcessed   *prometheus.CounterVec
	backfillRetriesTotal          *prometheus.CounterVec

	// Backfill Metrics (Performance) - all HistogramVec with instance label
	backfillBatchSize             *prometheus.HistogramVec
	backfillBatchLedgersProcessed *prometheus.HistogramVec
	backfillDuration              *prometheus.HistogramVec
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
			Name: "ingestion_ledger_latest",
			Help: "Latest ledger ingested",
		},
	)
	m.oldestLedgerIngested = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ingestion_ledger_oldest",
			Help: "Oldest ledger ingested (backfill boundary)",
		},
	)
	m.ingestionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ingestion_duration_seconds",
			Help:    "Duration of ledger ingestion",
			Buckets: []float64{0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.125, 0.15, 0.2, 0.3, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10},
		},
		[]string{},
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

	// RPC Method Metrics (application-level)
	m.rpcMethodCallsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_method_calls_total",
			Help: "Total number of RPC method calls at the application level",
		},
		[]string{"method"},
	)
	m.rpcMethodDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "rpc_method_duration_seconds",
			Help:       "Duration of RPC method execution including parsing and validation",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"method"},
	)
	m.rpcMethodErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_method_errors_total",
			Help: "Total number of RPC method errors by error type",
		},
		[]string{"method", "error_type"},
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

	// State Change Metrics
	m.stateChangeProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ingestion_state_change_processing_duration_seconds",
			Help:    "Duration of state change processing by processor type",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"processor"},
	)
	m.stateChangesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingestion_state_changes_total",
			Help: "Total number of state changes persisted to database by type and category",
		},
		[]string{"type", "category"},
	)

	// Ingestion Phase Metrics
	m.ingestionPhaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ingestion_phase_duration_seconds",
			Help:    "Duration of each ingestion phase",
			Buckets: []float64{0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1, 2, 3, 4, 5, 6, 7, 10, 30, 60},
		},
		[]string{"phase"},
	)
	m.ingestionLedgersProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingestion_ledgers_processed_total",
			Help: "Total number of ledgers processed during ingestion",
		},
	)
	m.ingestionTransactionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingestion_transactions_processed_total",
			Help: "Total number of transactions processed during ingestion",
		},
	)
	m.ingestionOperationsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingestion_operations_processed_total",
			Help: "Total number of operations processed during ingestion",
		},
	)
	m.ingestionBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestion_batch_size",
			Help:    "Number of ledgers processed per ingestion batch",
			Buckets: prometheus.ExponentialBuckets(1, 2, 8), // 1, 2, 4, 8, 16, 32, 64, 128
		},
	)
	m.ingestionParticipantsCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestion_participants_count",
			Help:    "Number of unique participants per ingestion batch",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048
		},
	)

	// GraphQL Metrics
	m.graphqlFieldDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "graphql_field_duration_seconds",
			Help:       "Duration of GraphQL field resolver execution",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"operation_name", "field_name"},
	)
	m.graphqlFieldsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "graphql_fields_total",
			Help: "Total number of GraphQL field resolutions",
		},
		[]string{"operation_name", "field_name", "success"},
	)
	m.graphqlComplexity = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "graphql_complexity",
			Help:       "GraphQL query complexity values",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"operation_name"},
	)
	m.graphqlErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "graphql_errors_total",
			Help: "Total number of GraphQL errors",
		},
		[]string{"operation_name", "error_type"},
	)

	// Backfill Progress Gauges (with instance label)
	m.backfillStartLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_start_ledger",
			Help: "Target start ledger for this backfill instance",
		},
		[]string{"instance"},
	)
	m.backfillEndLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_end_ledger",
			Help: "Target end ledger for this backfill instance",
		},
		[]string{"instance"},
	)
	m.backfillCurrentLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_current_ledger",
			Help: "Most recently processed ledger in this backfill instance",
		},
		[]string{"instance"},
	)
	m.backfillBatchesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_batches_total",
			Help: "Total number of batches to process in this backfill instance",
		},
		[]string{"instance"},
	)
	m.backfillBatchesCompleted = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_batches_completed",
			Help: "Number of batches successfully completed in this backfill instance",
		},
		[]string{"instance"},
	)
	m.backfillBatchesFailed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_batches_failed",
			Help: "Number of batches that failed in this backfill instance",
		},
		[]string{"instance"},
	)

	// Backfill Phase Duration (with instance AND phase labels)
	m.backfillPhaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "backfill_phase_duration_seconds",
			Help:    "Duration of each backfill phase",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"instance", "phase"},
	)

	// Backfill Counters (with instance label)
	m.backfillLedgersProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfill_ledgers_processed_total",
			Help: "Total ledgers processed during backfill",
		},
		[]string{"instance"},
	)
	m.backfillTransactionsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfill_transactions_processed_total",
			Help: "Total transactions processed during backfill",
		},
		[]string{"instance"},
	)
	m.backfillOperationsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfill_operations_processed_total",
			Help: "Total operations processed during backfill",
		},
		[]string{"instance"},
	)
	m.backfillRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfill_retries_total",
			Help: "Total number of retry attempts for ledger fetch during backfill",
		},
		[]string{"instance"},
	)

	// Backfill Performance Metrics (with instance label)
	m.backfillBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "backfill_batch_size",
			Help:    "Number of ledgers per batch in backfill",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
		},
		[]string{"instance"},
	)
	m.backfillBatchLedgersProcessed = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "backfill_batch_ledgers_processed",
			Help:    "Actual ledgers processed per batch in backfill",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
		},
		[]string{"instance"},
	)
	m.backfillDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "backfill_duration_seconds",
			Help:    "Total duration of backfill job",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 86400}, // 1min to 24h
		},
		[]string{"instance"},
	)

	m.registerMetrics()
	return m
}

func (m *metricsService) registerMetrics() {
	collector := sqlstats.NewStatsCollector("wallet-backend-db", m.db)
	m.registry.MustRegister(
		collector,
		m.latestLedgerIngested,
		m.oldestLedgerIngested,
		m.ingestionDuration,
		m.activeAccounts,
		m.rpcRequestsTotal,
		m.rpcRequestsDuration,
		m.rpcEndpointFailures,
		m.rpcEndpointSuccesses,
		m.rpcServiceHealth,
		m.rpcLatestLedger,
		m.rpcMethodCallsTotal,
		m.rpcMethodDuration,
		m.rpcMethodErrorsTotal,
		m.numRequestsTotal,
		m.requestsDuration,
		m.dbQueryDuration,
		m.dbQueriesTotal,
		m.dbQueryErrors,
		m.dbTransactions,
		m.dbTxnDuration,
		m.dbBatchSize,
		m.signatureVerificationExpired,
		m.stateChangeProcessingDuration,
		m.stateChangesTotal,
		m.ingestionPhaseDuration,
		m.ingestionLedgersProcessed,
		m.ingestionTransactionsTotal,
		m.ingestionOperationsTotal,
		m.ingestionBatchSize,
		m.ingestionParticipantsCount,
		m.graphqlFieldDuration,
		m.graphqlFieldsTotal,
		m.graphqlComplexity,
		m.graphqlErrorsTotal,
		// Backfill Progress
		m.backfillStartLedger,
		m.backfillEndLedger,
		m.backfillCurrentLedger,
		m.backfillBatchesTotal,
		m.backfillBatchesCompleted,
		m.backfillBatchesFailed,
		// Backfill Phases
		m.backfillPhaseDuration,
		// Backfill Counters
		m.backfillLedgersProcessed,
		m.backfillTransactionsProcessed,
		m.backfillOperationsProcessed,
		m.backfillRetriesTotal,
		// Backfill Performance
		m.backfillBatchSize,
		m.backfillBatchLedgersProcessed,
		m.backfillDuration,
	)
}

// RegisterPool registers a worker pool for metrics collection
func (m *metricsService) RegisterPoolMetrics(channel string, pool pond.Pool) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        "pool_workers_running",
			Help:        "Number of running worker goroutines",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.RunningWorkers())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        "pool_tasks_submitted_total",
			Help:        "Number of tasks submitted",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.SubmittedTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        "pool_tasks_waiting",
			Help:        "Number of tasks currently waiting in the queue",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.WaitingTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        "pool_tasks_successful_total",
			Help:        "Number of tasks that completed successfully",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.SuccessfulTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        "pool_tasks_failed_total",
			Help:        "Number of tasks that completed with panic",
			ConstLabels: prometheus.Labels{"channel": channel},
		},
		func() float64 {
			return float64(pool.FailedTasks())
		},
	))

	m.registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        "pool_tasks_completed_total",
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

func (m *metricsService) SetOldestLedgerIngested(value float64) {
	m.oldestLedgerIngested.Set(value)
}

func (m *metricsService) ObserveIngestionDuration(duration float64) {
	m.ingestionDuration.WithLabelValues().Observe(duration)
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

// RPC Method Metrics (application-level)
func (m *metricsService) IncRPCMethodCalls(method string) {
	m.rpcMethodCallsTotal.WithLabelValues(method).Inc()
}

func (m *metricsService) ObserveRPCMethodDuration(method string, duration float64) {
	m.rpcMethodDuration.WithLabelValues(method).Observe(duration)
}

func (m *metricsService) IncRPCMethodErrors(method, errorType string) {
	m.rpcMethodErrorsTotal.WithLabelValues(method, errorType).Inc()
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

// State Change Metrics
func (m *metricsService) ObserveStateChangeProcessingDuration(processor string, duration float64) {
	m.stateChangeProcessingDuration.WithLabelValues(processor).Observe(duration)
}

func (m *metricsService) IncStateChanges(stateChangeType, category string, count int) {
	m.stateChangesTotal.WithLabelValues(stateChangeType, category).Add(float64(count))
}

// Ingestion Phase Metrics
func (m *metricsService) ObserveIngestionPhaseDuration(phase string, duration float64) {
	m.ingestionPhaseDuration.WithLabelValues(phase).Observe(duration)
}

func (m *metricsService) IncIngestionLedgersProcessed(count int) {
	m.ingestionLedgersProcessed.Add(float64(count))
}

func (m *metricsService) IncIngestionTransactionsProcessed(count int) {
	m.ingestionTransactionsTotal.Add(float64(count))
}

func (m *metricsService) IncIngestionOperationsProcessed(count int) {
	m.ingestionOperationsTotal.Add(float64(count))
}

func (m *metricsService) ObserveIngestionBatchSize(size int) {
	m.ingestionBatchSize.Observe(float64(size))
}

func (m *metricsService) ObserveIngestionParticipantsCount(count int) {
	m.ingestionParticipantsCount.Observe(float64(count))
}

// GraphQL Metrics
func (m *metricsService) ObserveGraphQLFieldDuration(operationName, fieldName string, duration float64) {
	m.graphqlFieldDuration.WithLabelValues(operationName, fieldName).Observe(duration)
}

func (m *metricsService) IncGraphQLField(operationName, fieldName string, success bool) {
	successStr := "true"
	if !success {
		successStr = "false"
	}
	m.graphqlFieldsTotal.WithLabelValues(operationName, fieldName, successStr).Inc()
}

func (m *metricsService) ObserveGraphQLComplexity(operationName string, complexity int) {
	m.graphqlComplexity.WithLabelValues(operationName).Observe(float64(complexity))
}

func (m *metricsService) IncGraphQLError(operationName, errorType string) {
	m.graphqlErrorsTotal.WithLabelValues(operationName, errorType).Inc()
}

// Backfill Metrics
func (m *metricsService) SetBackfillStartLedger(instance string, ledger uint32) {
	m.backfillStartLedger.WithLabelValues(instance).Set(float64(ledger))
}

func (m *metricsService) SetBackfillEndLedger(instance string, ledger uint32) {
	m.backfillEndLedger.WithLabelValues(instance).Set(float64(ledger))
}

func (m *metricsService) SetBackfillCurrentLedger(instance string, ledger uint32) {
	m.backfillCurrentLedger.WithLabelValues(instance).Set(float64(ledger))
}

func (m *metricsService) SetBackfillBatchesTotal(instance string, count int) {
	m.backfillBatchesTotal.WithLabelValues(instance).Set(float64(count))
}

func (m *metricsService) IncBackfillBatchesCompleted(instance string) {
	m.backfillBatchesCompleted.WithLabelValues(instance).Inc()
}

func (m *metricsService) IncBackfillBatchesFailed(instance string) {
	m.backfillBatchesFailed.WithLabelValues(instance).Inc()
}

func (m *metricsService) ObserveBackfillPhaseDuration(instance string, phase string, duration float64) {
	m.backfillPhaseDuration.WithLabelValues(instance, phase).Observe(duration)
}

func (m *metricsService) IncBackfillLedgersProcessed(instance string, count int) {
	m.backfillLedgersProcessed.WithLabelValues(instance).Add(float64(count))
}

func (m *metricsService) IncBackfillTransactionsProcessed(instance string, count int) {
	m.backfillTransactionsProcessed.WithLabelValues(instance).Add(float64(count))
}

func (m *metricsService) IncBackfillOperationsProcessed(instance string, count int) {
	m.backfillOperationsProcessed.WithLabelValues(instance).Add(float64(count))
}

func (m *metricsService) IncBackfillRetries(instance string) {
	m.backfillRetriesTotal.WithLabelValues(instance).Inc()
}

func (m *metricsService) ObserveBackfillBatchSize(instance string, size int) {
	m.backfillBatchSize.WithLabelValues(instance).Observe(float64(size))
}

func (m *metricsService) ObserveBackfillBatchLedgersProcessed(instance string, count int) {
	m.backfillBatchLedgersProcessed.WithLabelValues(instance).Observe(float64(count))
}

func (m *metricsService) ObserveBackfillDuration(instance string, duration float64) {
	m.backfillDuration.WithLabelValues(instance).Observe(duration)
}
