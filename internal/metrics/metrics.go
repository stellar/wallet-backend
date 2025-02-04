package metrics

import (
	"strconv"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

// MetricsService handles all metrics for the wallet-backend
type MetricsService struct {
	registry *prometheus.Registry
	db       *sqlx.DB

	// Ingest Service Metrics
	numPaymentOpsIngestedPerLedger      *prometheus.GaugeVec
	numTssTransactionsIngestedPerLedger *prometheus.GaugeVec
	latestLedgerIngested                prometheus.Gauge
	ingestionDuration                   *prometheus.SummaryVec

	// Account Metrics
	numAccountsRegistered   *prometheus.CounterVec
	numAccountsDeregistered *prometheus.CounterVec

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
	httpErrors       *prometheus.CounterVec
}

// NewMetricsService creates a new metrics service with all metrics registered
func NewMetricsService(db *sqlx.DB) *MetricsService {
	m := &MetricsService{
		registry: prometheus.NewRegistry(),
		db:       db,
	}

	// Ingest Service Metrics
	m.numPaymentOpsIngestedPerLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "num_payment_ops_ingested_per_ledger",
			Help: "Number of payment operations ingested per ledger",
		},
		[]string{"operation_type"},
	)
	m.numTssTransactionsIngestedPerLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "num_tss_transactions_ingested_per_ledger",
			Help: "Number of tss transactions ingested per ledger",
		},
		[]string{"status"},
	)
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
	m.numAccountsRegistered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "num_accounts_registered",
			Help: "Number of accounts registered",
		},
		[]string{"address"},
	)
	m.numAccountsDeregistered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "num_accounts_deregistered",
			Help: "Number of accounts deregistered",
		},
		[]string{"address"},
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
		[]string{"endpoint", "method"},
	)
	m.requestsDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "http_request_duration_seconds",
			Help:       "Duration of HTTP requests in seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"endpoint", "method"},
	)
	m.httpErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_errors_total",
			Help: "Total number of HTTP errors",
		},
		[]string{"endpoint", "method", "status_code", "error_class"},
	)

	m.registerMetrics()
	return m
}

func (m *MetricsService) registerMetrics() {
	collector := sqlstats.NewStatsCollector("wallet-backend-db", m.db)
	m.registry.MustRegister(collector)
	m.registry.MustRegister(m.numPaymentOpsIngestedPerLedger)
	m.registry.MustRegister(m.numTssTransactionsIngestedPerLedger)
	m.registry.MustRegister(m.latestLedgerIngested)
	m.registry.MustRegister(m.ingestionDuration)
	m.registry.MustRegister(m.numAccountsRegistered)
	m.registry.MustRegister(m.numAccountsDeregistered)
	m.registry.MustRegister(m.rpcRequestsTotal)
	m.registry.MustRegister(m.rpcRequestsDuration)
	m.registry.MustRegister(m.rpcEndpointFailures)
	m.registry.MustRegister(m.rpcEndpointSuccesses)
	m.registry.MustRegister(m.rpcServiceHealth)
	m.registry.MustRegister(m.rpcLatestLedger)
	m.registry.MustRegister(m.numRequestsTotal)
	m.registry.MustRegister(m.requestsDuration)
	m.registry.MustRegister(m.httpErrors)
}

// GetRegistry returns the prometheus registry
func (m *MetricsService) GetRegistry() *prometheus.Registry {
	return m.registry
}

// Ingest Service Metrics
func (m *MetricsService) SetNumPaymentOpsIngestedPerLedger(operationType string, value int) {
	m.numPaymentOpsIngestedPerLedger.WithLabelValues(operationType).Set(float64(value))
}

func (m *MetricsService) SetNumTssTransactionsIngestedPerLedger(status string, value float64) {
	m.numTssTransactionsIngestedPerLedger.WithLabelValues(status).Set(value)
}

func (m *MetricsService) SetLatestLedgerIngested(value float64) {
	m.latestLedgerIngested.Set(value)
}

func (m *MetricsService) ObserveIngestionDuration(ingestionType string, duration float64) {
	m.ingestionDuration.WithLabelValues(ingestionType).Observe(duration)
}

// Account Service Metrics
func (m *MetricsService) IncNumAccountsRegistered(address string) {
	m.numAccountsRegistered.WithLabelValues(address).Inc()
}

func (m *MetricsService) IncNumAccountsDeregistered(address string) {
	m.numAccountsDeregistered.WithLabelValues(address).Inc()
}

// RPC Service Metrics
func (m *MetricsService) IncRPCRequests(endpoint string) {
	m.rpcRequestsTotal.WithLabelValues(endpoint).Inc()
}

func (m *MetricsService) ObserveRPCRequestDuration(endpoint string, duration float64) {
	m.rpcRequestsDuration.WithLabelValues(endpoint).Observe(duration)
}

func (m *MetricsService) IncRPCEndpointFailure(endpoint string) {
	m.rpcEndpointFailures.WithLabelValues(endpoint).Inc()
}

func (m *MetricsService) IncRPCEndpointSuccess(endpoint string) {
	m.rpcEndpointSuccesses.WithLabelValues(endpoint).Inc()
}

func (m *MetricsService) SetRPCServiceHealth(healthy bool) {
	if healthy {
		m.rpcServiceHealth.Set(1)
	} else {
		m.rpcServiceHealth.Set(0)
	}
}

func (m *MetricsService) SetRPCLatestLedger(ledger int64) {
	m.rpcLatestLedger.Set(float64(ledger))
}

// HTTP Request Metrics
func (m *MetricsService) IncNumRequests(endpoint, method string) {
	m.numRequestsTotal.WithLabelValues(endpoint, method).Inc()
}

func (m *MetricsService) ObserveRequestDuration(endpoint, method string, duration float64) {
	m.requestsDuration.WithLabelValues(endpoint, method).Observe(duration)
}

func (m *MetricsService) IncHTTPError(endpoint, method string, statusCode int) {
	errorClass := "5xx"
	if statusCode < 500 {
		errorClass = "4xx"
	}
	m.httpErrors.WithLabelValues(
		endpoint,
		method,
		strconv.Itoa(statusCode),
		errorClass,
	).Inc()
}
