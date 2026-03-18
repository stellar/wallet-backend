package metrics

import (
	"strconv"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus collectors for the wallet-backend service.
type Metrics struct {
	DB        *DBMetrics
	RPC       *RPCMetrics
	Ingestion *IngestionMetrics
	HTTP      *HTTPMetrics
	GraphQL   *GraphQLMetrics
	Auth      *AuthMetrics
	registry  *prometheus.Registry
}

// NewMetrics creates a new Metrics instance with all sub-struct collectors registered.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	return &Metrics{
		DB:        newDBMetrics(reg),
		RPC:       newRPCMetrics(reg),
		Ingestion: newIngestionMetrics(reg),
		HTTP:      newHTTPMetrics(reg),
		GraphQL:   newGraphQLMetrics(reg),
		Auth:      newAuthMetrics(reg),
		registry:  reg,
	}
}

// Registry returns the prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// RegisterPoolMetrics registers pond worker pool metrics on this Metrics' registry.
func (m *Metrics) RegisterPoolMetrics(channel string, pool pond.Pool) {
	RegisterPoolMetrics(m.registry, channel, pool)
}

// RegisterDBPoolMetrics registers pgxpool connection pool metrics on this Metrics' registry.
func (m *Metrics) RegisterDBPoolMetrics(pool *pgxpool.Pool) {
	RegisterDBPoolMetrics(m.registry, pool)
}

// ---------------------------------------------------------------------------
// Legacy interface — kept temporarily so the codebase compiles during migration.
// Will be deleted once all consumers are migrated to use concrete Metrics struct.
// ---------------------------------------------------------------------------

type MetricsService interface {
	RegisterPoolMetrics(channel string, pool pond.Pool)
	RegisterDBPoolMetrics(pool *pgxpool.Pool)
	GetRegistry() *prometheus.Registry
	SetLatestLedgerIngested(value float64)
	SetOldestLedgerIngested(value float64)
	ObserveIngestionDuration(duration float64)
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
}

// metricsService implements MetricsService by delegating to the new Metrics struct.
type metricsService struct {
	*Metrics
}

// NewMetricsService creates a new MetricsService backed by the new Metrics struct.
func NewMetricsService() MetricsService {
	reg := prometheus.NewRegistry()
	return &metricsService{Metrics: NewMetrics(reg)}
}

func (m *metricsService) GetRegistry() *prometheus.Registry { return m.registry }

// Ingest Service Metrics

func (m *metricsService) SetLatestLedgerIngested(value float64) {
	m.Ingestion.LatestLedger.Set(value)
}

func (m *metricsService) SetOldestLedgerIngested(value float64) {
	m.Ingestion.OldestLedger.Set(value)
}

func (m *metricsService) ObserveIngestionDuration(duration float64) {
	m.Ingestion.Duration.WithLabelValues().Observe(duration)
}

// RPC Service Metrics
func (m *metricsService) IncRPCRequests(endpoint string) {
	m.RPC.RequestsTotal.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) ObserveRPCRequestDuration(endpoint string, duration float64) {
	m.RPC.RequestsDuration.WithLabelValues(endpoint).Observe(duration)
}

func (m *metricsService) IncRPCEndpointFailure(endpoint string) {
	m.RPC.EndpointFailures.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) IncRPCEndpointSuccess(endpoint string) {
	m.RPC.EndpointSuccesses.WithLabelValues(endpoint).Inc()
}

func (m *metricsService) SetRPCServiceHealth(healthy bool) {
	if healthy {
		m.RPC.ServiceHealth.Set(1)
	} else {
		m.RPC.ServiceHealth.Set(0)
	}
}

func (m *metricsService) SetRPCLatestLedger(ledger int64) {
	m.RPC.LatestLedger.Set(float64(ledger))
}

// RPC Method Metrics (application-level)
func (m *metricsService) IncRPCMethodCalls(method string) {
	m.RPC.MethodCallsTotal.WithLabelValues(method).Inc()
}

func (m *metricsService) ObserveRPCMethodDuration(method string, duration float64) {
	m.RPC.MethodDuration.WithLabelValues(method).Observe(duration)
}

func (m *metricsService) IncRPCMethodErrors(method, errorType string) {
	m.RPC.MethodErrorsTotal.WithLabelValues(method, errorType).Inc()
}

// HTTP Request Metrics
func (m *metricsService) IncNumRequests(endpoint, method string, statusCode int) {
	m.HTTP.RequestsTotal.WithLabelValues(endpoint, method, strconv.Itoa(statusCode)).Inc()
}

func (m *metricsService) ObserveRequestDuration(endpoint, method string, duration float64) {
	m.HTTP.RequestsDuration.WithLabelValues(endpoint, method).Observe(duration)
}

// DB Query Metrics
func (m *metricsService) ObserveDBQueryDuration(queryType, table string, duration float64) {
	m.DB.QueryDuration.WithLabelValues(queryType, table).Observe(duration)
}

func (m *metricsService) IncDBQuery(queryType, table string) {
	m.DB.QueriesTotal.WithLabelValues(queryType, table).Inc()
}

func (m *metricsService) IncDBQueryError(queryType, table, errorType string) {
	m.DB.QueryErrors.WithLabelValues(queryType, table, errorType).Inc()
}

func (m *metricsService) IncDBTransaction(status string) {
	m.DB.TransactionsTotal.WithLabelValues(status).Inc()
}

func (m *metricsService) ObserveDBTransactionDuration(status string, duration float64) {
	m.DB.TransactionDuration.WithLabelValues(status).Observe(duration)
}

func (m *metricsService) ObserveDBBatchSize(operation, table string, size int) {
	m.DB.BatchSize.WithLabelValues(operation, table).Observe(float64(size))
}

// Signature Verification Metrics
func (m *metricsService) IncSignatureVerificationExpired(expiredSeconds float64) {
	m.Auth.ExpiredSignaturesTotal.Inc()
	_ = expiredSeconds // Previously used as label; now just count occurrences.
}

// State Change Metrics
func (m *metricsService) ObserveStateChangeProcessingDuration(processor string, duration float64) {
	m.Ingestion.StateChangeProcessingDuration.WithLabelValues(processor).Observe(duration)
}

func (m *metricsService) IncStateChanges(stateChangeType, category string, count int) {
	m.Ingestion.StateChangesTotal.WithLabelValues(stateChangeType, category).Add(float64(count))
}

// Ingestion Phase Metrics
func (m *metricsService) ObserveIngestionPhaseDuration(phase string, duration float64) {
	m.Ingestion.PhaseDuration.WithLabelValues(phase).Observe(duration)
}

func (m *metricsService) IncIngestionLedgersProcessed(count int) {
	m.Ingestion.LedgersProcessed.Add(float64(count))
}

func (m *metricsService) IncIngestionTransactionsProcessed(count int) {
	m.Ingestion.TransactionsTotal.Add(float64(count))
}

func (m *metricsService) IncIngestionOperationsProcessed(count int) {
	m.Ingestion.OperationsTotal.Add(float64(count))
}

func (m *metricsService) ObserveIngestionBatchSize(size int) {
	m.Ingestion.BatchSize.Observe(float64(size))
}

func (m *metricsService) ObserveIngestionParticipantsCount(count int) {
	m.Ingestion.ParticipantsCount.Observe(float64(count))
}

// GraphQL Metrics
func (m *metricsService) ObserveGraphQLFieldDuration(operationName, fieldName string, duration float64) {
	m.GraphQL.FieldDuration.WithLabelValues(operationName, fieldName).Observe(duration)
}

func (m *metricsService) IncGraphQLField(operationName, fieldName string, success bool) {
	successStr := "true"
	if !success {
		successStr = "false"
	}
	m.GraphQL.FieldsTotal.WithLabelValues(operationName, fieldName, successStr).Inc()
}

func (m *metricsService) ObserveGraphQLComplexity(operationName string, complexity int) {
	m.GraphQL.Complexity.WithLabelValues(operationName).Observe(float64(complexity))
}

func (m *metricsService) IncGraphQLError(operationName, errorType string) {
	m.GraphQL.ErrorsTotal.WithLabelValues(operationName, errorType).Inc()
}

// Legacy compatibility: RegisterPoolMetrics on metricsService delegates to the new function.
func (m *metricsService) RegisterPoolMetrics(channel string, pool pond.Pool) {
	RegisterPoolMetrics(m.registry, channel, pool)
}

// Legacy compatibility: RegisterDBPoolMetrics on metricsService delegates to the new function.
func (m *metricsService) RegisterDBPoolMetrics(pool *pgxpool.Pool) {
	RegisterDBPoolMetrics(m.registry, pool)
}

