package metrics

import (
	"github.com/alitto/pond"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

// MockMetricsService is a mock implementation of MetricsService
type MockMetricsService struct {
	mock.Mock
}

// NewMockMetricsService creates a new mock metrics service
func NewMockMetricsService() *MockMetricsService {
	return &MockMetricsService{}
}

func (m *MockMetricsService) RegisterPoolMetrics(channel string, pool *pond.WorkerPool) {
	m.Called(channel, pool)
}

func (m *MockMetricsService) GetRegistry() *prometheus.Registry {
	args := m.Called()
	return args.Get(0).(*prometheus.Registry)
}

func (m *MockMetricsService) SetLatestLedgerIngested(value float64) {
	m.Called(value)
}

func (m *MockMetricsService) ObserveIngestionDuration(ingestionType string, duration float64) {
	m.Called(ingestionType, duration)
}

func (m *MockMetricsService) IncActiveAccount() {
	m.Called()
}

func (m *MockMetricsService) DecActiveAccount() {
	m.Called()
}

func (m *MockMetricsService) IncRPCRequests(endpoint string) {
	m.Called(endpoint)
}

func (m *MockMetricsService) ObserveRPCRequestDuration(endpoint string, duration float64) {
	m.Called(endpoint, duration)
}

func (m *MockMetricsService) IncRPCEndpointFailure(endpoint string) {
	m.Called(endpoint)
}

func (m *MockMetricsService) IncRPCEndpointSuccess(endpoint string) {
	m.Called(endpoint)
}

func (m *MockMetricsService) SetRPCServiceHealth(healthy bool) {
	m.Called(healthy)
}

func (m *MockMetricsService) SetRPCLatestLedger(ledger int64) {
	m.Called(ledger)
}

func (m *MockMetricsService) IncNumRequests(endpoint, method string, statusCode int) {
	m.Called(endpoint, method, statusCode)
}

func (m *MockMetricsService) ObserveRequestDuration(endpoint, method string, duration float64) {
	m.Called(endpoint, method, duration)
}

func (m *MockMetricsService) ObserveDBQueryDuration(queryType, table string, duration float64) {
	m.Called(queryType, table, duration)
}

func (m *MockMetricsService) IncDBQuery(queryType, table string) {
	m.Called(queryType, table)
}

func (m *MockMetricsService) IncSignatureVerificationExpired(expiredSeconds float64) {
	m.Called(expiredSeconds)
}

// Ingestion Phase Metrics
func (m *MockMetricsService) ObserveIngestionPhaseDuration(phase string, duration float64) {
	m.Called(phase, duration)
}

func (m *MockMetricsService) IncIngestionLedgersProcessed(count int) {
	m.Called(count)
}

func (m *MockMetricsService) IncIngestionTransactionsProcessed(count int) {
	m.Called(count)
}

func (m *MockMetricsService) ObserveIngestionBatchSize(size int) {
	m.Called(size)
}

func (m *MockMetricsService) ObserveIngestionParticipantsCount(count int) {
	m.Called(count)
}
