package metrics

import (
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
	ingestionDuration                   prometheus.Histogram

	// Account Service Metrics
	numAccountsRegistered *prometheus.CounterVec
	numAccountsDeregistered *prometheus.CounterVec
}

// NewMetricsService creates a new metrics service with all metrics registered
func NewMetricsService(db *sqlx.DB) *MetricsService {
	m := &MetricsService{
		registry: prometheus.NewRegistry(),
		db:       db,
	}

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

	m.ingestionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestion_duration_seconds",
			Help:    "Duration of ledger ingestion",
			Buckets: prometheus.DefBuckets,
		},
	)

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

func (m *MetricsService) ObserveIngestionDuration(duration float64) {
	m.ingestionDuration.Observe(duration)
}

// Account Service Metrics
func (m *MetricsService) SetNumAccountsRegistered(address string) {
	m.numAccountsRegistered.WithLabelValues(address).Inc()
}

func (m *MetricsService) SetNumAccountsDeregistered(address string) {
	m.numAccountsDeregistered.WithLabelValues(address).Inc()
}
