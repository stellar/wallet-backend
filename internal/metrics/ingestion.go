package metrics

import "github.com/prometheus/client_golang/prometheus"

// IngestionMetrics holds Prometheus collectors for ledger ingestion.
type IngestionMetrics struct {
	// LatestLedger tracks the most recently ingested ledger sequence number.
	// PromQL: wallet_ingestion_latest_ledger
	LatestLedger prometheus.Gauge
	// OldestLedger tracks the oldest ingested ledger (backfill boundary).
	// PromQL: wallet_ingestion_oldest_ledger
	OldestLedger prometheus.Gauge
	// Duration observes end-to-end ledger ingestion time (fetch + process + persist).
	// PromQL: histogram_quantile(0.99, rate(wallet_ingestion_duration_seconds_bucket[5m]))
	Duration prometheus.Histogram
	// PhaseDuration observes per-phase ingestion time, labeled by phase.
	// PromQL: histogram_quantile(0.99, rate(wallet_ingestion_phase_duration_seconds_bucket{phase="process_ledger"}[5m]))
	PhaseDuration *prometheus.HistogramVec
	// LedgersProcessed counts total ledgers ingested.
	// PromQL: rate(wallet_ingestion_ledgers_total[5m])
	LedgersProcessed prometheus.Counter
	// TransactionsTotal counts total transactions ingested.
	// PromQL: rate(wallet_ingestion_transactions_total[5m])
	TransactionsTotal prometheus.Counter
	// OperationsTotal counts total operations ingested.
	// PromQL: rate(wallet_ingestion_operations_total[5m])
	OperationsTotal prometheus.Counter
	// ParticipantsCount observes unique participants per ledger.
	// PromQL: histogram_quantile(0.5, rate(wallet_ingestion_participants_per_ledger_bucket[5m]))
	ParticipantsCount prometheus.Histogram
	// LagLedgers tracks how far behind ingestion is from the backend tip.
	// PromQL: wallet_ingestion_lag_ledgers > 100
	LagLedgers prometheus.Gauge
	// LedgerFetchDuration observes time to fetch a ledger from the backend (including retries).
	// PromQL: histogram_quantile(0.99, rate(wallet_ingestion_ledger_fetch_duration_seconds_bucket[5m]))
	LedgerFetchDuration prometheus.Histogram
	// RetriesTotal counts individual retry attempts by operation.
	// PromQL: rate(wallet_ingestion_retries_total{operation="ledger_fetch"}[5m])
	RetriesTotal *prometheus.CounterVec
	// RetryExhaustionsTotal counts when all retries are exhausted (fatal) by operation.
	// PromQL: increase(wallet_ingestion_retry_exhaustions_total[1h]) > 0
	RetryExhaustionsTotal *prometheus.CounterVec
	// ErrorsTotal counts ingestion errors by operation.
	// PromQL: rate(wallet_ingestion_errors_total[5m])
	ErrorsTotal *prometheus.CounterVec
	// State change metrics
	StateChangeProcessingDuration *prometheus.HistogramVec
	StateChangesTotal             *prometheus.CounterVec
	// Protocol state production metrics
	ProtocolStateProcessingDuration *prometheus.HistogramVec
	ProtocolContractCacheAccess     *prometheus.CounterVec
	ProtocolContractCacheRefresh    prometheus.Histogram
}

func newIngestionMetrics(reg prometheus.Registerer) *IngestionMetrics {
	m := &IngestionMetrics{
		LatestLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_ingestion_latest_ledger",
			Help: "Latest ledger ingested.",
		}),
		OldestLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_ingestion_oldest_ledger",
			Help: "Oldest ledger ingested (backfill boundary).",
		}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_duration_seconds",
			Help:    "Duration of ledger ingestion.",
			Buckets: []float64{0.05, 0.1, 0.15, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7, 10},
		}),
		PhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_phase_duration_seconds",
			Help:    "Duration of each ingestion phase.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60},
		}, []string{"phase"}),
		LedgersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_ledgers_total",
			Help: "Total number of ledgers processed during ingestion.",
		}),
		TransactionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_transactions_total",
			Help: "Total number of transactions processed during ingestion.",
		}),
		OperationsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_operations_total",
			Help: "Total number of operations processed during ingestion.",
		}),
		ParticipantsCount: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_participants_per_ledger",
			Help:    "Number of unique participants per ingestion batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		LagLedgers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_ingestion_lag_ledgers",
			Help: "Ledger backend tip minus current ingestion position.",
		}),
		LedgerFetchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_ledger_fetch_duration_seconds",
			Help:    "Time to fetch a ledger from the backend including retry overhead.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_retries_total",
			Help: "Individual retry attempts by operation.",
		}, []string{"operation"}),
		RetryExhaustionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_retry_exhaustions_total",
			Help: "Retry exhaustions (all attempts failed) by operation.",
		}, []string{"operation"}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_errors_total",
			Help: "Ingestion errors by operation.",
		}, []string{"operation"}),
		StateChangeProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_state_change_processing_duration_seconds",
			Help:    "Duration of state change processing by processor type.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		}, []string{"processor"}),
		StateChangesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_state_changes_total",
			Help: "Total number of state changes persisted to database by type and category.",
		}, []string{"type", "category"}),
		ProtocolStateProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_protocol_state_processing_duration_seconds",
			Help:    "Duration of protocol state persistence by protocol ID and phase (persist_history, persist_current_state).",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		}, []string{"protocol_id", "phase"}),
		ProtocolContractCacheAccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_protocol_contract_cache_access_total",
			Help: "Protocol contract cache accesses by protocol ID and result (hit, miss, refresh).",
		}, []string{"protocol_id", "result"}),
		ProtocolContractCacheRefresh: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_protocol_contract_cache_refresh_duration_seconds",
			Help:    "Duration of protocol contract cache refresh queries.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		}),
	}
	reg.MustRegister(
		m.LatestLedger,
		m.OldestLedger,
		m.Duration,
		m.PhaseDuration,
		m.LedgersProcessed,
		m.TransactionsTotal,
		m.OperationsTotal,
		m.ParticipantsCount,
		m.LagLedgers,
		m.LedgerFetchDuration,
		m.RetriesTotal,
		m.RetryExhaustionsTotal,
		m.ErrorsTotal,
		m.StateChangeProcessingDuration,
		m.StateChangesTotal,
		m.ProtocolStateProcessingDuration,
		m.ProtocolContractCacheAccess,
		m.ProtocolContractCacheRefresh,
	)
	return m
}
