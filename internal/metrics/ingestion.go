package metrics

import "github.com/prometheus/client_golang/prometheus"

// IngestionMetrics holds Prometheus collectors for ledger ingestion.
type IngestionMetrics struct {
	LatestLedger      prometheus.Gauge
	OldestLedger      prometheus.Gauge
	Duration          *prometheus.HistogramVec
	PhaseDuration     *prometheus.HistogramVec
	LedgersProcessed  prometheus.Counter
	TransactionsTotal prometheus.Counter
	OperationsTotal   prometheus.Counter
	BatchSize         prometheus.Histogram
	ParticipantsCount prometheus.Histogram

	// State change metrics
	StateChangeProcessingDuration *prometheus.HistogramVec
	StateChangesTotal             *prometheus.CounterVec
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
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_duration_seconds",
			Help:    "Duration of ledger ingestion.",
			Buckets: []float64{0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.125, 0.15, 0.2, 0.3, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10},
		}, []string{}),
		PhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_phase_duration_seconds",
			Help:    "Duration of each ingestion phase.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1, 2, 3, 4, 5, 6, 7, 10, 30, 60},
		}, []string{"phase"}),
		LedgersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_ledgers_processed_total",
			Help: "Total number of ledgers processed during ingestion.",
		}),
		TransactionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_transactions_processed_total",
			Help: "Total number of transactions processed during ingestion.",
		}),
		OperationsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_ingestion_operations_processed_total",
			Help: "Total number of operations processed during ingestion.",
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_batch_size",
			Help:    "Number of ledgers processed per ingestion batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 8),
		}),
		ParticipantsCount: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_participants_count",
			Help:    "Number of unique participants per ingestion batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		StateChangeProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_ingestion_state_change_processing_duration_seconds",
			Help:    "Duration of state change processing by processor type.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		}, []string{"processor"}),
		StateChangesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_state_changes_total",
			Help: "Total number of state changes persisted to database by type and category.",
		}, []string{"type", "category"}),
	}
	reg.MustRegister(
		m.LatestLedger,
		m.OldestLedger,
		m.Duration,
		m.PhaseDuration,
		m.LedgersProcessed,
		m.TransactionsTotal,
		m.OperationsTotal,
		m.BatchSize,
		m.ParticipantsCount,
		m.StateChangeProcessingDuration,
		m.StateChangesTotal,
	)
	return m
}
