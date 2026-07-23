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
	// Duration observes ledger processing time: process_ledger + prepare_classification +
	// insert_into_db. It excludes the ledger-fetch phase (including tip-wait) by design — see
	// LedgerFetchDuration for that — since fetch time is bounded by backend/tip latency, not by
	// this process's own work.
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
	// LedgerFetchDuration observes time to fetch a ledger from the backend, including retry
	// overhead AND tip-wait: at the live tip, the backend blocks GetLedger until the next ledger
	// closes, so this metric's steady-state floor tracks ledger close cadence (~5-6s) rather than
	// pure I/O latency — that is the intended semantics, not a measurement bug.
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
	// WasmClassificationFailuresTotal counts WASM classification failures —
	// genuine internal errors, not legitimate non-matches. The protocol_id
	// label is the validator that was attempted (e.g. "sep41"), or "unknown"
	// when spec extraction failed before any validator ran; it is never NULL.
	// The reason label distinguishes spec_extraction_error from
	// validate_error. Independently of this metric, the protocol_wasms row
	// for the affected WASM is left with its protocol_id column NULL; recover
	// by re-running the protocol-setup CLI.
	// PromQL: rate(wallet_ingestion_wasm_classification_failures_total[5m]) > 0
	WasmClassificationFailuresTotal *prometheus.CounterVec
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
			Help:    "Duration of ledger processing (process + persist). Excludes the ledger-fetch phase, including tip-wait; see wallet_ingestion_ledger_fetch_duration_seconds for that.",
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
			Help:    "Time to fetch a ledger from the backend, including retry overhead and tip-wait. At the live tip, this tracks ledger close cadence rather than pure fetch latency — that is the intended semantics.",
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
			Help:    "Duration of protocol state persistence by protocol ID and phase (process_ledger, load_current_state, persist_history, persist_current_state).",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		}, []string{"protocol_id", "phase"}),
		WasmClassificationFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_ingestion_wasm_classification_failures_total",
			Help: "WASM classification failures by attempted validator (protocol_id label; \"unknown\" if spec extraction failed) and reason. The corresponding protocol_wasms.protocol_id column is left NULL; recover via the protocol-setup CLI.",
		}, []string{"protocol_id", "reason"}),
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
		m.WasmClassificationFailuresTotal,
	)
	return m
}
