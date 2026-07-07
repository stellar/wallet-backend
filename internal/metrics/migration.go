package metrics

import "github.com/prometheus/client_golang/prometheus"

// Migration status gauge values for MigrationMetrics.Status.
const (
	MigrationStatusInProgress float64 = 0
	MigrationStatusSuccess    float64 = 1
	MigrationStatusFailed     float64 = 2
)

// MigrationMetrics holds Prometheus collectors for protocol-migrate runs
// (current-state and history). Loop-level metrics carry no protocol_id label
// because each ledger is fetched once and shared across all protocols;
// per-protocol metrics carry a protocol_id label.
type MigrationMetrics struct {
	// CurrentLedger is the latest ledger sequence the loop has processed.
	// PromQL: wallet_migration_current_ledger
	CurrentLedger prometheus.Gauge
	// LedgersProcessed counts ledgers processed; rate() yields live l/s.
	// PromQL: rate(wallet_migration_ledgers_total[$__rate_interval])
	LedgersProcessed prometheus.Counter
	// PhaseDuration observes per-stage wall time, labeled by phase
	// (fetch, extract, process, flush). rate(_sum) yields the time-share.
	// PromQL: histogram_quantile(0.99, sum by (le, phase) (rate(wallet_migration_phase_duration_seconds_bucket[$__rate_interval])))
	PhaseDuration *prometheus.HistogramVec
	// TargetTip is the real chain tip (RPC getHealth latestLedger), the
	// denominator for % remaining. 0 when no RPC tip provider is configured.
	// PromQL: wallet_migration_target_tip
	TargetTip prometheus.Gauge
	// Cursor is the committed CAS cursor (current position) per protocol.
	// PromQL: wallet_migration_cursor
	Cursor *prometheus.GaugeVec
	// StartLedger is where each protocol's run began (% denominator base).
	// PromQL: wallet_migration_start_ledger
	StartLedger *prometheus.GaugeVec
	// Handoffs counts CAS-loss-to-live-ingestion events per protocol (convergence).
	// PromQL: increase(wallet_migration_handoffs_total[$__range])
	Handoffs *prometheus.CounterVec
	// Status reports per-protocol migration status (see Migration* consts):
	// 0=in_progress, 1=success, 2=failed.
	// PromQL: wallet_migration_status
	Status *prometheus.GaugeVec
}

func newMigrationMetrics(reg prometheus.Registerer) *MigrationMetrics {
	m := &MigrationMetrics{
		CurrentLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_migration_current_ledger",
			Help: "Latest ledger sequence processed by the protocol migration loop.",
		}),
		LedgersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_migration_ledgers_total",
			Help: "Total ledgers processed during protocol migration.",
		}),
		PhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wallet_migration_phase_duration_seconds",
			Help: "Duration of each migration phase (fetch, extract, process, flush).",
			// Tail past 10s covers a large window flush (thousands of ledgers in one commit) so
			// its p99 quantile isn't pinned to the top bucket; the _sum time-share is exact regardless.
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 20, 30},
		}, []string{"phase"}),
		TargetTip: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_migration_target_tip",
			Help: "Real chain tip (RPC getHealth latestLedger) used as the %-remaining denominator; 0 if no RPC tip provider.",
		}),
		Cursor: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wallet_migration_cursor",
			Help: "Committed CAS cursor (current position) per protocol.",
		}, []string{"protocol_id"}),
		StartLedger: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wallet_migration_start_ledger",
			Help: "Committed cursor at this protocol's migration start; %-complete denominator base (Cursor - StartLedger = ledgers migrated).",
		}, []string{"protocol_id"}),
		Handoffs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_migration_handoffs_total",
			Help: "CAS-loss-to-live-ingestion handoff events per protocol.",
		}, []string{"protocol_id"}),
		Status: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wallet_migration_status",
			Help: "Per-protocol migration status: 0=in_progress, 1=success, 2=failed.",
		}, []string{"protocol_id"}),
	}
	reg.MustRegister(
		m.CurrentLedger,
		m.LedgersProcessed,
		m.PhaseDuration,
		m.TargetTip,
		m.Cursor,
		m.StartLedger,
		m.Handoffs,
		m.Status,
	)
	return m
}
