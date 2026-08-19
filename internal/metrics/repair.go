package metrics

import "github.com/prometheus/client_golang/prometheus"

// Outcome label values for RepairMetrics.Outcomes.
const (
	RepairOutcomeApplied   = "applied"
	RepairOutcomeUnchanged = "unchanged"
	RepairOutcomeSkipped   = "skipped"
	RepairOutcomeGaveUp    = "gave_up"
)

// RepairMetrics holds Prometheus collectors for protocol-repair runs. Every
// metric carries a protocol_id label: a run repairs one protocol's current
// state.
type RepairMetrics struct {
	// UnitsChecked counts units entering the fetch/apply loop; its rate is run progress.
	// PromQL: rate(wallet_repair_units_checked_total[$__rate_interval])
	UnitsChecked *prometheus.CounterVec
	// Outcomes counts terminal per-unit outcomes, labeled by outcome (see
	// RepairOutcome* consts): applied, unchanged, skipped, gave_up.
	// PromQL: sum by (outcome) (increase(wallet_repair_unit_outcomes_total[$__range]))
	Outcomes *prometheus.CounterVec
}

// NewRepairMetrics creates the protocol-repair collectors and registers them on reg.
func NewRepairMetrics(reg prometheus.Registerer) *RepairMetrics {
	m := &RepairMetrics{
		UnitsChecked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_repair_units_checked_total",
			Help: "Repair units entering the fetch/apply loop, per protocol.",
		}, []string{"protocol_id"}),
		Outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_repair_unit_outcomes_total",
			Help: "Terminal per-unit repair outcomes per protocol: applied, unchanged, skipped (truth unavailable), gave_up (hot row).",
		}, []string{"protocol_id", "outcome"}),
	}
	reg.MustRegister(
		m.UnitsChecked,
		m.Outcomes,
	)
	return m
}
