package metrics

import "github.com/prometheus/client_golang/prometheus"

// DataMigrationMetrics holds Prometheus collectors for data-migrate runs.
type DataMigrationMetrics struct {
	// Cursor is the lowest ledger a data migration has rewritten; it walks down to the
	// oldest ingested ledger.
	// PromQL: wallet_data_migration_cursor
	Cursor *prometheus.GaugeVec
	// ErrorsTotal counts failed slice attempts (each slice is tried a fixed number of times).
	// PromQL: increase(wallet_data_migration_errors_total[1h]) > 0
	ErrorsTotal *prometheus.CounterVec
}

func newDataMigrationMetrics(reg prometheus.Registerer) *DataMigrationMetrics {
	m := &DataMigrationMetrics{
		Cursor: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wallet_data_migration_cursor",
			Help: "Lowest ledger a data migration has rewritten; walks down to the oldest ingested ledger.",
		}, []string{"name"}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_data_migration_errors_total",
			Help: "Failed slice attempts of a data migration.",
		}, []string{"name"}),
	}
	reg.MustRegister(m.Cursor, m.ErrorsTotal)
	return m
}
