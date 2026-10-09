package metrics

import "github.com/prometheus/client_golang/prometheus"

// PricesMetrics holds Prometheus collectors for token pricing.
type PricesMetrics struct {
	// TradesTotal counts persisted fills by venue (sdex_orderbook, sdex_pool, soroswap, aquarius).
	// PromQL: sum by (venue) (rate(wallet_prices_trades_total[5m]))
	TradesTotal *prometheus.CounterVec
	// TradesUnpricedTotal counts persisted fills that carry no USD value.
	// PromQL: rate(wallet_prices_trades_unpriced_total[5m]) / sum(rate(wallet_prices_trades_total[5m]))
	TradesUnpricedTotal prometheus.Counter
	// OracleFetchesTotal counts oracle readings by result (success, error, none, stale, invalid).
	// PromQL: sum by (result) (rate(wallet_prices_oracle_fetches_total[15m]))
	OracleFetchesTotal *prometheus.CounterVec
	// AnchorAgeSeconds is the age of the oracle reading behind the anchor rates, set whenever the anchor is set.
	// PromQL: wallet_prices_anchor_age_seconds > 86400
	AnchorAgeSeconds prometheus.Gauge
	// SnapshotReloadDuration observes how long a price snapshot reload takes.
	// PromQL: histogram_quantile(0.99, rate(wallet_prices_snapshot_reload_duration_seconds_bucket[5m]))
	SnapshotReloadDuration prometheus.Histogram
	// SnapshotTokens is the number of tokens in the current price snapshot.
	// PromQL: wallet_prices_snapshot_tokens
	SnapshotTokens prometheus.Gauge
	// SnapshotReloadErrorsTotal counts failed snapshot reloads.
	// PromQL: increase(wallet_prices_snapshot_reload_errors_total[15m]) > 0
	SnapshotReloadErrorsTotal prometheus.Counter
	// ComparisonSamplesTotal counts comparison samples by result (both, external_missing, external_error).
	// PromQL: sum by (result) (rate(wallet_prices_comparison_samples_total[1h]))
	ComparisonSamplesTotal *prometheus.CounterVec
}

func newPricesMetrics(reg prometheus.Registerer) *PricesMetrics {
	m := &PricesMetrics{
		TradesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_prices_trades_total",
			Help: "Total fills persisted, by venue.",
		}, []string{"venue"}),
		TradesUnpricedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_prices_trades_unpriced_total",
			Help: "Total fills persisted without a USD value.",
		}),
		OracleFetchesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_prices_oracle_fetches_total",
			Help: "Total oracle readings, by result.",
		}, []string{"result"}),
		AnchorAgeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_prices_anchor_age_seconds",
			Help: "Age in seconds of the oracle reading behind the anchor rates when they were last set.",
		}),
		SnapshotReloadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_prices_snapshot_reload_duration_seconds",
			Help:    "Duration of a price snapshot reload.",
			Buckets: prometheus.DefBuckets,
		}),
		SnapshotTokens: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_prices_snapshot_tokens",
			Help: "Number of tokens in the current price snapshot.",
		}),
		SnapshotReloadErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_prices_snapshot_reload_errors_total",
			Help: "Total failed price snapshot reloads.",
		}),
		ComparisonSamplesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_prices_comparison_samples_total",
			Help: "Total price comparison samples, by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(
		m.TradesTotal,
		m.TradesUnpricedTotal,
		m.OracleFetchesTotal,
		m.AnchorAgeSeconds,
		m.SnapshotReloadDuration,
		m.SnapshotTokens,
		m.SnapshotReloadErrorsTotal,
		m.ComparisonSamplesTotal,
	)
	return m
}
