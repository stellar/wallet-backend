package metrics

import "github.com/prometheus/client_golang/prometheus"

// BlendPriceMetrics holds Prometheus collectors for the Blend v2 oracle price
// snapshot service. Unlike Migration/Ingestion, this runs on its own ticker
// independent of ledger ingestion (see services/blend.PriceSnapshotService),
// so it carries no ledger-sequence-shaped metrics of its own.
type BlendPriceMetrics struct {
	// SnapshotDuration observes one full SnapshotOnce pass's wall time
	// (target discovery + every oracle/Comet fetch + the final batch upsert).
	// PromQL: histogram_quantile(0.99, rate(wallet_blend_price_snapshot_duration_seconds_bucket[$__rate_interval]))
	SnapshotDuration prometheus.Histogram
	// FetchesTotal counts per-(oracle,asset) lastprice fetch outcomes (and the
	// Comet leg's single derived-valuation outcome), labeled by result:
	// success, error, none (SEP-40 Option::None — no price recorded yet),
	// stale (oracle-reported timestamp older than the pool contract's 24h
	// rejection window), or invalid (price ≤ 0).
	// PromQL: rate(wallet_blend_price_fetches_total{result="error"}[$__rate_interval])
	FetchesTotal *prometheus.CounterVec
	// PricesTracked is the row count written by the most recent snapshot pass.
	// PromQL: wallet_blend_prices_tracked
	PricesTracked prometheus.Gauge
	// OldestPriceAge is the age in seconds of the oldest oracle-reported price
	// observed by the most recent snapshot pass, including prices skipped as
	// stale — a dead oracle shows up as this gauge growing without bound.
	// PromQL: wallet_blend_price_oldest_age_seconds
	OldestPriceAge prometheus.Gauge
}

func newBlendPriceMetrics(reg prometheus.Registerer) *BlendPriceMetrics {
	m := &BlendPriceMetrics{
		SnapshotDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_blend_price_snapshot_duration_seconds",
			Help:    "Duration of a full Blend v2 oracle price snapshot pass.",
			Buckets: prometheus.DefBuckets,
		}),
		FetchesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_blend_price_fetches_total",
			Help: "Blend v2 oracle price fetch outcomes, labeled by result (success, error, none, stale, invalid).",
		}, []string{"result"}),
		PricesTracked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_blend_prices_tracked",
			Help: "Number of (oracle, asset) price rows written by the most recent Blend v2 snapshot pass.",
		}),
		OldestPriceAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_blend_price_oldest_age_seconds",
			Help: "Age in seconds of the oldest oracle-reported price observed by the most recent Blend v2 snapshot pass, including prices skipped as stale.",
		}),
	}
	reg.MustRegister(m.SnapshotDuration, m.FetchesTotal, m.PricesTracked, m.OldestPriceAge)
	return m
}
