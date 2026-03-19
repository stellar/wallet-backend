package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newHTTPMetrics(reg)

	require.NotNil(t, m.RequestsTotal)
	require.NotNil(t, m.RequestsDuration)
}

func TestHTTPMetrics_RequestsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newHTTPMetrics(reg)

	m.RequestsTotal.WithLabelValues("/health", "GET", "200").Inc()
	m.RequestsTotal.WithLabelValues("/health", "GET", "200").Inc()
	m.RequestsTotal.WithLabelValues("/graphql", "POST", "500").Inc()

	assert.Equal(t, 2.0, testutil.ToFloat64(m.RequestsTotal.WithLabelValues("/health", "GET", "200")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RequestsTotal.WithLabelValues("/graphql", "POST", "500")))
}

func TestHTTPMetrics_RequestsDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newHTTPMetrics(reg)

	m.RequestsDuration.WithLabelValues("/health", "GET").Observe(0.15)
	m.RequestsDuration.WithLabelValues("/graphql", "POST").Observe(0.25)

	// CollectAndCount counts distinct label-value combinations (metric series), NOT
	// internal structure (quantiles, buckets, _sum, _count). A SummaryVec with two
	// observed label combos returns 2, regardless of how many quantiles it tracks.
	assert.Equal(t, 2, testutil.CollectAndCount(m.RequestsDuration))
}

func TestHTTPMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newHTTPMetrics(reg)

	for _, c := range []prometheus.Collector{m.RequestsTotal, m.RequestsDuration} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
