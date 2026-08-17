package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlendPriceMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newBlendPriceMetrics(reg)

	require.NotNil(t, m.SnapshotDuration)
	require.NotNil(t, m.FetchesTotal)
	require.NotNil(t, m.PricesTracked)
	require.NotNil(t, m.OldestPriceAge)
}

func TestBlendPriceMetrics_Record(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newBlendPriceMetrics(reg)

	m.SnapshotDuration.Observe(0.25)
	m.FetchesTotal.WithLabelValues("success").Inc()
	m.FetchesTotal.WithLabelValues("success").Inc()
	m.FetchesTotal.WithLabelValues("error").Inc()
	m.FetchesTotal.WithLabelValues("none").Inc()
	m.PricesTracked.Set(2)
	m.OldestPriceAge.Set(90000)

	assert.Equal(t, 2.0, testutil.ToFloat64(m.FetchesTotal.WithLabelValues("success")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.FetchesTotal.WithLabelValues("error")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.FetchesTotal.WithLabelValues("none")))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.PricesTracked))
	assert.Equal(t, 90000.0, testutil.ToFloat64(m.OldestPriceAge))
}

func TestBlendPriceMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newBlendPriceMetrics(reg)

	for _, c := range []prometheus.Collector{m.SnapshotDuration, m.FetchesTotal, m.PricesTracked, m.OldestPriceAge} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
