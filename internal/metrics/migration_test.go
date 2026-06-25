package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMigrationMetrics(reg)

	require.NotNil(t, m.CurrentLedger)
	require.NotNil(t, m.LedgersProcessed)
	require.NotNil(t, m.PhaseDuration)
	require.NotNil(t, m.TargetTip)
	require.NotNil(t, m.Cursor)
	require.NotNil(t, m.StartLedger)
	require.NotNil(t, m.Handoffs)
	require.NotNil(t, m.Status)
}

func TestMigrationMetrics_Record(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMigrationMetrics(reg)

	m.LedgersProcessed.Inc()
	m.LedgersProcessed.Inc()
	m.CurrentLedger.Set(59000000)
	m.TargetTip.Set(59010000)
	m.Cursor.WithLabelValues("SEP41").Set(58999000)
	m.StartLedger.WithLabelValues("SEP41").Set(50457424)
	m.Handoffs.WithLabelValues("SEP41").Inc()
	m.Status.WithLabelValues("SEP41").Set(MigrationStatusSuccess)
	m.PhaseDuration.WithLabelValues("fetch").Observe(0.012) // no panic = registered

	assert.Equal(t, 2.0, testutil.ToFloat64(m.LedgersProcessed))
	assert.Equal(t, 59000000.0, testutil.ToFloat64(m.CurrentLedger))
	assert.Equal(t, 59010000.0, testutil.ToFloat64(m.TargetTip))
	assert.Equal(t, 58999000.0, testutil.ToFloat64(m.Cursor.WithLabelValues("SEP41")))
	assert.Equal(t, 50457424.0, testutil.ToFloat64(m.StartLedger.WithLabelValues("SEP41")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Handoffs.WithLabelValues("SEP41")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Status.WithLabelValues("SEP41")))
}

func TestMigrationMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMigrationMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.CurrentLedger, m.LedgersProcessed, m.PhaseDuration, m.TargetTip,
		m.Cursor, m.StartLedger, m.Handoffs, m.Status,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
