package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphQLMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newGraphQLMetrics(reg)

	require.NotNil(t, m.FieldDuration)
	require.NotNil(t, m.FieldsTotal)
	require.NotNil(t, m.Complexity)
	require.NotNil(t, m.ErrorsTotal)
}

func TestGraphQLMetrics_FieldsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newGraphQLMetrics(reg)

	tests := []struct {
		op, field, success string
		count              int
		expected           float64
	}{
		{"GetAccount", "balance", "true", 3, 3.0},
		{"GetAccount", "balance", "false", 1, 1.0},
	}
	for _, tt := range tests {
		for range tt.count {
			m.FieldsTotal.WithLabelValues(tt.op, tt.field, tt.success).Inc()
		}
		assert.Equal(t, tt.expected, testutil.ToFloat64(m.FieldsTotal.WithLabelValues(tt.op, tt.field, tt.success)))
	}
}

func TestGraphQLMetrics_ErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newGraphQLMetrics(reg)

	m.ErrorsTotal.WithLabelValues("MyQuery", "validation").Inc()
	m.ErrorsTotal.WithLabelValues("MyQuery", "internal").Add(5)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("MyQuery", "validation")))
	assert.Equal(t, 5.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("MyQuery", "internal")))
}

func TestGraphQLMetrics_Durations(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newGraphQLMetrics(reg)

	m.FieldDuration.WithLabelValues("Op", "field1").Observe(0.5)
	m.FieldDuration.WithLabelValues("Op", "field2").Observe(0.3)
	m.Complexity.WithLabelValues("Op").Observe(42)

	// CollectAndCount counts distinct label-value combinations (metric series), NOT
	// internal structure (quantiles, _sum, _count). Two observed label combos = 2.
	assert.Equal(t, 2, testutil.CollectAndCount(m.FieldDuration))
	assert.Equal(t, 1, testutil.CollectAndCount(m.Complexity))
}

func TestGraphQLMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newGraphQLMetrics(reg)

	for _, c := range []prometheus.Collector{m.FieldDuration, m.FieldsTotal, m.Complexity, m.ErrorsTotal} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
