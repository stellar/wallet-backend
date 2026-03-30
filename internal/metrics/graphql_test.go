package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphQLMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	require.NotNil(t, m.OperationDuration)
	require.NotNil(t, m.OperationsTotal)
	require.NotNil(t, m.InFlightOperations)
	require.NotNil(t, m.ResponseSize)
	require.NotNil(t, m.Complexity)
	require.NotNil(t, m.ErrorsTotal)
	require.NotNil(t, m.DeprecatedFieldsTotal)
}

func TestGraphQLMetrics_OperationsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	tests := []struct {
		op, opType, status string
		count              int
		expected           float64
	}{
		{"GetAccount", "query", "success", 3, 3.0},
		{"GetAccount", "query", "error", 1, 1.0},
		{"SubmitTransaction", "mutation", "success", 2, 2.0},
	}
	for _, tt := range tests {
		for range tt.count {
			m.OperationsTotal.WithLabelValues(tt.op, tt.opType, tt.status).Inc()
		}
		assert.Equal(t, tt.expected, testutil.ToFloat64(m.OperationsTotal.WithLabelValues(tt.op, tt.opType, tt.status)))
	}
}

func TestGraphQLMetrics_InFlightOperations(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	m.InFlightOperations.Inc()
	assert.Equal(t, 1.0, testutil.ToFloat64(m.InFlightOperations))

	m.InFlightOperations.Inc()
	assert.Equal(t, 2.0, testutil.ToFloat64(m.InFlightOperations))

	m.InFlightOperations.Dec()
	assert.Equal(t, 1.0, testutil.ToFloat64(m.InFlightOperations))
}

func TestGraphQLMetrics_ErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	m.ErrorsTotal.WithLabelValues("MyQuery", "validation_error").Inc()
	m.ErrorsTotal.WithLabelValues("MyQuery", "internal_error").Add(5)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("MyQuery", "validation_error")))
	assert.Equal(t, 5.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("MyQuery", "internal_error")))
}

func TestGraphQLMetrics_HistogramBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	// Observe one value in each histogram to make it gatherable.
	m.OperationDuration.WithLabelValues("Op", "query").Observe(0.1)
	m.Complexity.WithLabelValues("Op").Observe(42)
	m.ResponseSize.WithLabelValues("Op", "query").Observe(1024)

	families, err := reg.Gather()
	require.NoError(t, err)

	bucketCounts := map[string]int{
		"wallet_graphql_operation_duration_seconds": 11,
		"wallet_graphql_complexity":                 11,
		"wallet_graphql_response_size_bytes":        8,
	}

	for _, f := range families {
		expected, ok := bucketCounts[f.GetName()]
		if !ok {
			continue
		}
		for _, metric := range f.GetMetric() {
			h := metric.GetHistogram()
			require.NotNil(t, h, "histogram nil for %s", f.GetName())
			assert.Len(t, h.GetBucket(), expected, "bucket count mismatch for %s", f.GetName())
		}
	}
}

func TestGraphQLMetrics_GoldenExposition(t *testing.T) {
	t.Run("ErrorsTotal", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewGraphQLMetrics(reg)

		m.ErrorsTotal.WithLabelValues("GetAccount", "validation_error").Add(2)

		expected := strings.NewReader(`
		# HELP wallet_graphql_errors_total Total number of GraphQL errors.
		# TYPE wallet_graphql_errors_total counter
		wallet_graphql_errors_total{error_type="validation_error",operation_name="GetAccount"} 2
	`)
		err := testutil.CollectAndCompare(m.ErrorsTotal, expected)
		require.NoError(t, err)
	})

	t.Run("OperationsTotal", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewGraphQLMetrics(reg)

		m.OperationsTotal.WithLabelValues("GetAccount", "query", "success").Add(5)

		expected := strings.NewReader(`
		# HELP wallet_graphql_operations_total Total number of GraphQL operations processed.
		# TYPE wallet_graphql_operations_total counter
		wallet_graphql_operations_total{operation_name="GetAccount",operation_type="query",status="success"} 5
	`)
		err := testutil.CollectAndCompare(m.OperationsTotal, expected)
		require.NoError(t, err)
	})
}

func TestGraphQLMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGraphQLMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.OperationDuration, m.OperationsTotal, m.InFlightOperations,
		m.ResponseSize, m.Complexity, m.ErrorsTotal, m.DeprecatedFieldsTotal,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
