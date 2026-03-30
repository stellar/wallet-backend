package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	require.NotNil(t, m.RequestsTotal)
	require.NotNil(t, m.RequestsDuration)
	require.NotNil(t, m.EndpointFailures)
	require.NotNil(t, m.EndpointSuccesses)
	require.NotNil(t, m.ServiceHealth)
	require.NotNil(t, m.LatestLedger)
	require.NotNil(t, m.MethodCallsTotal)
	require.NotNil(t, m.MethodDuration)
	require.NotNil(t, m.MethodErrorsTotal)
}

func TestRPCMetrics_TransportCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	tests := []struct {
		name     string
		inc      func()
		get      func() float64
		expected float64
	}{
		{
			name:     "RequestsTotal",
			inc:      func() { m.RequestsTotal.WithLabelValues("https://rpc.stellar.org").Inc() },
			get:      func() float64 { return testutil.ToFloat64(m.RequestsTotal.WithLabelValues("https://rpc.stellar.org")) },
			expected: 1.0,
		},
		{
			name: "EndpointFailures",
			inc: func() {
				m.EndpointFailures.WithLabelValues("https://rpc.stellar.org").Add(3)
			},
			get: func() float64 {
				return testutil.ToFloat64(m.EndpointFailures.WithLabelValues("https://rpc.stellar.org"))
			},
			expected: 3.0,
		},
		{
			name: "EndpointSuccesses",
			inc: func() {
				m.EndpointSuccesses.WithLabelValues("https://rpc.stellar.org").Add(5)
			},
			get: func() float64 {
				return testutil.ToFloat64(m.EndpointSuccesses.WithLabelValues("https://rpc.stellar.org"))
			},
			expected: 5.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.inc()
			assert.Equal(t, tt.expected, tt.get())
		})
	}
}

func TestRPCMetrics_Gauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	m.ServiceHealth.Set(1)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.ServiceHealth))

	m.ServiceHealth.Set(0)
	assert.Equal(t, 0.0, testutil.ToFloat64(m.ServiceHealth))

	m.LatestLedger.Set(12345)
	assert.Equal(t, 12345.0, testutil.ToFloat64(m.LatestLedger))
}

func TestRPCMetrics_MethodMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	m.MethodCallsTotal.WithLabelValues("getTransaction").Inc()
	m.MethodErrorsTotal.WithLabelValues("getTransaction", "not_found").Inc()
	m.MethodDuration.WithLabelValues("getTransaction").Observe(0.25)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.MethodCallsTotal.WithLabelValues("getTransaction")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.MethodErrorsTotal.WithLabelValues("getTransaction", "not_found")))
	// CollectAndCount counts label combos, not quantiles — one method observed = 1.
	assert.Equal(t, 1, testutil.CollectAndCount(m.MethodDuration))
}

func TestRPCMetrics_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	m.MethodCallsTotal.WithLabelValues("getTransaction").Add(10)

	expected := strings.NewReader(`
		# HELP wallet_rpc_method_calls_total Total number of RPC method calls at the application level.
		# TYPE wallet_rpc_method_calls_total counter
		wallet_rpc_method_calls_total{method="getTransaction"} 10
	`)
	err := testutil.CollectAndCompare(m.MethodCallsTotal, expected)
	require.NoError(t, err)
}

func TestRPCMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.RequestsTotal, m.RequestsDuration, m.EndpointFailures,
		m.EndpointSuccesses, m.ServiceHealth, m.LatestLedger,
		m.MethodCallsTotal, m.MethodDuration, m.MethodErrorsTotal,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
