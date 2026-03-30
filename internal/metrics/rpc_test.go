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

	require.NotNil(t, m.RequestDuration)
	require.NotNil(t, m.RequestsTotal)
	require.NotNil(t, m.InFlightRequests)
	require.NotNil(t, m.ResponseSizeBytes)
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
			name: "RequestsTotal success",
			inc:  func() { m.RequestsTotal.WithLabelValues("getTransaction", "success").Inc() },
			get: func() float64 {
				return testutil.ToFloat64(m.RequestsTotal.WithLabelValues("getTransaction", "success"))
			},
			expected: 1.0,
		},
		{
			name: "RequestsTotal failure",
			inc: func() {
				m.RequestsTotal.WithLabelValues("getTransaction", "failure").Add(3)
			},
			get: func() float64 {
				return testutil.ToFloat64(m.RequestsTotal.WithLabelValues("getTransaction", "failure"))
			},
			expected: 3.0,
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

	m.InFlightRequests.Inc()
	m.InFlightRequests.Inc()
	assert.Equal(t, 2.0, testutil.ToFloat64(m.InFlightRequests))
	m.InFlightRequests.Dec()
	assert.Equal(t, 1.0, testutil.ToFloat64(m.InFlightRequests))
}

func TestRPCMetrics_MethodMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	m.MethodCallsTotal.WithLabelValues("getTransaction").Inc()
	m.MethodErrorsTotal.WithLabelValues("getTransaction", "not_found").Inc()
	m.MethodDuration.WithLabelValues("getTransaction").Observe(0.25)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.MethodCallsTotal.WithLabelValues("getTransaction")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.MethodErrorsTotal.WithLabelValues("getTransaction", "not_found")))
	assert.Equal(t, 1, testutil.CollectAndCount(m.MethodDuration))
}

func TestRPCMetrics_Histograms(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	m.RequestDuration.WithLabelValues("getTransaction").Observe(0.15)
	m.ResponseSizeBytes.WithLabelValues("getTransaction").Observe(1024)
	m.MethodDuration.WithLabelValues("GetTransaction").Observe(0.2)

	assert.Equal(t, 1, testutil.CollectAndCount(m.RequestDuration))
	assert.Equal(t, 1, testutil.CollectAndCount(m.ResponseSizeBytes))
	assert.Equal(t, 1, testutil.CollectAndCount(m.MethodDuration))
}

func TestRPCMetrics_HistogramBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)

	// Trigger a single observation so histogram families are populated.
	m.RequestDuration.WithLabelValues("getHealth").Observe(0.01)
	m.MethodDuration.WithLabelValues("GetHealth").Observe(0.01)
	m.ResponseSizeBytes.WithLabelValues("getHealth").Observe(256)

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, fam := range families {
		switch fam.GetName() {
		case "wallet_rpc_request_duration_seconds", "wallet_rpc_method_duration_seconds":
			// 10 explicit buckets + implicit +Inf = 11
			buckets := fam.GetMetric()[0].GetHistogram().GetBucket()
			assert.Len(t, buckets, len(rpcDurationBuckets), "duration histogram should have %d buckets", len(rpcDurationBuckets))
		case "wallet_rpc_response_size_bytes":
			buckets := fam.GetMetric()[0].GetHistogram().GetBucket()
			assert.Len(t, buckets, 8, "response size histogram should have 8 buckets")
		}
	}
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
		m.RequestDuration, m.RequestsTotal, m.InFlightRequests,
		m.ResponseSizeBytes, m.ServiceHealth, m.LatestLedger,
		m.MethodCallsTotal, m.MethodDuration, m.MethodErrorsTotal,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
