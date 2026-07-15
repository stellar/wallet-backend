package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	require.NotNil(t, m.DB)
	require.NotNil(t, m.RPC)
	require.NotNil(t, m.Ingestion)
	require.NotNil(t, m.HTTP)
	require.NotNil(t, m.GraphQL)
	require.NotNil(t, m.Auth)
	require.NotNil(t, m.Migration)
	require.NotNil(t, m.Dataloader)

	assert.Same(t, reg, m.Registry())
}

// TestNewMetrics_CrossSubstructExposition verifies that metrics from different
// sub-structs coexist on the same registry without collision.
func TestNewMetrics_CrossSubstructExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.Auth.ExpiredSignaturesTotal.Inc()
	m.HTTP.RequestsTotal.WithLabelValues("/health", "GET", "200").Inc()

	expected := strings.NewReader(`
		# HELP wallet_auth_expired_signatures_total Total number of signature verifications that failed due to expiration.
		# TYPE wallet_auth_expired_signatures_total counter
		wallet_auth_expired_signatures_total 1
	`)
	err := testutil.CollectAndCompare(m.Auth.ExpiredSignaturesTotal, expected)
	require.NoError(t, err)
}

func TestNewMetrics_IndependentRegistries(t *testing.T) {
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	// Two separate Metrics instances with independent registries must not panic.
	m1 := NewMetrics(reg1)
	m2 := NewMetrics(reg2)

	require.NotNil(t, m1)
	require.NotNil(t, m2)
	assert.NotSame(t, m1.Registry(), m2.Registry())
}

func TestProtocolStateMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	t.Run("protocol state processing duration", func(t *testing.T) {
		m.Ingestion.ProtocolStateProcessingDuration.WithLabelValues("SEP41", "process_ledger").Observe(0.05)
		m.Ingestion.ProtocolStateProcessingDuration.WithLabelValues("SEP41", "persist_history").Observe(0.10)
		m.Ingestion.ProtocolStateProcessingDuration.WithLabelValues("BLEND", "persist_current_state").Observe(0.03)

		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		found := false
		phaseCounts := make(map[string]uint64)
		for _, mf := range metricFamilies {
			if mf.GetName() == "wallet_ingestion_protocol_state_processing_duration_seconds" {
				found = true
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["protocol_id"] == "SEP41" {
						phaseCounts[labels["phase"]] = metric.GetHistogram().GetSampleCount()
					}
				}
			}
		}

		assert.True(t, found, "wallet_ingestion_protocol_state_processing_duration_seconds metric not found")
		assert.Equal(t, uint64(1), phaseCounts["process_ledger"])
		assert.Equal(t, uint64(1), phaseCounts["persist_history"])
	})
}

// TestNewMetrics_GoRuntimeCollectors verifies the registry exposes Go runtime
// metrics (goroutines, heap, GC). Without them, GC pressure and memory growth
// in the serve process are invisible to Prometheus/Grafana.
func TestNewMetrics_GoRuntimeCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	assert.True(t, names["go_goroutines"], "expected go_goroutines on the registry")
	assert.True(t, names["go_memstats_heap_inuse_bytes"], "expected go_memstats_heap_inuse_bytes on the registry")
}
