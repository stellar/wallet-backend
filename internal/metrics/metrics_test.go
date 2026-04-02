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

	t.Run("protocol contract cache access", func(t *testing.T) {
		m.Ingestion.ProtocolContractCacheAccess.WithLabelValues("SEP41", "hit").Inc()
		m.Ingestion.ProtocolContractCacheAccess.WithLabelValues("SEP41", "miss").Inc()
		m.Ingestion.ProtocolContractCacheAccess.WithLabelValues("BLEND", "hit").Inc()

		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		found := false
		values := make(map[string]float64)
		for _, mf := range metricFamilies {
			if mf.GetName() == "wallet_ingestion_protocol_contract_cache_access_total" {
				found = true
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					key := labels["protocol_id"] + "|" + labels["result"]
					values[key] = metric.GetCounter().GetValue()
				}
			}
		}

		assert.True(t, found, "wallet_ingestion_protocol_contract_cache_access_total metric not found")
		assert.Equal(t, float64(1), values["SEP41|hit"])
		assert.Equal(t, float64(1), values["SEP41|miss"])
		assert.Equal(t, float64(1), values["BLEND|hit"])
	})

	t.Run("protocol contract cache refresh duration", func(t *testing.T) {
		m.Ingestion.ProtocolContractCacheRefresh.Observe(0.02)
		m.Ingestion.ProtocolContractCacheRefresh.Observe(0.08)

		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "wallet_ingestion_protocol_contract_cache_refresh_duration_seconds" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, uint64(2), metric.GetHistogram().GetSampleCount())
				assert.InDelta(t, 0.10, metric.GetHistogram().GetSampleSum(), 0.001)
			}
		}

		assert.True(t, found, "wallet_ingestion_protocol_contract_cache_refresh_duration_seconds metric not found")
	})
}
