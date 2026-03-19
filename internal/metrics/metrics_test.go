package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
