package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAuthMetrics(reg)

	require.NotNil(t, m.ExpiredSignaturesTotal)
}

func TestAuthMetrics_ExpiredSignaturesTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAuthMetrics(reg)

	m.ExpiredSignaturesTotal.Inc()
	m.ExpiredSignaturesTotal.Inc()

	assert.Equal(t, 2.0, testutil.ToFloat64(m.ExpiredSignaturesTotal))
}

func TestAuthMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAuthMetrics(reg)

	problems, err := testutil.CollectAndLint(m.ExpiredSignaturesTotal)
	require.NoError(t, err)
	assert.Empty(t, problems)
}
