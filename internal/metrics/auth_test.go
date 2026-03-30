package metrics

import (
	"strings"
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

// TestAuthMetrics_GoldenExposition validates name, help, type, and value in full
// Prometheus exposition format. Catches regressions if any of these change.
func TestAuthMetrics_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAuthMetrics(reg)

	m.ExpiredSignaturesTotal.Inc()

	expected := strings.NewReader(`
		# HELP wallet_auth_expired_signatures_total Total number of signature verifications that failed due to expiration.
		# TYPE wallet_auth_expired_signatures_total counter
		wallet_auth_expired_signatures_total 1
	`)
	err := testutil.CollectAndCompare(m.ExpiredSignaturesTotal, expected)
	require.NoError(t, err)
}

func TestAuthMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAuthMetrics(reg)

	problems, err := testutil.CollectAndLint(m.ExpiredSignaturesTotal)
	require.NoError(t, err)
	assert.Empty(t, problems)
}
