package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	require.NotNil(t, m.QueryDuration)
	require.NotNil(t, m.QueriesTotal)
	require.NotNil(t, m.QueryErrors)
	require.NotNil(t, m.TransactionsTotal)
	require.NotNil(t, m.TransactionDuration)
	require.NotNil(t, m.BatchSize)
}

func TestDBMetrics_QueriesTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	tests := []struct {
		queryType, table string
		count            int
	}{
		{"select", "accounts", 3},
		{"insert", "transactions", 1},
	}
	for _, tt := range tests {
		for range tt.count {
			m.QueriesTotal.WithLabelValues(tt.queryType, tt.table).Inc()
		}
		assert.Equal(t, float64(tt.count), testutil.ToFloat64(m.QueriesTotal.WithLabelValues(tt.queryType, tt.table)))
	}
}

func TestDBMetrics_QueryErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	m.QueryErrors.WithLabelValues("insert", "accounts", "constraint_violation").Inc()
	m.QueryErrors.WithLabelValues("select", "balances", "timeout").Add(2)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.QueryErrors.WithLabelValues("insert", "accounts", "constraint_violation")))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.QueryErrors.WithLabelValues("select", "balances", "timeout")))
}

func TestDBMetrics_TransactionsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	m.TransactionsTotal.WithLabelValues("committed").Add(5)
	m.TransactionsTotal.WithLabelValues("rolled_back").Add(2)

	assert.Equal(t, 5.0, testutil.ToFloat64(m.TransactionsTotal.WithLabelValues("committed")))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.TransactionsTotal.WithLabelValues("rolled_back")))
}

func TestDBMetrics_Durations(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	m.QueryDuration.WithLabelValues("select", "accounts").Observe(0.05)
	m.QueryDuration.WithLabelValues("insert", "balances").Observe(0.1)
	m.TransactionDuration.WithLabelValues("committed").Observe(0.1)

	assert.Equal(t, 2, testutil.CollectAndCount(m.QueryDuration))
	assert.Equal(t, 1, testutil.CollectAndCount(m.TransactionDuration))
}

func TestDBMetrics_BatchSize_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	m.BatchSize.WithLabelValues("upsert", "balances").Observe(100)
	m.BatchSize.WithLabelValues("insert", "accounts").Observe(50)

	// Two label combinations = 2 metric series.
	assert.Equal(t, 2, testutil.CollectAndCount(m.BatchSize))

	// To verify histogram bucket configuration, we use reg.Gather() which returns the
	// full protobuf representation. CollectAndCount can't help here — it only counts
	// distinct label combos, not internal bucket structure. GetBucket() returns the
	// explicit boundaries only; the +Inf bucket is implicit in the proto and excluded.
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_db_batch_operation_size" {
			for _, metric := range f.GetMetric() {
				h := metric.GetHistogram()
				require.NotNil(t, h)
				assert.Len(t, h.GetBucket(), 12) // ExponentialBuckets(1, 2, 12)
			}
		}
	}
}

func TestDBMetrics_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	m.QueriesTotal.WithLabelValues("select", "accounts").Add(7)

	expected := strings.NewReader(`
		# HELP wallet_db_queries_total Total number of database queries.
		# TYPE wallet_db_queries_total counter
		wallet_db_queries_total{query_type="select",table="accounts"} 7
	`)
	err := testutil.CollectAndCompare(m.QueriesTotal, expected)
	require.NoError(t, err)
}

func TestDBMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDBMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.QueryDuration, m.QueriesTotal, m.QueryErrors,
		m.TransactionsTotal, m.TransactionDuration, m.BatchSize,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
