package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataloaderMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDataloaderMetrics(reg)

	require.NotNil(t, m.BatchSize)
	require.NotNil(t, m.FetchDuration)
}

func TestDataloaderMetrics_HistogramBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDataloaderMetrics(reg)

	m.BatchSize.WithLabelValues("OperationsByToIDLoader").Observe(10)
	m.FetchDuration.WithLabelValues("OperationsByToIDLoader").Observe(0.01)

	families, err := reg.Gather()
	require.NoError(t, err)

	bucketCounts := map[string]int{
		"wallet_graphql_dataloader_batch_size":             7,
		"wallet_graphql_dataloader_fetch_duration_seconds": 9,
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

func TestDataloaderMetrics_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDataloaderMetrics(reg)

	m.BatchSize.WithLabelValues("OperationsByToIDLoader").Observe(50)

	expected := strings.NewReader(`
		# HELP wallet_graphql_dataloader_batch_size Number of distinct keys collected into one dataloader batch.
		# TYPE wallet_graphql_dataloader_batch_size histogram
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="1"} 0
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="2"} 0
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="5"} 0
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="10"} 0
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="25"} 0
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="50"} 1
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="100"} 1
		wallet_graphql_dataloader_batch_size_bucket{loader="OperationsByToIDLoader",le="+Inf"} 1
		wallet_graphql_dataloader_batch_size_sum{loader="OperationsByToIDLoader"} 50
		wallet_graphql_dataloader_batch_size_count{loader="OperationsByToIDLoader"} 1
	`)
	err := testutil.CollectAndCompare(m.BatchSize, expected)
	require.NoError(t, err)
}

func TestDataloaderMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newDataloaderMetrics(reg)

	for _, c := range []prometheus.Collector{m.BatchSize, m.FetchDuration} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
