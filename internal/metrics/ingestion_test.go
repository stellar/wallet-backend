package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestionMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	require.NotNil(t, m.LatestLedger)
	require.NotNil(t, m.OldestLedger)
	require.NotNil(t, m.Duration)
	require.NotNil(t, m.PhaseDuration)
	require.NotNil(t, m.LedgersProcessed)
	require.NotNil(t, m.TransactionsTotal)
	require.NotNil(t, m.OperationsTotal)
	require.NotNil(t, m.BatchSize)
	require.NotNil(t, m.ParticipantsCount)
	require.NotNil(t, m.StateChangeProcessingDuration)
	require.NotNil(t, m.StateChangesTotal)
}

func TestIngestionMetrics_Gauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.LatestLedger.Set(100000)
	m.OldestLedger.Set(50000)

	assert.Equal(t, 100000.0, testutil.ToFloat64(m.LatestLedger))
	assert.Equal(t, 50000.0, testutil.ToFloat64(m.OldestLedger))
}

func TestIngestionMetrics_Counters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.LedgersProcessed.Add(10)
	m.TransactionsTotal.Add(100)
	m.OperationsTotal.Add(250)

	assert.Equal(t, 10.0, testutil.ToFloat64(m.LedgersProcessed))
	assert.Equal(t, 100.0, testutil.ToFloat64(m.TransactionsTotal))
	assert.Equal(t, 250.0, testutil.ToFloat64(m.OperationsTotal))
}

func TestIngestionMetrics_Duration_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.Duration.WithLabelValues().Observe(0.5)

	// To verify bucket configuration we use reg.Gather() → protobuf inspection.
	// CollectAndCount only counts distinct label combos, not internal histogram structure.
	// GetBucket() returns explicit boundaries only; +Inf is implicit and excluded.
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_duration_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 20) // 20 custom boundaries
		}
	}
}

func TestIngestionMetrics_PhaseDuration_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.PhaseDuration.WithLabelValues("fetch").Observe(0.1)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_phase_duration_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 17) // 17 custom boundaries
		}
	}
}

func TestIngestionMetrics_BatchSize_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BatchSize.Observe(50)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_batch_size" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 8) // ExponentialBuckets(1, 2, 8)
		}
	}
}

func TestIngestionMetrics_ParticipantsCount_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.ParticipantsCount.Observe(100)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_participants_count" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 12) // ExponentialBuckets(1, 2, 12)
		}
	}
}

func TestIngestionMetrics_StateChanges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.StateChangesTotal.WithLabelValues("balance", "created").Add(5)
	m.StateChangesTotal.WithLabelValues("balance", "updated").Add(3)
	m.StateChangeProcessingDuration.WithLabelValues("balance_processor").Observe(0.01)

	assert.Equal(t, 5.0, testutil.ToFloat64(m.StateChangesTotal.WithLabelValues("balance", "created")))
	assert.Equal(t, 3.0, testutil.ToFloat64(m.StateChangesTotal.WithLabelValues("balance", "updated")))
}

func TestIngestionMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.LatestLedger, m.OldestLedger, m.Duration, m.PhaseDuration,
		m.LedgersProcessed, m.TransactionsTotal, m.OperationsTotal,
		m.BatchSize, m.ParticipantsCount,
		m.StateChangeProcessingDuration, m.StateChangesTotal,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
