package metrics

import (
	"strings"
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
	require.NotNil(t, m.ParticipantsCount)
	require.NotNil(t, m.LagLedgers)
	require.NotNil(t, m.LedgerFetchDuration)
	require.NotNil(t, m.RetriesTotal)
	require.NotNil(t, m.RetryExhaustionsTotal)
	require.NotNil(t, m.ErrorsTotal)
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

	m.Duration.Observe(0.5)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_duration_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 13) // 13 custom boundaries
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
			assert.Len(t, h.GetBucket(), 13) // 13 custom boundaries
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
		if f.GetName() == "wallet_ingestion_participants_per_ledger" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 15) // ExponentialBuckets(1, 2, 15)
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

func TestIngestionMetrics_GoldenExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.StateChangesTotal.WithLabelValues("balance", "created").Add(42)
	m.StateChangesTotal.WithLabelValues("balance", "removed").Add(3)

	expected := strings.NewReader(`
		# HELP wallet_ingestion_state_changes_total Total number of state changes persisted to database by type and category.
		# TYPE wallet_ingestion_state_changes_total counter
		wallet_ingestion_state_changes_total{category="created",type="balance"} 42
		wallet_ingestion_state_changes_total{category="removed",type="balance"} 3
	`)
	err := testutil.CollectAndCompare(m.StateChangesTotal, expected)
	require.NoError(t, err)
}

func TestIngestionMetrics_LagLedgers(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.LagLedgers.Set(42)
	assert.Equal(t, 42.0, testutil.ToFloat64(m.LagLedgers))
}

func TestIngestionMetrics_LedgerFetchDuration_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.LedgerFetchDuration.Observe(0.5)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_ledger_fetch_duration_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 10) // 10 custom boundaries
		}
	}
}

func TestIngestionMetrics_RetriesAndExhaustions(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.RetriesTotal.WithLabelValues("ledger_fetch").Inc()
	m.RetriesTotal.WithLabelValues("ledger_fetch").Inc()
	m.RetriesTotal.WithLabelValues("db_persist").Inc()
	m.RetryExhaustionsTotal.WithLabelValues("ledger_fetch").Inc()

	assert.Equal(t, 2.0, testutil.ToFloat64(m.RetriesTotal.WithLabelValues("ledger_fetch")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RetriesTotal.WithLabelValues("db_persist")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RetryExhaustionsTotal.WithLabelValues("ledger_fetch")))
	assert.Equal(t, 0.0, testutil.ToFloat64(m.RetryExhaustionsTotal.WithLabelValues("db_persist")))
}

func TestIngestionMetrics_ErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.ErrorsTotal.WithLabelValues("ledger_fetch").Inc()
	m.ErrorsTotal.WithLabelValues("ingest_live").Inc()
	m.ErrorsTotal.WithLabelValues("ingest_live").Inc()

	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("ledger_fetch")))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("ingest_live")))
}

func TestIngestionMetrics_BackfillRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	require.NotNil(t, m.BackfillChannelWait)
	require.NotNil(t, m.BackfillChannelUtilization)
	require.NotNil(t, m.BackfillLedgersFlushed)
	require.NotNil(t, m.BackfillBatchSize)
	require.NotNil(t, m.BackfillGapProgress)
	require.NotNil(t, m.BackfillGapStartLedger)
	require.NotNil(t, m.BackfillGapEndLedger)
}

func TestIngestionMetrics_BackfillChannelWait_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(0.1)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_backfill_channel_wait_seconds" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 10)
		}
	}
}

func TestIngestionMetrics_BackfillBatchSize_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	m.BackfillBatchSize.Observe(50)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "wallet_ingestion_backfill_batch_size" {
			h := f.GetMetric()[0].GetHistogram()
			assert.Len(t, h.GetBucket(), 10) // LinearBuckets(10, 10, 10)
		}
	}
}

func TestIngestionMetrics_BackfillChannelWait_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	assert.NotPanics(t, func() {
		m.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("ledger", "receive").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("flush", "send").Observe(0.1)
		m.BackfillChannelWait.WithLabelValues("flush", "receive").Observe(0.1)
	})
}

func TestIngestionMetrics_Lint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIngestionMetrics(reg)

	for _, c := range []prometheus.Collector{
		m.LatestLedger, m.OldestLedger, m.Duration, m.PhaseDuration,
		m.LedgersProcessed, m.TransactionsTotal, m.OperationsTotal,
		m.ParticipantsCount,
		m.LagLedgers, m.LedgerFetchDuration,
		m.RetriesTotal, m.RetryExhaustionsTotal, m.ErrorsTotal,
		m.StateChangeProcessingDuration, m.StateChangesTotal,
		m.BackfillChannelWait, m.BackfillChannelUtilization,
		m.BackfillLedgersFlushed, m.BackfillBatchSize,
		m.BackfillGapProgress, m.BackfillGapStartLedger, m.BackfillGapEndLedger,
	} {
		problems, err := testutil.CollectAndLint(c)
		require.NoError(t, err)
		assert.Empty(t, problems)
	}
}
