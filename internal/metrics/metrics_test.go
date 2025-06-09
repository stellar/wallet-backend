package metrics

import (
	"testing"
	"time"

	"github.com/alitto/pond"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *sqlx.DB {
	// For testing, we can use a mock DB or sqlite in-memory
	db, err := sqlx.Connect("sqlite3", ":memory:")
	require.NoError(t, err)
	return db
}

func TestNewMetricsService(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)
	assert.NotNil(t, ms)
	assert.NotNil(t, ms.GetRegistry())
}

func TestIngestMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("payment ops metrics", func(t *testing.T) {
		ms.SetNumPaymentOpsIngestedPerLedger("create_account", 5)
		ms.SetNumPaymentOpsIngestedPerLedger("payment", 10)

		// We can't directly access the metric values, but we can verify they're collected
		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "num_payment_ops_ingested_per_ledger" {
				found = true
				assert.Equal(t, 2, len(mf.GetMetric()))
			}
		}
		assert.True(t, found)
	})

	t.Run("latest ledger metrics", func(t *testing.T) {
		ms.SetLatestLedgerIngested(1234)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "latest_ledger_ingested" {
				found = true
				assert.Equal(t, 1, len(mf.GetMetric()))
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion duration metrics", func(t *testing.T) {
		ms.ObserveIngestionDuration("payment", 0.5)
		ms.ObserveIngestionDuration("transaction", 1.0)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_duration_seconds" {
				found = true
				assert.Equal(t, 2, len(mf.GetMetric()))
			}
		}
		assert.True(t, found)
	})
}

func TestAccountMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("active accounts counter", func(t *testing.T) {
		// Initial state should be 0
		_, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		// Increment and verify
		ms.IncActiveAccount()
		ms.IncActiveAccount()

		// Decrement and verify
		ms.DecActiveAccount()

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "active_accounts" {
				found = true
				assert.Equal(t, 1, len(mf.GetMetric()))
			}
		}
		assert.True(t, found)
	})
}

func TestRPCMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("RPC request metrics", func(t *testing.T) {
		endpoint := "test_endpoint"

		ms.IncRPCRequests(endpoint)
		ms.ObserveRPCRequestDuration(endpoint, 0.1)
		ms.IncRPCEndpointSuccess(endpoint)
		ms.IncRPCEndpointFailure(endpoint)
		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		foundRequests := false
		foundDuration := false
		foundSuccess := false
		foundFailures := false
		for _, mf := range metricFamilies {
			switch mf.GetName() {
			case "rpc_requests_total":
				foundRequests = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(1), metric.GetCounter().GetValue())
				assert.Equal(t, "test_endpoint", metric.GetLabel()[0].GetValue())
			case "rpc_requests_duration_seconds":
				foundDuration = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
				assert.Equal(t, 0.1, metric.GetSummary().GetSampleSum())
				assert.Equal(t, "test_endpoint", metric.GetLabel()[0].GetValue())
			case "rpc_endpoint_successes_total":
				foundSuccess = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(1), metric.GetCounter().GetValue())
				assert.Equal(t, "test_endpoint", metric.GetLabel()[0].GetValue())
			case "rpc_endpoint_failures_total":
				foundFailures = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(1), metric.GetCounter().GetValue())
				assert.Equal(t, "test_endpoint", metric.GetLabel()[0].GetValue())
			}
		}

		assert.True(t, foundRequests)
		assert.True(t, foundDuration)
		assert.True(t, foundSuccess)
		assert.True(t, foundFailures)
	})

	t.Run("RPC health metrics", func(t *testing.T) {
		ms.SetRPCServiceHealth(true)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_service_health" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(1), metric.GetGauge().GetValue())
			}
		}
		assert.True(t, found)

		ms.SetRPCServiceHealth(false)
		metricFamilies, err = ms.GetRegistry().Gather()
		require.NoError(t, err)

		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_service_health" {
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(0), metric.GetGauge().GetValue())
			}
		}
	})
}

func TestHTTPMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)
	endpoint := "/api/v1/test"
	method := "POST"
	statusCode := 200

	ms.IncNumRequests(endpoint, method, statusCode)
	ms.ObserveRequestDuration(endpoint, method, 0.05)

	metricFamilies, err := ms.GetRegistry().Gather()
	require.NoError(t, err)

	foundRequests := false
	foundDuration := false

	for _, mf := range metricFamilies {
		switch mf.GetName() {
		case "http_requests_total":
			foundRequests = true
			metric := mf.GetMetric()[0]
			assert.Equal(t, float64(1), metric.GetCounter().GetValue())
			// Verify all labels
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, endpoint, labels["endpoint"])
			assert.Equal(t, method, labels["method"])
			assert.Equal(t, "200", labels["status_code"])
		case "http_request_duration_seconds":
			foundDuration = true
			metric := mf.GetMetric()[0]
			assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
			assert.Equal(t, 0.05, metric.GetSummary().GetSampleSum())
			// Verify all labels
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, endpoint, labels["endpoint"])
			assert.Equal(t, method, labels["method"])
		}
	}

	assert.True(t, foundRequests)
	assert.True(t, foundDuration)
}

func TestDBMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)
	queryType := "SELECT"
	table := "accounts"

	ms.IncDBQuery(queryType, table)
	ms.ObserveDBQueryDuration(queryType, table, 0.01)

	metricFamilies, err := ms.GetRegistry().Gather()
	require.NoError(t, err)

	foundQueries := false
	foundDuration := false

	for _, mf := range metricFamilies {
		switch mf.GetName() {
		case "db_queries_total":
			foundQueries = true
			metric := mf.GetMetric()[0]
			assert.Equal(t, float64(1), metric.GetCounter().GetValue())
			// Verify labels
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, queryType, labels["query_type"])
			assert.Equal(t, table, labels["table"])
		case "db_query_duration_seconds":
			foundDuration = true
			metric := mf.GetMetric()[0]
			assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
			assert.Equal(t, 0.01, metric.GetSummary().GetSampleSum())
			// Verify labels
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, queryType, labels["query_type"])
			assert.Equal(t, table, labels["table"])
		}
	}

	assert.True(t, foundQueries, "Query counter metric not found")
	assert.True(t, foundDuration, "Query duration metric not found")
}

func TestPoolMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("worker pool metrics - success case", func(t *testing.T) {
		channel := "test_channel"
		pool := pond.New(5, 10)

		ms.RegisterPoolMetrics(channel, pool)

		// Submit some tasks to the pool
		for i := 0; i < 3; i++ {
			pool.Submit(func() {
				time.Sleep(10 * time.Millisecond)
			})
		}

		// Wait for tasks to complete
		time.Sleep(20 * time.Millisecond)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		// Map to store metric values
		metricValues := make(map[string]float64)

		for _, mf := range metricFamilies {
			metric := mf.GetMetric()[0]
			switch mf.GetName() {
			case "pool_workers_running_" + channel:
				metricValues["workers_running"] = metric.GetGauge().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for workers_running")

			case "pool_workers_idle_" + channel:
				metricValues["workers_idle"] = metric.GetGauge().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for workers_idle")

			case "pool_tasks_submitted_total_" + channel:
				metricValues["tasks_submitted"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for tasks_submitted")

			case "pool_tasks_completed_total_" + channel:
				metricValues["tasks_completed"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for tasks_completed")

			case "pool_tasks_successful_total_" + channel:
				metricValues["tasks_successful"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for tasks_successful")

			case "pool_tasks_failed_total_" + channel:
				metricValues["tasks_failed"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for tasks_failed")

			case "pool_tasks_waiting_" + channel:
				metricValues["tasks_waiting"] = metric.GetGauge().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for tasks_waiting")
			}
		}

		assert.Equal(t, float64(3), metricValues["tasks_submitted"], "Expected 3 tasks submitted")
		assert.Equal(t, float64(3), metricValues["tasks_completed"], "Expected 3 tasks completed")
		assert.Equal(t, float64(3), metricValues["tasks_successful"], "Expected 3 successful tasks")
		assert.Equal(t, float64(0), metricValues["tasks_failed"], "Expected 0 failed tasks")
		assert.Equal(t, float64(0), metricValues["tasks_waiting"], "Expected 0 waiting tasks")

		// Workers should be idle after tasks complete
		assert.Equal(t, float64(3), metricValues["workers_idle"], "Should have idle workers")
		pool.StopAndWait()
	})

	t.Run("worker pool metrics - with failures", func(t *testing.T) {
		channel := "test_channel_failures"
		pool := pond.New(2, 5)

		ms.RegisterPoolMetrics(channel, pool)

		// Submit tasks that will panic
		for i := 0; i < 2; i++ {
			pool.Submit(func() {
				panic("test panic")
			})
		}

		// Submit successful task
		pool.Submit(func() {
			time.Sleep(5 * time.Millisecond)
		})

		// Wait for tasks to complete
		time.Sleep(10 * time.Millisecond)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		metricValues := make(map[string]float64)

		for _, mf := range metricFamilies {
			metric := mf.GetMetric()[0]
			switch mf.GetName() {
			case "pool_tasks_failed_total_" + channel:
				metricValues["tasks_failed"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for failed tasks")
			case "pool_tasks_successful_total_" + channel:
				metricValues["tasks_successful"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for successful tasks")
			case "pool_tasks_submitted_total_" + channel:
				metricValues["tasks_submitted"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for submitted tasks")
			case "pool_tasks_completed_total_" + channel:
				metricValues["tasks_completed"] = metric.GetCounter().GetValue()
				assert.Equal(t, channel, metric.GetLabel()[0].GetValue(), "Unexpected channel label for completed tasks")
			}
		}

		assert.Equal(t, float64(3), metricValues["tasks_submitted"], "Expected 3 total tasks")
		assert.Equal(t, float64(2), metricValues["tasks_failed"], "Expected 2 failed tasks")
		assert.Equal(t, float64(1), metricValues["tasks_successful"], "Expected 1 successful task")
		assert.Equal(t, float64(3), metricValues["tasks_completed"], "Expected 3 completed tasks (both successful and failed)")

		pool.StopAndWait()
	})
}
