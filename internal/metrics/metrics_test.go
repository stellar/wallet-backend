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
		ms.ObserveIngestionDuration("transaction", 1.0)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_duration_seconds" {
				found = true
				assert.Equal(t, 1, len(mf.GetMetric()))
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

func TestDBErrorMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)
	queryType := "SELECT"
	table := "accounts"
	errorType := "no_rows"

	ms.IncDBQueryError(queryType, table, errorType)

	metricFamilies, err := ms.GetRegistry().Gather()
	require.NoError(t, err)

	found := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "db_query_errors_total" {
			found = true
			metric := mf.GetMetric()[0]
			assert.Equal(t, float64(1), metric.GetCounter().GetValue())
			// Verify labels
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, queryType, labels["query_type"])
			assert.Equal(t, table, labels["table"])
			assert.Equal(t, errorType, labels["error_type"])
		}
	}

	assert.True(t, found, "DB error metric not found")
}

func TestDBTransactionMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("transaction commit metrics", func(t *testing.T) {
		ms.IncDBTransaction("commit")
		ms.ObserveDBTransactionDuration("commit", 0.05)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		foundCounter := false
		foundDuration := false

		for _, mf := range metricFamilies {
			switch mf.GetName() {
			case "db_transactions_total":
				foundCounter = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(1), metric.GetCounter().GetValue())
				labels := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				assert.Equal(t, "commit", labels["status"])
			case "db_transaction_duration_seconds":
				foundDuration = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
				assert.Equal(t, 0.05, metric.GetSummary().GetSampleSum())
				labels := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				assert.Equal(t, "commit", labels["status"])
			}
		}

		assert.True(t, foundCounter, "Transaction counter not found")
		assert.True(t, foundDuration, "Transaction duration not found")
	})

	t.Run("transaction rollback metrics", func(t *testing.T) {
		ms.IncDBTransaction("rollback")
		ms.ObserveDBTransactionDuration("rollback", 0.02)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "db_transactions_total" {
				// Should have 2 metrics now (commit and rollback)
				assert.GreaterOrEqual(t, len(mf.GetMetric()), 2)
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["status"] == "rollback" {
						found = true
						assert.Equal(t, float64(1), metric.GetCounter().GetValue())
					}
				}
			}
		}

		assert.True(t, found, "Rollback metric not found")
	})
}

func TestDBBatchSizeMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("batch insert size tracking", func(t *testing.T) {
		operation := "INSERT"
		table := "transactions"
		batchSize := 100

		ms.ObserveDBBatchSize(operation, table, batchSize)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "db_batch_operation_size" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
				assert.Equal(t, float64(100), metric.GetHistogram().GetSampleSum())
				// Verify labels
				labels := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				assert.Equal(t, operation, labels["operation"])
				assert.Equal(t, table, labels["table"])
			}
		}

		assert.True(t, found, "Batch size metric not found")
	})

	t.Run("multiple batch sizes distribution", func(t *testing.T) {
		operation := "SELECT"
		table := "accounts"

		// Record multiple batch operations of different sizes
		ms.ObserveDBBatchSize(operation, table, 10)
		ms.ObserveDBBatchSize(operation, table, 50)
		ms.ObserveDBBatchSize(operation, table, 100)
		ms.ObserveDBBatchSize(operation, table, 500)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "db_batch_operation_size" {
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["operation"] == operation && labels["table"] == table {
						found = true
						histogram := metric.GetHistogram()
						assert.Equal(t, uint64(4), histogram.GetSampleCount())
						assert.Equal(t, float64(660), histogram.GetSampleSum()) // 10+50+100+500

						// Verify histogram buckets are populated
						assert.NotNil(t, histogram.GetBucket())
						assert.Greater(t, len(histogram.GetBucket()), 0)
					}
				}
			}
		}

		assert.True(t, found, "Batch size distribution metric not found")
	})
}

func TestIngestionPhaseMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("ingestion phase duration metrics", func(t *testing.T) {
		// Record durations for different phases
		ms.ObserveIngestionPhaseDuration("fetch_ledgers", 0.5)
		ms.ObserveIngestionPhaseDuration("collect_transaction_data", 1.2)
		ms.ObserveIngestionPhaseDuration("fetch_existing_accounts", 0.3)
		ms.ObserveIngestionPhaseDuration("process_and_buffer", 2.1)
		ms.ObserveIngestionPhaseDuration("merge_buffers", 0.1)
		ms.ObserveIngestionPhaseDuration("db_insertion", 1.5)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_phase_duration_seconds" {
				found = true
				// Should have 6 metrics (one for each phase)
				assert.Equal(t, 6, len(mf.GetMetric()))

				// Verify each phase is recorded
				phaseLabels := make(map[string]bool)
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					phaseLabels[labels["phase"]] = true
					assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
				}

				assert.True(t, phaseLabels["fetch_ledgers"])
				assert.True(t, phaseLabels["collect_transaction_data"])
				assert.True(t, phaseLabels["fetch_existing_accounts"])
				assert.True(t, phaseLabels["process_and_buffer"])
				assert.True(t, phaseLabels["merge_buffers"])
				assert.True(t, phaseLabels["db_insertion"])
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion ledgers processed counter", func(t *testing.T) {
		ms.IncIngestionLedgersProcessed(10)
		ms.IncIngestionLedgersProcessed(5)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_ledgers_processed_total" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(15), metric.GetCounter().GetValue())
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion transactions processed counter", func(t *testing.T) {
		ms.IncIngestionTransactionsProcessed(100)
		ms.IncIngestionTransactionsProcessed(50)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_transactions_processed_total" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(150), metric.GetCounter().GetValue())
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion operations processed counter", func(t *testing.T) {
		ms.IncIngestionOperationsProcessed(200)
		ms.IncIngestionOperationsProcessed(75)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_operations_processed_total" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(275), metric.GetCounter().GetValue())
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion batch size histogram", func(t *testing.T) {
		// Record various batch sizes
		ms.ObserveIngestionBatchSize(1)
		ms.ObserveIngestionBatchSize(10)
		ms.ObserveIngestionBatchSize(50)
		ms.ObserveIngestionBatchSize(25)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_batch_size" {
				found = true
				metric := mf.GetMetric()[0]
				histogram := metric.GetHistogram()
				assert.Equal(t, uint64(4), histogram.GetSampleCount())
				assert.Equal(t, float64(86), histogram.GetSampleSum()) // 1 + 10 + 50 + 25 = 86
			}
		}
		assert.True(t, found)
	})

	t.Run("ingestion participants count histogram", func(t *testing.T) {
		// Record various participant counts
		ms.ObserveIngestionParticipantsCount(5)
		ms.ObserveIngestionParticipantsCount(100)
		ms.ObserveIngestionParticipantsCount(500)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "ingestion_participants_count" {
				found = true
				metric := mf.GetMetric()[0]
				histogram := metric.GetHistogram()
				assert.Equal(t, uint64(3), histogram.GetSampleCount())
				assert.Equal(t, float64(605), histogram.GetSampleSum()) // 5 + 100 + 500 = 605
			}
		}
		assert.True(t, found)
	})
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

func TestRPCMethodMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ms := NewMetricsService(db)

	t.Run("RPC method calls counter", func(t *testing.T) {
		method := "GetTransaction"

		ms.IncRPCMethodCalls(method)
		ms.IncRPCMethodCalls(method)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_method_calls_total" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, float64(2), metric.GetCounter().GetValue())
				assert.Equal(t, method, metric.GetLabel()[0].GetValue())
			}
		}
		assert.True(t, found, "rpc_method_calls_total metric not found")
	})

	t.Run("RPC method duration", func(t *testing.T) {
		method := "SendTransaction"

		ms.ObserveRPCMethodDuration(method, 0.25)
		ms.ObserveRPCMethodDuration(method, 0.35)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_method_duration_seconds" {
				found = true
				metric := mf.GetMetric()[0]
				assert.Equal(t, uint64(2), metric.GetSummary().GetSampleCount())
				assert.Equal(t, 0.6, metric.GetSummary().GetSampleSum())
				assert.Equal(t, method, metric.GetLabel()[0].GetValue())
			}
		}
		assert.True(t, found, "rpc_method_duration_seconds metric not found")
	})

	t.Run("RPC method errors by type", func(t *testing.T) {
		method := "GetLedgers"
		errorType := "json_unmarshal_error"

		ms.IncRPCMethodErrors(method, errorType)
		ms.IncRPCMethodErrors(method, "rpc_error")
		ms.IncRPCMethodErrors(method, errorType)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		found := false
		unmarshalErrors := 0
		rpcErrors := 0

		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_method_errors_total" {
				found = true
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}

					assert.Equal(t, method, labels["method"])

					switch labels["error_type"] {
					case "json_unmarshal_error":
						unmarshalErrors = int(metric.GetCounter().GetValue())
					case "rpc_error":
						rpcErrors = int(metric.GetCounter().GetValue())
					}
				}
			}
		}

		assert.True(t, found, "rpc_method_errors_total metric not found")
		assert.Equal(t, 2, unmarshalErrors, "Expected 2 json_unmarshal_error")
		assert.Equal(t, 1, rpcErrors, "Expected 1 rpc_error")
	})

	t.Run("All RPC method metrics together", func(t *testing.T) {
		method := "SimulateTransaction"

		// Simulate a complete RPC method execution
		ms.IncRPCMethodCalls(method)
		ms.ObserveRPCMethodDuration(method, 0.15)

		metricFamilies, err := ms.GetRegistry().Gather()
		require.NoError(t, err)

		foundCalls := false
		foundDuration := false

		for _, mf := range metricFamilies {
			switch mf.GetName() {
			case "rpc_method_calls_total":
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["method"] == method {
						foundCalls = true
						assert.Equal(t, float64(1), metric.GetCounter().GetValue())
					}
				}
			case "rpc_method_duration_seconds":
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["method"] == method {
						foundDuration = true
						assert.Equal(t, uint64(1), metric.GetSummary().GetSampleCount())
					}
				}
			}
		}

		assert.True(t, foundCalls, "Method calls metric not found for "+method)
		assert.True(t, foundDuration, "Method duration metric not found for "+method)
	})

	t.Run("Multiple methods tracked independently", func(t *testing.T) {
		// Create a new metrics service to avoid interference from previous tests
		msNew := NewMetricsService(db)
		methods := []string{"GetHealth", "GetLedgerEntries", "GetAccountLedgerSequence"}

		for i, method := range methods {
			msNew.IncRPCMethodCalls(method)
			msNew.ObserveRPCMethodDuration(method, float64(i+1)*0.1)
		}

		metricFamilies, err := msNew.GetRegistry().Gather()
		require.NoError(t, err)

		methodsFound := make(map[string]bool)

		for _, mf := range metricFamilies {
			if mf.GetName() == "rpc_method_calls_total" {
				for _, metric := range mf.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					method := labels["method"]
					for _, m := range methods {
						if method == m {
							methodsFound[m] = true
							assert.Equal(t, float64(1), metric.GetCounter().GetValue())
						}
					}
				}
			}
		}

		for _, method := range methods {
			assert.True(t, methodsFound[method], "Method "+method+" not tracked")
		}
	})
}
