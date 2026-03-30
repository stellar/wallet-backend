package metrics

import (
	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics registers Prometheus metrics for a pond worker pool.
//
// Each metric carries a ConstLabel pool_name to disambiguate multiple pools
// (e.g. ledger_indexer, backfill, contract_metadata) on the same registry.
// ConstLabels is used here because GaugeFunc/CounterFunc have no Vec variant.
//
// Registered metrics:
//
//   - wallet_pool_workers_running (gauge) — current active goroutines in the pool.
//     A sustained value near the pool's max size signals saturation.
//
//   - wallet_pool_tasks_submitted_total (counter) — throughput of task submissions.
//     Use rate() for tasks/sec dashboards.
//
//   - wallet_pool_tasks_waiting (gauge) — queue depth / backpressure indicator.
//     A growing value means workers can't keep up with submissions.
//
//   - wallet_pool_tasks_successful_total (counter) — healthy task completions.
//
//   - wallet_pool_tasks_failed_total (counter) — tasks that ended with a panic.
//     Any non-zero rate is alert-worthy.
//
//   - wallet_pool_tasks_dropped_total (counter) — tasks rejected because the queue was full.
//     Any non-zero rate is alert-worthy and indicates the pool needs tuning.
func RegisterPoolMetrics(reg prometheus.Registerer, poolName string, pool pond.Pool) {
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "wallet_pool_workers_running",
			Help:        "Number of running worker goroutines.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.RunningWorkers()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_submitted_total",
			Help:        "Total number of tasks submitted.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.SubmittedTasks()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "wallet_pool_tasks_waiting",
			Help:        "Number of tasks currently waiting in the queue.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.WaitingTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_successful_total",
			Help:        "Total number of tasks that completed successfully.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.SuccessfulTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_failed_total",
			Help:        "Total number of tasks that completed with panic.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.FailedTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_dropped_total",
			Help:        "Total number of tasks dropped because the queue was full.",
			ConstLabels: prometheus.Labels{"pool_name": poolName},
		}, func() float64 { return float64(pool.DroppedTasks()) }),
	)
}

// RegisterDBPoolMetrics registers Prometheus metrics for pgxpool connection pool statistics.
//
// Gauges — point-in-time connection state:
//
//   - wallet_db_pool_acquired_conns — connections currently checked out by application code.
//     High values relative to max_conns indicate heavy DB load.
//
//   - wallet_db_pool_idle_conns — connections available for immediate use.
//     Consistently zero means every acquire must wait or create a new connection.
//
//   - wallet_db_pool_constructing_conns — connections being established (TCP + TLS + auth).
//     Spikes indicate connection storms, often after pool exhaustion or database restarts.
//
//   - wallet_db_pool_total_conns — all open connections (acquired + idle + constructing).
//
//   - wallet_db_pool_max_conns — configured pool size limit.
//     Use with acquired_conns for utilization ratio: acquired_conns / max_conns.
//
// Counters — monotonic acquisition and lifecycle stats:
//
//   - wallet_db_pool_acquire_total — total connection acquisitions.
//     Use rate() for acquisition throughput.
//
//   - wallet_db_pool_acquire_wait_seconds_total — cumulative time spent waiting to acquire.
//     Divide by acquire_total for average wait time:
//     rate(acquire_wait_seconds_total[5m]) / rate(acquire_total[5m])
//
//   - wallet_db_pool_empty_acquire_total — acquires that found no idle connection.
//     A high ratio to acquire_total signals the pool is undersized.
//
//   - wallet_db_pool_empty_acquire_wait_seconds_total — cumulative time waiting when pool was empty.
//     The "pure contention" component of acquire wait time.
//
//   - wallet_db_pool_new_conns_total — new connections created (not reused from idle).
//     High rate indicates connection churn; check max idle time and lifetime settings.
//
//   - wallet_db_pool_canceled_acquire_total — acquisitions canceled by context (timeout/cancellation).
//     Any non-zero rate is alert-worthy — the app gave up waiting for a DB connection.
//
//   - wallet_db_pool_max_lifetime_destroy_total — connections closed by the max lifetime policy.
//     Normal background churn; a spike after config change is expected.
//
//   - wallet_db_pool_max_idle_destroy_total — connections closed by the max idle time policy.
//     Normal during low-traffic periods; sustained high rate during peak traffic is unusual.
func RegisterDBPoolMetrics(reg prometheus.Registerer, pool *pgxpool.Pool) {
	reg.MustRegister(
		// Gauges — point-in-time connection state.
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_acquired_conns",
			Help: "Number of currently acquired connections.",
		}, func() float64 { return float64(pool.Stat().AcquiredConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_idle_conns",
			Help: "Number of currently idle connections.",
		}, func() float64 { return float64(pool.Stat().IdleConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_constructing_conns",
			Help: "Number of connections currently being established.",
		}, func() float64 { return float64(pool.Stat().ConstructingConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_total_conns",
			Help: "Total number of connections currently open.",
		}, func() float64 { return float64(pool.Stat().TotalConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_max_conns",
			Help: "Maximum number of connections allowed.",
		}, func() float64 { return float64(pool.Stat().MaxConns()) }),

		// Counters — monotonic acquisition and lifecycle stats.
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_acquire_total",
			Help: "Total number of connection acquisitions from the pool.",
		}, func() float64 { return float64(pool.Stat().AcquireCount()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_acquire_wait_seconds_total",
			Help: "Total cumulative time spent waiting to acquire connections.",
		}, func() float64 { return pool.Stat().AcquireDuration().Seconds() }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_empty_acquire_total",
			Help: "Total number of acquires that had to wait because no idle connections were available.",
		}, func() float64 { return float64(pool.Stat().EmptyAcquireCount()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_empty_acquire_wait_seconds_total",
			Help: "Total cumulative time spent waiting for a connection from an empty pool.",
		}, func() float64 { return pool.Stat().EmptyAcquireWaitTime().Seconds() }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_new_conns_total",
			Help: "Total number of new connections created.",
		}, func() float64 { return float64(pool.Stat().NewConnsCount()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_canceled_acquire_total",
			Help: "Total number of connection acquisitions canceled by context.",
		}, func() float64 { return float64(pool.Stat().CanceledAcquireCount()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_max_lifetime_destroy_total",
			Help: "Total number of connections destroyed because they exceeded max lifetime.",
		}, func() float64 { return float64(pool.Stat().MaxLifetimeDestroyCount()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_max_idle_destroy_total",
			Help: "Total number of connections destroyed because they exceeded max idle time.",
		}, func() float64 { return float64(pool.Stat().MaxIdleDestroyCount()) }),
	)
}
