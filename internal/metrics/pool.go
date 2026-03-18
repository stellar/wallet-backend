package metrics

import (
	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics registers Prometheus metrics for a pond worker pool.
func RegisterPoolMetrics(reg prometheus.Registerer, channel string, pool pond.Pool) {
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "wallet_pool_workers_running",
			Help:        "Number of running worker goroutines.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.RunningWorkers()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_submitted_total",
			Help:        "Number of tasks submitted.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.SubmittedTasks()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "wallet_pool_tasks_waiting",
			Help:        "Number of tasks currently waiting in the queue.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.WaitingTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_successful_total",
			Help:        "Number of tasks that completed successfully.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.SuccessfulTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_failed_total",
			Help:        "Number of tasks that completed with panic.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.FailedTasks()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "wallet_pool_tasks_completed_total",
			Help:        "Number of tasks that completed either successfully or with panic.",
			ConstLabels: prometheus.Labels{"channel": channel},
		}, func() float64 { return float64(pool.CompletedTasks()) }),
	)
}

// RegisterDBPoolMetrics registers Prometheus metrics for pgxpool connection pool statistics.
func RegisterDBPoolMetrics(reg prometheus.Registerer, pool *pgxpool.Pool) {
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_acquire_total",
			Help: "Total number of connection acquisitions from the pool.",
		}, func() float64 { return float64(pool.Stat().AcquireCount()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_acquired_conns",
			Help: "Number of currently acquired connections.",
		}, func() float64 { return float64(pool.Stat().AcquiredConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_idle_conns",
			Help: "Number of currently idle connections.",
		}, func() float64 { return float64(pool.Stat().IdleConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_total_conns",
			Help: "Total number of connections currently open.",
		}, func() float64 { return float64(pool.Stat().TotalConns()) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_max_conns",
			Help: "Maximum number of connections allowed.",
		}, func() float64 { return float64(pool.Stat().MaxConns()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_acquire_duration_seconds",
			Help: "Total time spent waiting to acquire connections.",
		}, func() float64 { return pool.Stat().AcquireDuration().Seconds() }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "wallet_db_pool_constructing_conns",
			Help: "Number of connections currently being established.",
		}, func() float64 { return float64(pool.Stat().ConstructingConns()) }),

		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wallet_db_pool_empty_acquire_total",
			Help: "Total number of acquires that had to wait because no idle connections were available.",
		}, func() float64 { return float64(pool.Stat().EmptyAcquireCount()) }),
	)
}
