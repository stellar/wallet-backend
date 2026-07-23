package metrics

import "github.com/prometheus/client_golang/prometheus"

// DataloaderMetrics holds Prometheus collectors for GraphQL dataloader batching behavior.
// Batching efficiency is otherwise invisible in production: these expose how many keys land
// in each batch and how long the batch's fetch takes, per loader.
type DataloaderMetrics struct {
	// BatchSize observes the number of distinct keys collected into one batch, labeled by loader.
	// A distribution clustered at 1 signals a loader that never gets to batch (each key fires its
	// own fetch); a distribution near loaderBatchCapacity signals a page size the loader is
	// straining to keep up with.
	//
	//	histogram_quantile(0.50, rate(wallet_graphql_dataloader_batch_size_bucket[5m]))
	//
	// Labels: loader.
	BatchSize *prometheus.HistogramVec

	// FetchDuration observes the wall time of one batch's fetch execution, spanning every
	// shape-group fetch the batch was split into. Labels: loader.
	//
	//	histogram_quantile(0.99, rate(wallet_graphql_dataloader_fetch_duration_seconds_bucket[5m]))
	FetchDuration *prometheus.HistogramVec
}

// newDataloaderMetrics creates and registers all dataloader Prometheus collectors.
func newDataloaderMetrics(reg prometheus.Registerer) *DataloaderMetrics {
	m := &DataloaderMetrics{
		BatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_graphql_dataloader_batch_size",
			Help:    "Number of distinct keys collected into one dataloader batch.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 100},
		}, []string{"loader"}),
		FetchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_graphql_dataloader_fetch_duration_seconds",
			Help:    "Duration of a dataloader batch's fetch execution, spanning all shape-group fetches.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, 1},
		}, []string{"loader"}),
	}
	reg.MustRegister(m.BatchSize, m.FetchDuration)
	return m
}
