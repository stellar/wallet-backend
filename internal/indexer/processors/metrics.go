package processors

// MetricsServiceInterface defines the metrics operations needed by processors
type MetricsServiceInterface interface {
	ObserveStateChangeProcessingDuration(processor string, duration float64)
}
