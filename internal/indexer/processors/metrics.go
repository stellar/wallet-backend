package processors

// MetricsServiceInterface defines the metrics operations needed by processors
type MetricsServiceInterface interface {
	IncStateChangeCreated(category string)
	ObserveStateChangeProcessingDuration(processor string, duration float64)
}
