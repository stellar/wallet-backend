package entities

type HealthStatus string

const (
	Healthy   HealthStatus = "healthy"
	Unhealthy HealthStatus = "unhealthy"
	Error     HealthStatus = "error"
)

type HealthResponse struct {
	Status HealthStatus `json:"status"`
}
