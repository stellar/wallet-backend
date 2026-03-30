package metrics

import (
	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus collectors for the wallet-backend service.
type Metrics struct {
	DB        *DBMetrics
	RPC       *RPCMetrics
	Ingestion *IngestionMetrics
	HTTP      *HTTPMetrics
	GraphQL   *GraphQLMetrics
	Auth      *AuthMetrics
	registry  *prometheus.Registry
}

// NewMetrics creates a new Metrics instance with all sub-struct collectors registered.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	return &Metrics{
		DB:        newDBMetrics(reg),
		RPC:       newRPCMetrics(reg),
		Ingestion: newIngestionMetrics(reg),
		HTTP:      newHTTPMetrics(reg),
		GraphQL:   NewGraphQLMetrics(reg),
		Auth:      newAuthMetrics(reg),
		registry:  reg,
	}
}

// Registry returns the prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// RegisterPoolMetrics registers pond worker pool metrics on this Metrics' registry.
func (m *Metrics) RegisterPoolMetrics(poolName string, pool pond.Pool) {
	RegisterPoolMetrics(m.registry, poolName, pool)
}

// RegisterDBPoolMetrics registers pgxpool connection pool metrics on this Metrics' registry.
func (m *Metrics) RegisterDBPoolMetrics(pool *pgxpool.Pool) {
	RegisterDBPoolMetrics(m.registry, pool)
}
