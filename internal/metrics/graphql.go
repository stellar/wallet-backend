package metrics

import "github.com/prometheus/client_golang/prometheus"

// GraphQLMetrics holds Prometheus collectors for GraphQL operations.
type GraphQLMetrics struct {
	FieldDuration *prometheus.SummaryVec
	FieldsTotal   *prometheus.CounterVec
	Complexity    *prometheus.SummaryVec
	ErrorsTotal   *prometheus.CounterVec
}

func newGraphQLMetrics(reg prometheus.Registerer) *GraphQLMetrics {
	m := &GraphQLMetrics{
		FieldDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_graphql_field_duration_seconds",
			Help:       "Duration of GraphQL field resolver execution.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"operation_name", "field_name"}),
		FieldsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_graphql_fields_total",
			Help: "Total number of GraphQL field resolutions.",
		}, []string{"operation_name", "field_name", "success"}),
		Complexity: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "wallet_graphql_complexity",
			Help:       "GraphQL query complexity values.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"operation_name"}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_graphql_errors_total",
			Help: "Total number of GraphQL errors.",
		}, []string{"operation_name", "error_type"}),
	}
	reg.MustRegister(m.FieldDuration, m.FieldsTotal, m.Complexity, m.ErrorsTotal)
	return m
}
