package middleware

import (
	"context"

	"github.com/99designs/gqlgen/graphql"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// GraphQLFieldMetrics tracks deprecated field accesses via a lightweight field middleware.
// It fires only when a resolved field has the @deprecated directive, producing zero
// series until deprecated fields actually exist in the schema.
type GraphQLFieldMetrics struct {
	metrics *metrics.GraphQLMetrics
}

// NewGraphQLFieldMetrics creates a new deprecated-field tracking middleware.
func NewGraphQLFieldMetrics(graphqlMetrics *metrics.GraphQLMetrics) *GraphQLFieldMetrics {
	return &GraphQLFieldMetrics{metrics: graphqlMetrics}
}

// Middleware is the field middleware function. It calls the resolver first, then
// checks whether the field carries a @deprecated directive. Non-deprecated fields
// (the vast majority) short-circuit after a nil-pointer guard and a single map lookup.
func (m *GraphQLFieldMetrics) Middleware(ctx context.Context, next graphql.Resolver) (interface{}, error) {
	res, err := next(ctx)

	fc := graphql.GetFieldContext(ctx)
	if fc == nil || fc.Field.Field == nil || fc.Field.Field.Definition == nil {
		return res, err
	}

	if fc.Field.Field.Definition.Directives.ForName("deprecated") != nil {
		oc := graphql.GetOperationContext(ctx)
		operationName := GetOperationIdentifier(oc)
		fieldPath := GetFieldPath(fc)
		if fieldPath == "" {
			fieldPath = fc.Field.Name
		}
		m.metrics.DeprecatedFieldsTotal.WithLabelValues(operationName, fieldPath).Inc()
	}

	return res, err
}
