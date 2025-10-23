package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// GraphQLFieldMetrics tracks metrics for individual GraphQL field resolvers.
// It measures field execution duration, counts resolutions, and tracks errors.
type GraphQLFieldMetrics struct {
	metricsService metrics.MetricsService
}

// NewGraphQLFieldMetrics creates a new GraphQL field metrics middleware.
func NewGraphQLFieldMetrics(metricsService metrics.MetricsService) *GraphQLFieldMetrics {
	return &GraphQLFieldMetrics{
		metricsService: metricsService,
	}
}

// Middleware is the field middleware function that wraps field resolver execution.
// It measures execution time and tracks success/failure for each field resolution.
func (m *GraphQLFieldMetrics) Middleware(ctx context.Context, next graphql.Resolver) (interface{}, error) {
	// Get the field context to extract operation and field information
	fc := graphql.GetFieldContext(ctx)
	if fc == nil {
		// If we can't get field context, just pass through
		return next(ctx)
	}

	oc := graphql.GetOperationContext(ctx)
	operationName := "<unnamed>"
	if oc != nil && oc.OperationName != "" {
		operationName = oc.OperationName
	}

	fieldName := fc.Field.Name

	startTime := time.Now()
	res, err := next(ctx)
	duration := time.Since(startTime).Seconds()
	m.metricsService.ObserveGraphQLFieldDuration(operationName, fieldName, duration)

	success := err == nil
	m.metricsService.IncGraphQLField(operationName, fieldName, success)

	if err != nil {
		errorType := classifyGraphQLError(err)
		m.metricsService.IncGraphQLError(operationName, errorType)
	}

	return res, err
}

// classifyGraphQLError determines the error type from a GraphQL error.
// It inspects error extensions and error structure to categorize the error.
func classifyGraphQLError(err error) string {
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		if gqlErr.Extensions != nil {
			if code, ok := gqlErr.Extensions["code"].(string); ok {
				switch code {
				case "GRAPHQL_VALIDATION_FAILED":
					return "validation_error"
				case "GRAPHQL_PARSE_FAILED":
					return "parse_error"
				case "BAD_USER_INPUT":
					return "bad_input"
				case "UNAUTHENTICATED":
					return "auth_error"
				case "FORBIDDEN":
					return "forbidden"
				case "INTERNAL_SERVER_ERROR":
					return "internal_error"
				default:
					return code
				}
			}
		}
	}

	return "unknown"
}
