// Package middleware provides HTTP middleware components for the wallet backend server.
// This file implements GraphQL field-level metrics tracking.
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

	// Get operation context to extract operation name
	oc := graphql.GetOperationContext(ctx)
	operationName := "<unnamed>"
	if oc != nil && oc.OperationName != "" {
		operationName = oc.OperationName
	}

	// Get the field name
	fieldName := fc.Field.Name

	// Start timing the field resolution
	startTime := time.Now()

	// Execute the actual field resolver
	res, err := next(ctx)

	// Calculate duration
	duration := time.Since(startTime).Seconds()

	// Record duration metric
	m.metricsService.ObserveGraphQLFieldDuration(operationName, fieldName, duration)

	// Record success/failure count
	success := err == nil
	m.metricsService.IncGraphQLField(operationName, fieldName, success)

	// If there was an error, track error type
	if err != nil {
		errorType := classifyGraphQLError(err)
		m.metricsService.IncGraphQLError(operationName, errorType)
	}

	return res, err
}

// classifyGraphQLError determines the error type from a GraphQL error.
// It inspects error extensions and error structure to categorize the error.
func classifyGraphQLError(err error) string {
	// Check if it's a gqlerror.Error with extensions
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		if gqlErr.Extensions != nil {
			// Check for error code in extensions
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

		// Check for common error message patterns
		msg := gqlErr.Message
		if msg != "" {
			// Database errors
			if containsAny(msg, []string{"database", "sql", "query failed"}) {
				return "database_error"
			}
			// Validation errors
			if containsAny(msg, []string{"invalid", "validation", "required"}) {
				return "validation_error"
			}
			// Not found errors
			if containsAny(msg, []string{"not found", "does not exist"}) {
				return "not_found"
			}
		}
	}

	// Default to unknown error type
	return "unknown"
}

// containsAny checks if a string contains any of the given substrings.
func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}
