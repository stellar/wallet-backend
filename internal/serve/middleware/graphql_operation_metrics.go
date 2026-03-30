package middleware

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// GraphQLOperationMetrics tracks Prometheus metrics at the GraphQL operation level.
// It hooks into gqlgen's AroundOperations to measure duration, throughput, errors,
// and response size for each query or mutation.
type GraphQLOperationMetrics struct {
	metrics *metrics.GraphQLMetrics
}

// NewGraphQLOperationMetrics creates a new operation-level metrics middleware.
func NewGraphQLOperationMetrics(m *metrics.GraphQLMetrics) *GraphQLOperationMetrics {
	return &GraphQLOperationMetrics{metrics: m}
}

// Middleware is the AroundOperations hook. It wraps the full operation lifecycle
// to record duration, in-flight count, throughput, errors, and response size.
func (m *GraphQLOperationMetrics) Middleware(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	m.metrics.InFlightOperations.Inc()
	startTime := time.Now()

	responseHandler := next(ctx)

	responded := false
	return func(ctx context.Context) *graphql.Response {
		defer func() {
			if !responded {
				responded = true
				m.metrics.InFlightOperations.Dec()
			}
		}()

		resp := responseHandler(ctx)
		if resp == nil {
			return nil
		}

		oc := graphql.GetOperationContext(ctx)
		operationName := GetOperationIdentifier(oc)
		operationType := "query"
		if oc != nil && oc.Operation != nil {
			operationType = strings.ToLower(string(oc.Operation.Operation))
		}

		duration := time.Since(startTime).Seconds()
		status := "success"
		if len(resp.Errors) > 0 {
			status = "error"
		}

		m.metrics.OperationDuration.WithLabelValues(operationName, operationType).Observe(duration)
		m.metrics.OperationsTotal.WithLabelValues(operationName, operationType, status).Inc()
		m.metrics.ResponseSize.WithLabelValues(operationName, operationType).Observe(float64(len(resp.Data)))

		for _, gqlErr := range resp.Errors {
			errorType := classifyGraphQLError(gqlErr)
			m.metrics.ErrorsTotal.WithLabelValues(operationName, errorType).Inc()
		}

		return resp
	}
}

// classifyGraphQLError determines the error type from a GraphQL error.
// It inspects error extensions to categorize the error for metrics labeling.
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
					return "unknown"
				}
			}
		}
	}

	return "unknown"
}
