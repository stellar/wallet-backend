package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// newTestGraphQLMetrics creates a GraphQLMetrics instance registered against a fresh registry.
func newTestGraphQLMetrics(t *testing.T) *metrics.GraphQLMetrics {
	t.Helper()
	reg := prometheus.NewRegistry()
	return metrics.NewGraphQLMetrics(reg)
}

// withOperationContext returns a context with a gqlgen OperationContext set,
// simulating what gqlgen provides inside AroundOperations.
func withOperationContext(ctx context.Context, name string, op ast.Operation) context.Context {
	oc := &graphql.OperationContext{
		OperationName: name,
		Operation: &ast.OperationDefinition{
			Operation: op,
		},
	}
	return graphql.WithOperationContext(ctx, oc)
}

func TestGraphQLOperationMetrics_SuccessfulQuery(t *testing.T) {
	m := newTestGraphQLMetrics(t)
	opMetrics := NewGraphQLOperationMetrics(m)

	respData := json.RawMessage(`{"accountByAddress":{"id":"123"}}`)
	resp := &graphql.Response{Data: respData}

	ctx := withOperationContext(context.Background(), "GetAccount", ast.Query)

	// Simulate AroundOperations: call Middleware, then call the returned ResponseHandler.
	next := func(ctx context.Context) graphql.ResponseHandler {
		return func(ctx context.Context) *graphql.Response {
			return resp
		}
	}
	responseHandler := opMetrics.Middleware(ctx, next)

	// In-flight should be 1 after Middleware() but before response.
	assert.Equal(t, 1.0, testutil.ToFloat64(m.InFlightOperations))

	result := responseHandler(ctx)
	require.NotNil(t, result)

	// In-flight should be back to 0 after response.
	assert.Equal(t, 0.0, testutil.ToFloat64(m.InFlightOperations))

	// OperationsTotal incremented with success.
	assert.Equal(t, 1.0, testutil.ToFloat64(m.OperationsTotal.WithLabelValues("GetAccount", "query", "success")))

	// OperationDuration observed (at least 1 series).
	assert.Equal(t, 1, testutil.CollectAndCount(m.OperationDuration))

	// ResponseSize observed with correct data length.
	assert.Equal(t, 1, testutil.CollectAndCount(m.ResponseSize))
}

func TestGraphQLOperationMetrics_ErrorResponse(t *testing.T) {
	m := newTestGraphQLMetrics(t)
	opMetrics := NewGraphQLOperationMetrics(m)

	resp := &graphql.Response{
		Data: json.RawMessage(`null`),
		Errors: gqlerror.List{
			{
				Message:    "not authenticated",
				Extensions: map[string]interface{}{"code": "UNAUTHENTICATED"},
			},
		},
	}

	ctx := withOperationContext(context.Background(), "GetAccount", ast.Query)
	next := func(ctx context.Context) graphql.ResponseHandler {
		return func(ctx context.Context) *graphql.Response {
			return resp
		}
	}
	responseHandler := opMetrics.Middleware(ctx, next)
	result := responseHandler(ctx)
	require.NotNil(t, result)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.OperationsTotal.WithLabelValues("GetAccount", "query", "error")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("GetAccount", "auth_error")))
	assert.Equal(t, 0.0, testutil.ToFloat64(m.InFlightOperations))
}

func TestGraphQLOperationMetrics_MultipleErrors(t *testing.T) {
	m := newTestGraphQLMetrics(t)
	opMetrics := NewGraphQLOperationMetrics(m)

	resp := &graphql.Response{
		Data: json.RawMessage(`null`),
		Errors: gqlerror.List{
			{
				Message:    "validation failed",
				Extensions: map[string]interface{}{"code": "GRAPHQL_VALIDATION_FAILED"},
			},
			{
				Message:    "bad input",
				Extensions: map[string]interface{}{"code": "BAD_USER_INPUT"},
			},
		},
	}

	ctx := withOperationContext(context.Background(), "CreateTx", ast.Mutation)
	next := func(ctx context.Context) graphql.ResponseHandler {
		return func(ctx context.Context) *graphql.Response {
			return resp
		}
	}
	responseHandler := opMetrics.Middleware(ctx, next)
	responseHandler(ctx)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.OperationsTotal.WithLabelValues("CreateTx", "mutation", "error")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("CreateTx", "validation_error")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("CreateTx", "bad_input")))
}

func TestGraphQLOperationMetrics_NilResponse(t *testing.T) {
	m := newTestGraphQLMetrics(t)
	opMetrics := NewGraphQLOperationMetrics(m)

	ctx := withOperationContext(context.Background(), "GetAccount", ast.Query)
	next := func(ctx context.Context) graphql.ResponseHandler {
		return func(ctx context.Context) *graphql.Response {
			return nil
		}
	}
	responseHandler := opMetrics.Middleware(ctx, next)

	assert.Equal(t, 1.0, testutil.ToFloat64(m.InFlightOperations))

	result := responseHandler(ctx)
	assert.Nil(t, result)

	// In-flight decremented even on nil response.
	assert.Equal(t, 0.0, testutil.ToFloat64(m.InFlightOperations))
}

func TestClassifyGraphQLError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "validation error",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "GRAPHQL_VALIDATION_FAILED"}},
			expected: "validation_error",
		},
		{
			name:     "parse error",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "GRAPHQL_PARSE_FAILED"}},
			expected: "parse_error",
		},
		{
			name:     "bad input",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "BAD_USER_INPUT"}},
			expected: "bad_input",
		},
		{
			name:     "auth error",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "UNAUTHENTICATED"}},
			expected: "auth_error",
		},
		{
			name:     "forbidden",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "FORBIDDEN"}},
			expected: "forbidden",
		},
		{
			name:     "internal error",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "INTERNAL_SERVER_ERROR"}},
			expected: "internal_error",
		},
		{
			name:     "unknown code passed through",
			err:      &gqlerror.Error{Extensions: map[string]interface{}{"code": "CUSTOM_CODE"}},
			expected: "CUSTOM_CODE",
		},
		{
			name:     "no extensions",
			err:      &gqlerror.Error{Message: "something went wrong"},
			expected: "unknown",
		},
		{
			name:     "non-gqlerror",
			err:      assert.AnError,
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, classifyGraphQLError(tt.err))
		})
	}
}
