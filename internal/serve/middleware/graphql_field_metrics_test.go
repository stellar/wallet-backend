package middleware

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// fieldTestContext builds a context that simulates what gqlgen provides inside an
// AroundFields middleware. In production, gqlgen injects two context values before
// calling field middleware:
//
//  1. OperationContext — identifies the top-level query/mutation (name + type).
//  2. FieldContext — identifies which field is being resolved, including its parsed
//     schema definition with any directives (e.g. @deprecated).
//
// The directives parameter attaches schema directives to the field's Definition.
// Passing an ast.DirectiveList containing {Name: "deprecated"} simulates a field
// declared as `oldField: String @deprecated(reason: "Use newField")` in the schema.
// Passing nil simulates a normal, non-deprecated field.
func fieldTestContext(operationName string, fieldName string, directives ast.DirectiveList) context.Context {
	oc := &graphql.OperationContext{
		OperationName: operationName,
		Operation: &ast.OperationDefinition{
			Operation: ast.Query,
		},
	}
	ctx := graphql.WithOperationContext(context.Background(), oc)

	fc := &graphql.FieldContext{
		Field: graphql.CollectedField{
			Field: &ast.Field{
				Name: fieldName,
				Definition: &ast.FieldDefinition{
					Name:       fieldName,
					Directives: directives,
				},
			},
		},
	}
	ctx = graphql.WithFieldContext(ctx, fc)
	return ctx
}

func TestGraphQLFieldMetrics_DeprecatedField(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	// Simulate a schema field declared as: oldField: String @deprecated(reason: "Use newField instead")
	// The middleware checks fc.Field.Field.Definition.Directives.ForName("deprecated"),
	// so the directive Name must be exactly "deprecated" to match.
	deprecatedDirectives := ast.DirectiveList{
		{Name: "deprecated", Arguments: ast.ArgumentList{
			{Name: "reason", Value: &ast.Value{Raw: "Use newField instead"}},
		}},
	}
	ctx := fieldTestContext("GetAccount", "oldField", deprecatedDirectives)

	resolver := func(ctx context.Context) (interface{}, error) {
		return "resolved", nil
	}

	res, err := fm.Middleware(ctx, resolver)
	require.NoError(t, err)
	assert.Equal(t, "resolved", res)

	// DeprecatedFieldsTotal should have been incremented.
	assert.Equal(t, 1.0, testutil.ToFloat64(m.DeprecatedFieldsTotal.WithLabelValues("GetAccount", "oldField")))
}

func TestGraphQLFieldMetrics_NonDeprecatedField(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	ctx := fieldTestContext("GetAccount", "address", nil)

	resolver := func(ctx context.Context) (interface{}, error) {
		return "resolved", nil
	}

	res, err := fm.Middleware(ctx, resolver)
	require.NoError(t, err)
	assert.Equal(t, "resolved", res)

	// No deprecated fields — counter should have zero series.
	assert.Equal(t, 0, testutil.CollectAndCount(m.DeprecatedFieldsTotal))
}

func TestGraphQLFieldMetrics_NilFieldContext(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	// Context with no FieldContext set — middleware should pass through.
	ctx := context.Background()
	resolver := func(ctx context.Context) (interface{}, error) {
		return "resolved", nil
	}

	res, err := fm.Middleware(ctx, resolver)
	require.NoError(t, err)
	assert.Equal(t, "resolved", res)
	assert.Equal(t, 0, testutil.CollectAndCount(m.DeprecatedFieldsTotal))
}

func TestGraphQLFieldMetrics_NilDefinition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	// FieldContext with nil Definition — middleware should pass through.
	oc := &graphql.OperationContext{OperationName: "GetAccount"}
	ctx := graphql.WithOperationContext(context.Background(), oc)
	fc := &graphql.FieldContext{
		Field: graphql.CollectedField{
			Field: &ast.Field{
				Name:       "address",
				Definition: nil,
			},
		},
	}
	ctx = graphql.WithFieldContext(ctx, fc)

	resolver := func(ctx context.Context) (interface{}, error) {
		return "resolved", nil
	}

	res, err := fm.Middleware(ctx, resolver)
	require.NoError(t, err)
	assert.Equal(t, "resolved", res)
	assert.Equal(t, 0, testutil.CollectAndCount(m.DeprecatedFieldsTotal))
}

func TestGraphQLFieldMetrics_ResolverError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	deprecatedDirectives := ast.DirectiveList{{Name: "deprecated"}}
	ctx := fieldTestContext("GetAccount", "oldField", deprecatedDirectives)

	resolver := func(ctx context.Context) (interface{}, error) {
		return nil, assert.AnError
	}

	res, err := fm.Middleware(ctx, resolver)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, assert.AnError)

	// Deprecated field should still be counted even when resolver errors.
	assert.Equal(t, 1.0, testutil.ToFloat64(m.DeprecatedFieldsTotal.WithLabelValues("GetAccount", "oldField")))
}

func TestGraphQLFieldMetrics_MultipleDeprecatedAccesses(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewGraphQLMetrics(reg)
	fm := NewGraphQLFieldMetrics(m)

	deprecatedDirectives := ast.DirectiveList{{Name: "deprecated"}}
	ctx := fieldTestContext("GetAccount", "oldField", deprecatedDirectives)

	resolver := func(ctx context.Context) (interface{}, error) {
		return "resolved", nil
	}

	for range 3 {
		_, _ = fm.Middleware(ctx, resolver)
	}

	assert.Equal(t, 3.0, testutil.ToFloat64(m.DeprecatedFieldsTotal.WithLabelValues("GetAccount", "oldField")))
}
