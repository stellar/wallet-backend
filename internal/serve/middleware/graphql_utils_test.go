// Package middleware provides HTTP middleware components for the wallet backend server.
// This file contains tests for GraphQL utility functions.
package middleware

import (
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestGetOperationIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		oc       *graphql.OperationContext
		expected string
	}{
		{
			name:     "nil operation context returns unnamed",
			oc:       nil,
			expected: "<unnamed>",
		},
		{
			name: "explicit operation name is preferred",
			oc: &graphql.OperationContext{
				OperationName: "GetAccount",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "accountByAddress"},
					},
				},
			},
			expected: "GetAccount",
		},
		{
			name: "anonymous query returns first field name",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "accountByAddress"},
					},
				},
			},
			expected: "accountByAddress",
		},
		{
			name: "anonymous query with multiple fields returns first field name",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "transactions"},
						&ast.Field{Name: "accountByAddress"},
					},
				},
			},
			expected: "transactions",
		},
		{
			name: "empty selection set returns unnamed",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{},
				},
			},
			expected: "<unnamed>",
		},
		{
			name: "nil operation returns unnamed",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation:     nil,
			},
			expected: "<unnamed>",
		},
		{
			name: "selection set with fragment spread (non-field) skips to unnamed",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.FragmentSpread{Name: "AccountFields"},
					},
				},
			},
			expected: "<unnamed>",
		},
		{
			name: "selection set with inline fragment then field returns field name",
			oc: &graphql.OperationContext{
				OperationName: "",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.InlineFragment{},
						&ast.Field{Name: "balancesByAccountAddress"},
					},
				},
			},
			expected: "balancesByAccountAddress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetOperationIdentifier(tt.oc)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFieldPath(t *testing.T) {
	tests := []struct {
		name     string
		fc       *graphql.FieldContext
		expected string
	}{
		{
			name:     "nil field context returns empty string",
			fc:       nil,
			expected: "",
		},
		{
			name: "single field returns field name",
			fc: &graphql.FieldContext{
				Field: graphql.CollectedField{
					Field: &ast.Field{Name: "accountByAddress", Alias: "accountByAddress"},
				},
			},
			expected: "accountByAddress",
		},
		{
			name: "nested field returns full path",
			fc: &graphql.FieldContext{
				Parent: &graphql.FieldContext{
					Field: graphql.CollectedField{
						Field: &ast.Field{Name: "accountByAddress", Alias: "accountByAddress"},
					},
				},
				Field: graphql.CollectedField{
					Field: &ast.Field{Name: "transactions", Alias: "transactions"},
				},
			},
			expected: "accountByAddress.transactions",
		},
		{
			name: "deeply nested field returns full path",
			fc: &graphql.FieldContext{
				Parent: &graphql.FieldContext{
					Parent: &graphql.FieldContext{
						Field: graphql.CollectedField{
							Field: &ast.Field{Name: "accountByAddress", Alias: "accountByAddress"},
						},
					},
					Field: graphql.CollectedField{
						Field: &ast.Field{Name: "transactions", Alias: "transactions"},
					},
				},
				Field: graphql.CollectedField{
					Field: &ast.Field{Name: "hash", Alias: "hash"},
				},
			},
			expected: "accountByAddress.transactions.hash",
		},
		{
			name: "field context with index excludes index from path",
			fc: &graphql.FieldContext{
				Parent: &graphql.FieldContext{
					Parent: &graphql.FieldContext{
						Parent: &graphql.FieldContext{
							Field: graphql.CollectedField{
								Field: &ast.Field{Name: "accountByAddress", Alias: "accountByAddress"},
							},
						},
						Field: graphql.CollectedField{
							Field: &ast.Field{Name: "transactions", Alias: "transactions"},
						},
					},
					Index: intPtr(0), // array index element - Path() only includes Index, not Field
				},
				Field: graphql.CollectedField{
					Field: &ast.Field{Name: "hash", Alias: "hash"},
				},
			},
			expected: "accountByAddress.transactions.hash",
		},
		{
			name: "field with alias uses alias",
			fc: &graphql.FieldContext{
				Parent: &graphql.FieldContext{
					Field: graphql.CollectedField{
						Field: &ast.Field{Name: "accountByAddress", Alias: "account"},
					},
				},
				Field: graphql.CollectedField{
					Field: &ast.Field{Name: "address", Alias: "addr"},
				},
			},
			expected: "account.addr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFieldPath(tt.fc)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// intPtr is a helper function to create a pointer to an int
func intPtr(i int) *int {
	return &i
}
