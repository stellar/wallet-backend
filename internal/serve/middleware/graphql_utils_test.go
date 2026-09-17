package middleware

import (
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/vektah/gqlparser/v2/ast"
)

// objectSubField builds a sub-field carrying a selection set of its own, i.e. an object-typed one.
func objectSubField(name string) *ast.Field {
	return &ast.Field{Name: name, SelectionSet: ast.SelectionSet{&ast.Field{Name: "edges"}}}
}

// rootFieldQuery builds an operation context whose single root field selects the given sub-fields.
func rootFieldQuery(rootField string, subFields ...ast.Selection) *graphql.OperationContext {
	return &graphql.OperationContext{
		Operation: &ast.OperationDefinition{
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: rootField, SelectionSet: ast.SelectionSet(subFields)},
			},
		},
	}
}

func TestGetOperationIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		oc       *graphql.OperationContext
		expected string
	}{
		{
			name:     "account balances query is identified by its sub-field",
			oc:       rootFieldQuery("accountByAddress", objectSubField("balances")),
			expected: "accountByAddress.balances",
		},
		{
			name:     "account transactions query is identified by its sub-field",
			oc:       rootFieldQuery("accountByAddress", objectSubField("transactions")),
			expected: "accountByAddress.transactions",
		},
		{
			name:     "account operations query is identified by its sub-field",
			oc:       rootFieldQuery("accountByAddress", objectSubField("operations")),
			expected: "accountByAddress.operations",
		},
		{
			name:     "account state changes query is identified by its sub-field",
			oc:       rootFieldQuery("accountByAddress", objectSubField("stateChanges")),
			expected: "accountByAddress.stateChanges",
		},
		{
			name:     "transaction operations query is identified by its sub-field",
			oc:       rootFieldQuery("transactionByHash", objectSubField("operations")),
			expected: "transactionByHash.operations",
		},
		{
			name:     "connection root field is identified by its edges sub-field",
			oc:       rootFieldQuery("blendPools", objectSubField("edges")),
			expected: "blendPools.edges",
		},
		{
			name: "scalar sub-fields before the object sub-field are skipped",
			oc: rootFieldQuery("accountByAddress",
				&ast.Field{Name: "address"},
				&ast.Field{Name: "id"},
				objectSubField("balances"),
			),
			expected: "accountByAddress.balances",
		},
		{
			name: "scalar-only selection set keeps the bare root field name",
			oc: rootFieldQuery("accountByAddress",
				&ast.Field{Name: "address"},
				&ast.Field{Name: "id"},
			),
			expected: "accountByAddress",
		},
		{
			name: "sub-selection of only fragments keeps the bare root field name",
			oc: rootFieldQuery("accountByAddress",
				&ast.FragmentSpread{Name: "AccountFields"},
			),
			expected: "accountByAddress",
		},
		{
			name: "first root field wins over a later root field with an object sub-field",
			oc: &graphql.OperationContext{
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "operationById"},
						&ast.Field{Name: "accountByAddress", SelectionSet: ast.SelectionSet{objectSubField("balances")}},
					},
				},
			},
			expected: "operationById",
		},
		{
			name:     "nil operation context returns unnamed",
			oc:       nil,
			expected: "<unnamed>",
		},
		{
			name: "explicit operation name is ignored in favor of root field name",
			oc: &graphql.OperationContext{
				OperationName: "GetAccount",
				Operation: &ast.OperationDefinition{
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "accountByAddress"},
					},
				},
			},
			expected: "accountByAddress",
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
						&ast.Field{Name: "accountByAddress"},
					},
				},
			},
			expected: "accountByAddress",
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
			name: "field with alias uses actual field name not alias",
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
			expected: "accountByAddress.address",
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
