package graphql

import (
	"context"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// field builds an *ast.Field with the given nested selection set (nil for a leaf scalar).
func field(name string, sub ast.SelectionSet) *ast.Field {
	return &ast.Field{Name: name, SelectionSet: sub}
}

func TestSelectionSetDepth(t *testing.T) {
	t.Run("flat selection set is depth 1", func(t *testing.T) {
		set := ast.SelectionSet{field("hash", nil), field("ledgerNumber", nil)}
		assert.Equal(t, 1, selectionSetDepth(set, nil, map[string]bool{}, 0))
	})

	t.Run("nested fields add one level each", func(t *testing.T) {
		// accountByAddress(1) { transactions(2) { edges(3) { node(4) { hash(5) } } } } => depth 5
		set := ast.SelectionSet{
			field("accountByAddress", ast.SelectionSet{
				field("transactions", ast.SelectionSet{
					field("edges", ast.SelectionSet{
						field("node", ast.SelectionSet{
							field("hash", nil),
						}),
					}),
				}),
			}),
		}
		assert.Equal(t, 5, selectionSetDepth(set, nil, map[string]bool{}, 0))
	})

	t.Run("inline fragment does not add a level", func(t *testing.T) {
		set := ast.SelectionSet{
			field("node", ast.SelectionSet{
				&ast.InlineFragment{
					SelectionSet: ast.SelectionSet{
						field("balance", nil),
					},
				},
			}),
		}
		// node=1, inline fragment transparent, balance=2
		assert.Equal(t, 2, selectionSetDepth(set, nil, map[string]bool{}, 0))
	})

	t.Run("fragment spread is resolved against the document's fragments and adds no level itself", func(t *testing.T) {
		fragments := ast.FragmentDefinitionList{
			{
				Name: "TxFields",
				SelectionSet: ast.SelectionSet{
					field("hash", nil),
					field("operations", ast.SelectionSet{
						field("id", nil),
					}),
				},
			},
		}
		set := ast.SelectionSet{
			field("transactionByHash", ast.SelectionSet{
				&ast.FragmentSpread{Name: "TxFields"},
			}),
		}
		// transactionByHash=1, spread transparent, operations=2, id=3
		assert.Equal(t, 3, selectionSetDepth(set, fragments, map[string]bool{}, 0))
	})

	t.Run("unresolvable fragment spread is skipped without panicking", func(t *testing.T) {
		set := ast.SelectionSet{
			field("node", ast.SelectionSet{
				&ast.FragmentSpread{Name: "DoesNotExist"},
			}),
		}
		assert.Equal(t, 1, selectionSetDepth(set, ast.FragmentDefinitionList{}, map[string]bool{}, 0))
	})

	t.Run("fragment cycle does not hang", func(t *testing.T) {
		fragments := ast.FragmentDefinitionList{
			{Name: "A", SelectionSet: ast.SelectionSet{&ast.FragmentSpread{Name: "B"}}},
			{Name: "B", SelectionSet: ast.SelectionSet{&ast.FragmentSpread{Name: "A"}}},
		}
		set := ast.SelectionSet{&ast.FragmentSpread{Name: "A"}}

		done := make(chan int, 1)
		go func() {
			done <- selectionSetDepth(set, fragments, map[string]bool{}, 0)
		}()

		select {
		case <-done:
			// Terminated; the exact depth value doesn't matter, only that it returned.
		case <-time.After(2 * time.Second):
			t.Fatal("selectionSetDepth did not terminate on a cyclic fragment spread")
		}
	})

	t.Run("a fragment spread more than once (not a cycle) is still counted each time", func(t *testing.T) {
		fragments := ast.FragmentDefinitionList{
			{Name: "Shared", SelectionSet: ast.SelectionSet{field("value", nil)}},
		}
		set := ast.SelectionSet{
			field("a", ast.SelectionSet{&ast.FragmentSpread{Name: "Shared"}}),
			field("b", ast.SelectionSet{&ast.FragmentSpread{Name: "Shared"}}),
		}
		// a=1/b=1, Shared transparent, value=2
		assert.Equal(t, 2, selectionSetDepth(set, fragments, map[string]bool{}, 0))
	})
}

func TestDepthLimitMutateOperationContext(t *testing.T) {
	newOpCtx := func(depth int) *graphql.OperationContext {
		set := ast.SelectionSet{field("leaf", nil)}
		for i := 0; i < depth-1; i++ {
			set = ast.SelectionSet{field("wrap", set)}
		}
		op := &ast.OperationDefinition{SelectionSet: set}
		return &graphql.OperationContext{
			Doc: &ast.QueryDocument{Operations: ast.OperationList{op}},
		}
	}

	t.Run("operation within the limit is allowed", func(t *testing.T) {
		d := DepthLimit{MaxDepth: 15}
		err := d.MutateOperationContext(context.Background(), newOpCtx(10))
		require.Nil(t, err)
	})

	t.Run("operation exceeding the limit is rejected with QUERY_TOO_DEEP", func(t *testing.T) {
		d := DepthLimit{MaxDepth: 15}
		err := d.MutateOperationContext(context.Background(), newOpCtx(16))
		require.NotNil(t, err)
		assert.Equal(t, errQueryTooDeep, err.Extensions["code"])
		assert.Contains(t, err.Message, "exceeds the limit of 15")
	})

	t.Run("MaxDepth <= 0 falls back to MaxQueryDepth", func(t *testing.T) {
		d := DepthLimit{}
		err := d.MutateOperationContext(context.Background(), newOpCtx(MaxQueryDepth+1))
		require.NotNil(t, err)
		assert.Contains(t, err.Message, "exceeds the limit of 15")
	})

	t.Run("unresolvable operation name returns no error", func(t *testing.T) {
		d := DepthLimit{MaxDepth: 1}
		opCtx := &graphql.OperationContext{
			OperationName: "Missing",
			Doc:           &ast.QueryDocument{Operations: ast.OperationList{{Name: "Other"}}},
		}
		err := d.MutateOperationContext(context.Background(), opCtx)
		require.Nil(t, err)
	})
}
