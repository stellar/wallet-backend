package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// MaxQueryDepth bounds how deeply nested a single GraphQL operation's selection set may be.
// gqlgen has no built-in depth limit, and the complexity limit does not substitute for one: a
// chain of first:1 connections costs only ~1 per level regardless of how deep it goes, so an
// attacker can nest arbitrarily deep while staying under the complexity budget. Freighter's
// deepest legitimate query (e.g. accountByAddress -> transactions -> edges -> node ->
// operations/stateChanges -> a BaseStateChange implementer field) nests about 6-8 levels deep, so
// 15 leaves generous headroom while still rejecting pathological deeply-nested queries.
const MaxQueryDepth = 15

// errQueryTooDeep is the extensions "code" returned when an operation exceeds MaxQueryDepth.
const errQueryTooDeep = "QUERY_TOO_DEEP"

// DepthLimit is a gqlgen HandlerExtension + OperationContextMutator (the same shape as gqlgen's
// own extension.ComplexityLimit) that rejects operations whose selection set nests deeper than
// MaxDepth. It resolves fragment spreads against the query document's fragments so depth can't be
// hidden behind a fragment indirection, and guards against fragment cycles.
type DepthLimit struct {
	MaxDepth int
}

var _ interface {
	graphql.HandlerExtension
	graphql.OperationContextMutator
} = DepthLimit{}

func (DepthLimit) ExtensionName() string { return "DepthLimit" }

func (DepthLimit) Validate(_ graphql.ExecutableSchema) error { return nil }

// MutateOperationContext rejects the operation with a QUERY_TOO_DEEP error if it nests deeper
// than MaxDepth (or MaxQueryDepth, if MaxDepth is unset).
func (d DepthLimit) MutateOperationContext(_ context.Context, opCtx *graphql.OperationContext) *gqlerror.Error {
	maxDepth := d.MaxDepth
	if maxDepth <= 0 {
		maxDepth = MaxQueryDepth
	}

	op := opCtx.Doc.Operations.ForName(opCtx.OperationName)
	if op == nil {
		return nil
	}

	depth := selectionSetDepth(op.SelectionSet, opCtx.Doc.Fragments, map[string]bool{}, 0)
	if depth > maxDepth {
		err := gqlerror.Errorf("operation has depth %d, which exceeds the limit of %d", depth, maxDepth)
		err.Extensions = map[string]interface{}{"code": errQueryTooDeep}
		return err
	}
	return nil
}

// selectionSetDepth returns the maximum nesting depth reachable from set, starting at depth.
// Every Field adds one level (whether or not it has children — a leaf scalar still counts as the
// level it was selected at); InlineFragments are transparent (they don't add a level, matching
// how they read in the query); FragmentSpreads are resolved against fragments and otherwise
// treated the same as an InlineFragment.
//
// visitedFragments tracks fragment names currently on the recursion stack, guarding against a
// fragment that (directly or transitively) spreads itself: rather than recursing forever, a
// repeated name is simply skipped.
func selectionSetDepth(set ast.SelectionSet, fragments ast.FragmentDefinitionList, visitedFragments map[string]bool, depth int) int {
	maxDepth := depth
	for _, sel := range set {
		var childDepth int
		switch sel := sel.(type) {
		case *ast.Field:
			childDepth = selectionSetDepth(sel.SelectionSet, fragments, visitedFragments, depth+1)
		case *ast.InlineFragment:
			childDepth = selectionSetDepth(sel.SelectionSet, fragments, visitedFragments, depth)
		case *ast.FragmentSpread:
			if visitedFragments[sel.Name] {
				continue
			}
			def := fragments.ForName(sel.Name)
			if def == nil {
				continue
			}
			visitedFragments[sel.Name] = true
			childDepth = selectionSetDepth(def.SelectionSet, fragments, visitedFragments, depth)
			delete(visitedFragments, sel.Name)
		default:
			continue
		}
		if childDepth > maxDepth {
			maxDepth = childDepth
		}
	}
	return maxDepth
}
