// Package middleware provides HTTP middleware components for the wallet backend server.
// This file provides utility functions for extracting GraphQL operation information.
package middleware

import (
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
)

// GetOperationIdentifier extracts a metrics-safe operation identifier from a GraphQL operation
// context. It derives the identifier from the shape of the query — the first root field name, plus
// the name of that field's first object-typed sub-field when it has one, joined by a "." — and
// never from the client-controlled OperationName: that value becomes a Prometheus label on several
// metric families, and using it directly would give clients control over label cardinality.
//
// The sub-field leg separates queries that share a root field. The four wallet queries all select
// accountByAddress and differ only one level down, so they resolve to accountByAddress.balances,
// accountByAddress.transactions, accountByAddress.operations and accountByAddress.stateChanges.
// Each drives a different set of database reads, and a single merged series averages the slow one
// away. A sub-field counts as object-typed when it carries a selection set of its own, which is
// what skips scalars such as address. Both legs come from the schema, so cardinality stays bounded
// by the root-field/sub-field pairs the schema permits.
func GetOperationIdentifier(oc *graphql.OperationContext) string {
	if oc == nil {
		return "<unnamed>"
	}

	if oc.Operation != nil && len(oc.Operation.SelectionSet) > 0 {
		for _, sel := range oc.Operation.SelectionSet {
			field, ok := sel.(*ast.Field)
			if !ok {
				continue
			}
			if subField := firstObjectSubField(field.SelectionSet); subField != "" {
				return field.Name + "." + subField
			}
			return field.Name
		}
	}

	return "<unnamed>"
}

// firstObjectSubField returns the name of the first object-typed field in a selection set, i.e. the
// first field carrying a selection set of its own. It returns "" when the set holds only scalar
// fields or no fields at all.
func firstObjectSubField(selectionSet ast.SelectionSet) string {
	for _, sel := range selectionSet {
		if field, ok := sel.(*ast.Field); ok && len(field.SelectionSet) > 0 {
			return field.Name
		}
	}

	return ""
}

// GetFieldPath extracts the full field path from a FieldContext, excluding array indices.
// This provides a complete path like "accountByAddress.transactions.hash" instead of just "hash".
// Array indices are excluded to avoid Prometheus cardinality explosion.
// Uses actual field names (not aliases) to prevent cardinality explosion from aliased queries.
func GetFieldPath(fc *graphql.FieldContext) string {
	if fc == nil {
		return ""
	}

	// Walk the parent chain to build the path (leaf to root)
	var parts []string
	for it := fc; it != nil; it = it.Parent {
		// Skip array index nodes
		if it.Index != nil {
			continue
		}
		// Use actual field name (not alias) to avoid cardinality explosion
		if it.Field.Field != nil {
			parts = append(parts, it.Field.Field.Name)
		}
	}

	if len(parts) == 0 {
		return ""
	}

	// Reverse since we walked from leaf to root
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	return strings.Join(parts, ".")
}
