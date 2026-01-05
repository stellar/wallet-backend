// Package middleware provides HTTP middleware components for the wallet backend server.
// This file provides utility functions for extracting GraphQL operation information.
package middleware

import (
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
)

// GetOperationIdentifier extracts a meaningful operation identifier from a GraphQL operation context.
// It prefers the explicit OperationName if provided, otherwise falls back to the first root field name.
// This is useful for metrics and logging when clients send anonymous queries.
func GetOperationIdentifier(oc *graphql.OperationContext) string {
	if oc == nil {
		return "<unnamed>"
	}

	// Prefer explicit operation name if provided
	if oc.OperationName != "" {
		return oc.OperationName
	}

	// Fall back to first root field name from the selection set
	if oc.Operation != nil && len(oc.Operation.SelectionSet) > 0 {
		for _, sel := range oc.Operation.SelectionSet {
			if field, ok := sel.(*ast.Field); ok {
				return field.Name
			}
		}
	}

	return "<unnamed>"
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
