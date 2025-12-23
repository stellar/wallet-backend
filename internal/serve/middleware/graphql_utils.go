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
func GetFieldPath(fc *graphql.FieldContext) string {
	if fc == nil {
		return ""
	}

	path := fc.Path()
	var parts []string
	for _, segment := range path {
		// Only include named segments, skip array indices (PathIndex)
		if name, ok := segment.(ast.PathName); ok {
			parts = append(parts, string(name))
		}
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ".")
}
