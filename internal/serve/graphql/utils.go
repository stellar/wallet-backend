// GraphQL utilities for error handling and other GraphQL-related functionality
// Contains helper functions for customizing GraphQL behavior
package graphql

import (
	"context"
	"errors"
	"fmt"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// CustomErrorPresenter provides more detailed error messages for GraphQL validation errors
func CustomErrorPresenter(ctx context.Context, err error) *gqlerror.Error {
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		// Check if this is a validation error for unknown field
		if gqlErr.Extensions != nil {
			if code, ok := gqlErr.Extensions["code"].(string); ok && code == "GRAPHQL_VALIDATION_FAILED" {
				// If the error message is "unknown field", provide more specific information
				if gqlErr.Message == "unknown field" && len(gqlErr.Path) > 0 {
					// Extract the field name from the path
					fieldPath := make([]string, len(gqlErr.Path))
					for i, p := range gqlErr.Path {
						fieldPath[i] = fmt.Sprintf("%v", p)
					}

					// Show which field is unknown
					if len(fieldPath) >= 3 && fieldPath[0] == "variable" && fieldPath[1] == "input" {
						unknownField := fieldPath[2]
						gqlErr.Message = fmt.Sprintf("Unknown field '%s'", unknownField)
					} else if len(fieldPath) > 0 {
						unknownField := fieldPath[len(fieldPath)-1]
						gqlErr.Message = fmt.Sprintf("Unknown field '%s'", unknownField)
					}
				}
			}
		}
	}
	return graphql.DefaultErrorPresenter(ctx, err)
}
