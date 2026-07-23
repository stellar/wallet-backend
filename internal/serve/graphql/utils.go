// GraphQL utilities for error handling and other GraphQL-related functionality
// Contains helper functions for customizing GraphQL behavior
package graphql

import (
	"context"
	"errors"
	"fmt"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

// DefaultPageLimit is the implicit page size used when GraphQL pagination args
// are omitted. Execution and complexity accounting must share this value.
const DefaultPageLimit int32 = 50

// clientSafeErrorCodes enumerates every GraphQL error "code" extension the API deliberately
// constructs (or that gqlgen itself assigns) for client consumption: validation/parse errors,
// the complexity and depth limits, and the app's own coded resolver errors (see
// resolvers/errors.go and the badUserInputError/balanceBadUserInputError helpers). Any error
// presented without one of these codes is treated as an internal failure by CustomErrorPresenter
// and masked before it reaches the client.
var clientSafeErrorCodes = map[string]bool{
	"BAD_USER_INPUT":            true,
	"INVALID_TRANSACTION_HASH":  true,
	"INVALID_ADDRESS":           true,
	"INTERNAL_ERROR":            true, // resolvers/account_balances.go: already a sanitized, generic message
	"GRAPHQL_VALIDATION_FAILED": true, // gqlparser query validation (unknown field, bad args, ...)
	"GRAPHQL_PARSE_FAILED":      true, // gqlparser query parse errors
	"COMPLEXITY_LIMIT_EXCEEDED": true, // gqlgen extension.FixedComplexityLimit
	"QUERY_TOO_DEEP":            true, // query depth limit extension
	"UNAUTHENTICATED":           true,
	"FORBIDDEN":                 true,
	"PERSISTED_QUERY_NOT_FOUND": true, // gqlgen extension.AutomaticPersistedQuery: hash-only cache miss; the client must receive it verbatim to retry with the full query
}

// CustomErrorPresenter provides more detailed error messages for GraphQL validation errors, and
// masks any error that isn't one of clientSafeErrorCodes behind a generic message. Resolvers and
// the data layer can return plain Go errors (a SQL driver error, a wrapped fmt.Errorf, ...); left
// unmasked, DefaultErrorPresenter would forward err.Error() verbatim to the client, potentially
// leaking query text, table/column names, or other internal detail (GQL-05).
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

	presented := graphql.DefaultErrorPresenter(ctx, err)

	if code, ok := presented.Extensions["code"].(string); ok && clientSafeErrorCodes[code] {
		return presented
	}

	oc := safeGetOperationContext(ctx)
	fc := graphql.GetFieldContext(ctx)
	log.Ctx(ctx).WithFields(log.F{
		"graphql_operation": middleware.GetOperationIdentifier(oc),
		"graphql_field":     middleware.GetFieldPath(fc),
	}).Errorf("internal GraphQL error: %v", err)

	return &gqlerror.Error{
		Message:    "internal server error",
		Path:       presented.Path,
		Locations:  presented.Locations,
		Extensions: map[string]interface{}{"code": "INTERNAL_SERVER_ERROR"},
	}
}

// safeGetOperationContext returns the request's OperationContext, or nil if none is set.
// graphql.GetOperationContext panics when no context has been attached; errors dispatched before
// DispatchOperation runs (parse errors, validation errors, and operationContextMutator errors such
// as the complexity and depth limits) go through DispatchError, which never attaches one, so the
// presenter must tolerate that instead of panicking on every such error.
func safeGetOperationContext(ctx context.Context) (oc *graphql.OperationContext) {
	defer func() {
		if recover() != nil {
			oc = nil
		}
	}()
	return graphql.GetOperationContext(ctx)
}
