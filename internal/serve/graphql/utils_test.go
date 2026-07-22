// Tests for GraphQL utilities
// Comprehensive test coverage for CustomErrorPresenter function
package graphql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestCustomErrorPresenter(t *testing.T) {
	ctx := context.Background()

	t.Run("non-gqlerror (e.g. a raw driver/SQL error) is masked as an internal error", func(t *testing.T) {
		regularErr := errors.New("pq: relation \"transactions\" does not exist")
		result := CustomErrorPresenter(ctx, regularErr)

		require.NotNil(t, result)
		assert.Equal(t, "internal server error", result.Message)
		assert.NotContains(t, result.Message, "transactions")
		assert.Equal(t, "INTERNAL_SERVER_ERROR", result.Extensions["code"])
	})

	t.Run("gqlerror without extensions (no code) is masked as an internal error", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "some graphql error",
			Path:    ast.Path{ast.PathName("field")},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "internal server error", result.Message)
		assert.Equal(t, "INTERNAL_SERVER_ERROR", result.Extensions["code"])
	})

	t.Run("gqlerror with an unrecognized code is masked as an internal error", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "some other error",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": "SOME_OTHER_CODE",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "internal server error", result.Message)
		assert.Equal(t, "INTERNAL_SERVER_ERROR", result.Extensions["code"])
		// The masked error still carries the original path, so clients can tell which field failed.
		assert.Equal(t, gqlErr.Path, result.Path)
	})

	t.Run("gqlerror with a known client-safe code passes through unchanged", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "first must be less than or equal to 100",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": "BAD_USER_INPUT",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "first must be less than or equal to 100", result.Message)
		assert.Equal(t, "BAD_USER_INPUT", result.Extensions["code"])
	})

	t.Run("gqlerror wrapped by fmt.Errorf still resolves its code through the chain", func(t *testing.T) {
		inner := &gqlerror.Error{
			Message:    "invalid cursor format",
			Extensions: map[string]interface{}{"code": "BAD_USER_INPUT"},
		}
		// errors.As only finds *gqlerror.Error through an Unwrap chain built by %w, so build one the
		// same way resolver call sites do (fmt.Errorf("...: %w", err)).
		wrapped := fmt.Errorf("parsing pagination params: %w", inner)

		result := CustomErrorPresenter(ctx, wrapped)
		require.NotNil(t, result)
		assert.Equal(t, "BAD_USER_INPUT", result.Extensions["code"])
	})

	for _, code := range []string{
		"INVALID_TRANSACTION_HASH", "INVALID_ADDRESS", "INTERNAL_ERROR",
		"COMPLEXITY_LIMIT_EXCEEDED", "QUERY_TOO_DEEP", "UNAUTHENTICATED", "FORBIDDEN",
		"PERSISTED_QUERY_NOT_FOUND",
	} {
		t.Run("known code "+code+" passes through unchanged", func(t *testing.T) {
			gqlErr := &gqlerror.Error{
				Message:    "a client-facing message",
				Extensions: map[string]interface{}{"code": code},
			}
			result := CustomErrorPresenter(ctx, gqlErr)
			require.NotNil(t, result)
			assert.Equal(t, "a client-facing message", result.Message)
			assert.Equal(t, code, result.Extensions["code"])
		})
	}

	t.Run("APQ hash-only cache miss passes through so the client retries with the full query", func(t *testing.T) {
		// gqlgen's extension.AutomaticPersistedQuery emits this exact code/message when a
		// client sends only a query hash that is not yet cached. If it were masked to
		// INTERNAL_SERVER_ERROR, the client would never learn to resend the full query and
		// every cold-cache APQ request would fail.
		gqlErr := &gqlerror.Error{
			Message:    "PersistedQueryNotFound",
			Extensions: map[string]interface{}{"code": "PERSISTED_QUERY_NOT_FOUND"},
		}
		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "PersistedQueryNotFound", result.Message)
		assert.Equal(t, "PERSISTED_QUERY_NOT_FOUND", result.Extensions["code"])
	})

	t.Run("validation error with unknown field - variable/input path", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("variable"), ast.PathName("input"), ast.PathName("invalidField")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "Unknown field 'invalidField'", result.Message)
	})

	t.Run("validation error with unknown field - other path structure", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("query"), ast.PathName("someField"), ast.PathName("unknownField")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "Unknown field 'unknownField'", result.Message)
	})

	t.Run("validation error with unknown field - single path element", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("badField")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "Unknown field 'badField'", result.Message)
	})

	t.Run("validation error with unknown field - empty path", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// Should remain unchanged since path is empty
		assert.Equal(t, "unknown field", result.Message)
	})

	t.Run("validation error with different message", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "different validation error",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// Should remain unchanged since message is not "unknown field"
		assert.Equal(t, "different validation error", result.Message)
	})

	t.Run("validation error with non-string code is masked (not a recognized code)", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": 123, // non-string code
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// The unknown-field rewrite only fires for "code" == "GRAPHQL_VALIDATION_FAILED" (a string
		// comparison), so a non-string code skips it same as before; but since it's also not a
		// recognized client-safe code, the error is now masked.
		assert.Equal(t, "internal server error", result.Message)
	})

	t.Run("validation error with mixed path types", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("variable"), ast.PathIndex(0), ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// Should use the last element
		assert.Equal(t, "Unknown field 'field'", result.Message)
	})

	t.Run("validation error with variable/input path but only 2 elements", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("variable"), ast.PathName("input")},
			Extensions: map[string]interface{}{
				"code": "GRAPHQL_VALIDATION_FAILED",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// Should use the last element since we don't have 3+ elements
		assert.Equal(t, "Unknown field 'input'", result.Message)
	})
}
