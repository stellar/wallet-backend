// Tests for GraphQL utilities
// Comprehensive test coverage for CustomErrorPresenter function
package graphql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestCustomErrorPresenter(t *testing.T) {
	ctx := context.Background()

	t.Run("non-gqlerror passes through to default presenter", func(t *testing.T) {
		regularErr := errors.New("regular error")
		result := CustomErrorPresenter(ctx, regularErr)

		// The default presenter should convert this to a gqlerror
		require.NotNil(t, result)
		assert.Equal(t, "regular error", result.Message)
	})

	t.Run("gqlerror without extensions passes through", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "some graphql error",
			Path:    ast.Path{ast.PathName("field")},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "some graphql error", result.Message)
	})

	t.Run("gqlerror with non-validation code passes through", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "some other error",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": "SOME_OTHER_CODE",
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		assert.Equal(t, "some other error", result.Message)
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

	t.Run("validation error with non-string code", func(t *testing.T) {
		gqlErr := &gqlerror.Error{
			Message: "unknown field",
			Path:    ast.Path{ast.PathName("field")},
			Extensions: map[string]interface{}{
				"code": 123, // non-string code
			},
		}

		result := CustomErrorPresenter(ctx, gqlErr)
		require.NotNil(t, result)
		// Should remain unchanged since code is not a string
		assert.Equal(t, "unknown field", result.Message)
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
