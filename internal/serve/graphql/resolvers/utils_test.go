package resolvers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestParseAccountPaginationParams(t *testing.T) {
	t.Run("rejects first above max with BAD_USER_INPUT", func(t *testing.T) {
		first := maxAccountPageLimit + 1
		_, err := parseAccountPaginationParams(&first, nil, nil, nil, CursorTypeComposite)
		require.Error(t, err)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		assert.Contains(t, gqlErr.Message, "first must be less than or equal to 100")
	})

	t.Run("rejects last above max with BAD_USER_INPUT", func(t *testing.T) {
		last := maxAccountPageLimit + 1
		_, err := parseAccountPaginationParams(nil, nil, &last, nil, CursorTypeStateChange)
		require.Error(t, err)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		assert.Contains(t, gqlErr.Message, "last must be less than or equal to 100")
	})

	t.Run("accepts first at max", func(t *testing.T) {
		first := maxAccountPageLimit
		params, err := parseAccountPaginationParams(&first, nil, nil, nil, CursorTypeComposite)
		require.NoError(t, err)
		require.NotNil(t, params.Limit)
		assert.Equal(t, maxAccountPageLimit, *params.Limit)
	})

	t.Run("accepts last at max", func(t *testing.T) {
		last := maxAccountPageLimit
		params, err := parseAccountPaginationParams(nil, nil, &last, nil, CursorTypeStateChange)
		require.NoError(t, err)
		require.NotNil(t, params.Limit)
		assert.Equal(t, maxAccountPageLimit, *params.Limit)
	})
}
