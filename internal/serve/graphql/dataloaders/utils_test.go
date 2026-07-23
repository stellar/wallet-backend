package dataloaders

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestParseStateChangeIDs(t *testing.T) {
	t.Run("valid IDs parse successfully", func(t *testing.T) {
		toIDs, opIDs, scIDs, err := parseStateChangeIDs([]string{"100-200-1", "300-400-2"})
		require.NoError(t, err)
		assert.Equal(t, []int64{100, 300}, toIDs)
		assert.Equal(t, []int64{200, 400}, opIDs)
		assert.Equal(t, []int64{1, 2}, scIDs)
	})

	t.Run("wrong number of parts returns BAD_USER_INPUT", func(t *testing.T) {
		_, _, _, err := parseStateChangeIDs([]string{"100-200"})
		require.Error(t, err)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})

	t.Run("non-numeric to_id returns BAD_USER_INPUT", func(t *testing.T) {
		_, _, _, err := parseStateChangeIDs([]string{"abc-200-1"})
		require.Error(t, err)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})

	t.Run("non-numeric operation_id returns BAD_USER_INPUT", func(t *testing.T) {
		_, _, _, err := parseStateChangeIDs([]string{"100-abc-1"})
		require.Error(t, err)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})

	t.Run("non-numeric state_change_id returns BAD_USER_INPUT", func(t *testing.T) {
		_, _, _, err := parseStateChangeIDs([]string{"100-200-abc"})
		require.Error(t, err)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})
}
