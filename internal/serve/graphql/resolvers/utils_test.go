package resolvers

import (
	"testing"
	"time"

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

func TestParseRootPaginationParams(t *testing.T) {
	t.Run("rejects first above max with BAD_USER_INPUT", func(t *testing.T) {
		first := maxRootPageLimit + 1
		_, err := parseRootPaginationParams(&first, nil, nil, nil, CursorTypeComposite)
		require.Error(t, err)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		assert.Contains(t, gqlErr.Message, "first must be less than or equal to 100")
	})

	t.Run("rejects a first at math.MaxInt32 with BAD_USER_INPUT (not an overflow)", func(t *testing.T) {
		first := int32(2147483647)
		_, err := parseRootPaginationParams(&first, nil, nil, nil, CursorTypeComposite)
		require.Error(t, err)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})

	t.Run("rejects last above max with BAD_USER_INPUT", func(t *testing.T) {
		last := maxRootPageLimit + 1
		_, err := parseRootPaginationParams(nil, nil, &last, nil, CursorTypeStateChange)
		require.Error(t, err)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		assert.Contains(t, gqlErr.Message, "last must be less than or equal to 100")
	})

	t.Run("accepts first at max", func(t *testing.T) {
		first := maxRootPageLimit
		params, err := parseRootPaginationParams(&first, nil, nil, nil, CursorTypeComposite)
		require.NoError(t, err)
		require.NotNil(t, params.Limit)
		assert.Equal(t, maxRootPageLimit, *params.Limit)
	})
}

func TestBuildRootTimeRange(t *testing.T) {
	t.Run("both nil defaults to a 7-day window ending now", func(t *testing.T) {
		before := time.Now()
		tr, err := buildRootTimeRange(nil, nil)
		after := time.Now()
		require.NoError(t, err)
		require.NotNil(t, tr)
		require.NotNil(t, tr.Since)
		require.NotNil(t, tr.Until)

		assert.True(t, tr.Until.After(before) || tr.Until.Equal(before))
		assert.True(t, tr.Until.Before(after) || tr.Until.Equal(after))
		assert.WithinDuration(t, before.Add(-defaultRootQueryWindow), *tr.Since, time.Second)
	})

	t.Run("explicit since only is open-ended on until", func(t *testing.T) {
		since := time.Now().Add(-30 * 24 * time.Hour)
		tr, err := buildRootTimeRange(&since, nil)
		require.NoError(t, err)
		require.NotNil(t, tr)
		assert.Equal(t, since, *tr.Since)
		assert.Nil(t, tr.Until)
	})

	t.Run("explicit until only is open-ended on since", func(t *testing.T) {
		until := time.Now()
		tr, err := buildRootTimeRange(nil, &until)
		require.NoError(t, err)
		require.NotNil(t, tr)
		assert.Nil(t, tr.Since)
		assert.Equal(t, until, *tr.Until)
	})

	t.Run("until before since is rejected as BAD_USER_INPUT", func(t *testing.T) {
		since := time.Now()
		until := since.Add(-1 * time.Hour)
		_, err := buildRootTimeRange(&since, &until)
		requireBadUserInput(t, err)
	})
}

// requireBadUserInput asserts that err chains to a *gqlerror.Error carrying the BAD_USER_INPUT
// code, the shape the custom error presenter (GQL-05) requires to avoid masking a legitimate
// client-input error behind a generic "internal server error".
func requireBadUserInput(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var gqlErr *gqlerror.Error
	require.ErrorAs(t, err, &gqlErr)
	assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
}

func TestValidatePaginationParamsReturnsBadUserInput(t *testing.T) {
	first := int32(1)
	last := int32(1)
	after := "cursor"
	before := "cursor"
	zero := int32(0)
	negative := int32(-1)

	requireBadUserInput(t, validatePaginationParams(&first, nil, &last, nil))
	requireBadUserInput(t, validatePaginationParams(nil, &after, nil, &before))
	requireBadUserInput(t, validatePaginationParams(&zero, nil, nil, nil))
	requireBadUserInput(t, validatePaginationParams(nil, nil, &zero, nil))
	requireBadUserInput(t, validatePaginationParams(&negative, nil, nil, nil))
	requireBadUserInput(t, validatePaginationParams(nil, nil, &negative, nil))
	requireBadUserInput(t, validatePaginationParams(&first, nil, nil, &before))
	requireBadUserInput(t, validatePaginationParams(nil, &after, &last, nil))
}

func TestDecodeInt64CursorReturnsBadUserInput(t *testing.T) {
	t.Run("invalid base64 returns BAD_USER_INPUT", func(t *testing.T) {
		bad := "not-valid-base64!!!"
		_, err := decodeInt64Cursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("non-numeric decoded value returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("not-a-number")
		_, err := decodeInt64Cursor(&bad)
		requireBadUserInput(t, err)
	})
}

func TestDecodeStringCursorReturnsBadUserInput(t *testing.T) {
	bad := "not-valid-base64!!!"
	_, err := decodeStringCursor(&bad)
	requireBadUserInput(t, err)
}

func TestParseCompositeCursorReturnsBadUserInput(t *testing.T) {
	t.Run("invalid base64 returns BAD_USER_INPUT", func(t *testing.T) {
		bad := "not-valid-base64!!!"
		_, err := parseCompositeCursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("wrong number of parts returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("only-one-part")
		_, err := parseCompositeCursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("non-numeric ledger_created_at returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("notanumber:5")
		_, err := parseCompositeCursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("non-numeric id returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("5:notanumber")
		_, err := parseCompositeCursor(&bad)
		requireBadUserInput(t, err)
	})
}

func TestParseStateChangeCursorReturnsBadUserInput(t *testing.T) {
	t.Run("invalid base64 returns BAD_USER_INPUT", func(t *testing.T) {
		bad := "not-valid-base64!!!"
		_, err := parseStateChangeCursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("wrong number of parts returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("only:three:parts")
		_, err := parseStateChangeCursor(&bad)
		requireBadUserInput(t, err)
	})

	t.Run("non-numeric part returns BAD_USER_INPUT", func(t *testing.T) {
		bad := encodeCursor("5:6:notanumber:8")
		_, err := parseStateChangeCursor(&bad)
		requireBadUserInput(t, err)
	})
}

// TestParsePaginationParamsMalformedCursorIsBadUserInput locks in the GQL-11 interplay with GQL-05:
// a malformed client cursor must still surface as BAD_USER_INPUT after being wrapped by
// parsePaginationParams's fmt.Errorf("...: %w", err) — the presenter's errors.As must find the
// coded error through the wrap chain rather than it being masked as an internal error.
func TestParsePaginationParamsMalformedCursorIsBadUserInput(t *testing.T) {
	first := int32(1)
	bad := "not-valid-base64!!!"

	_, err := parsePaginationParams(&first, &bad, nil, nil, CursorTypeComposite)
	requireBadUserInput(t, err)

	_, err = parsePaginationParams(&first, &bad, nil, nil, CursorTypeStateChange)
	requireBadUserInput(t, err)

	_, err = parsePaginationParams(&first, &bad, nil, nil, CursorTypeString)
	requireBadUserInput(t, err)

	_, err = parsePaginationParams(&first, &bad, nil, nil, CursorTypeInt64)
	requireBadUserInput(t, err)
}
