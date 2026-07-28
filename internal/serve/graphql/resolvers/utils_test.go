package resolvers

import (
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// collectedFields wraps GraphQL field names into the CollectedField shape getDBColumns consumes.
func collectedFields(names ...string) []graphql.CollectedField {
	fields := make([]graphql.CollectedField, len(names))
	for i, n := range names {
		fields[i] = graphql.CollectedField{Field: &ast.Field{Name: n}}
	}
	return fields
}

func TestGetDBColumns(t *testing.T) {
	t.Run("renamed and derived field mappings", func(t *testing.T) {
		cases := []struct {
			field string
			want  string
		}{
			// GraphQL field names that differ from the model json tags.
			{"category", "state_change_category"},
			{"reason", "state_change_reason"},
			{"signerAddress", "signer_account_id"},
			{"creatorAddress", "creator_account_id"},
			{"destinationAddress", "destination_account_id"},
			{"spender", "spender_account_id"},
			// Fields derived from a flattened old/new column pair.
			{"oldWeight", "signer_weight_old"},
			{"newWeight", "signer_weight_new"},
			{"oldThreshold", "threshold_old"},
			{"newThreshold", "threshold_new"},
			{"oldLimit", "trustline_limit_old"},
			{"limit", "trustline_limit_new"},
			{"newLimit", "trustline_limit_new"},
			// Fields extracted from the key_value JSONB blob.
			{"oldHomeDomain", "key_value"},
			{"newHomeDomain", "key_value"},
			{"name", "key_value"},
			{"oldValue", "key_value"},
			{"newValue", "key_value"},
			{"expirationLedger", "key_value"},
			// A field whose name already matches its json tag falls through to the tag→db map.
			{"tokenId", "token_id"},
			{"amount", "amount"},
			{"threshold", "threshold"},
		}
		for _, tc := range cases {
			t.Run(tc.field, func(t *testing.T) {
				got := getDBColumns(types.StateChange{}, collectedFields(tc.field))
				assert.Equal(t, []string{tc.want}, got)
			})
		}
	})

	t.Run("fields sharing a backing column collapse to one", func(t *testing.T) {
		// A repeated column in the SELECT list would break positional row scanning, so the
		// several fields extracted from key_value must dedupe to a single column.
		got := getDBColumns(types.StateChange{}, collectedFields("name", "oldValue", "newValue", "expirationLedger"))
		assert.Equal(t, []string{"key_value"}, got)

		got = getDBColumns(types.StateChange{}, collectedFields("oldHomeDomain", "newHomeDomain"))
		assert.Equal(t, []string{"key_value"}, got)
	})

	t.Run("preserves order across distinct columns", func(t *testing.T) {
		got := getDBColumns(types.StateChange{}, collectedFields("category", "tokenId", "category", "amount"))
		assert.Equal(t, []string{"state_change_category", "token_id", "amount"}, got)
	})

	t.Run("unknown fields are skipped", func(t *testing.T) {
		got := getDBColumns(types.StateChange{}, collectedFields("thisFieldDoesNotExist", "amount"))
		assert.Equal(t, []string{"amount"}, got)
	})
}

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
