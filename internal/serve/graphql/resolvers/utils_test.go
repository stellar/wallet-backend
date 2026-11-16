package resolvers

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
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
			// The data entry name has its own column.
			{"name", "data_entry_name"},
			// Fields extracted from the key_value JSONB blob.
			{"homeDomain", "key_value"},
			{"oldHomeDomain", "key_value"},
			{"newHomeDomain", "key_value"},
			{"value", "key_value"},
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
		got := getDBColumns(types.StateChange{}, collectedFields("value", "oldValue", "newValue", "expirationLedger"))
		assert.Equal(t, []string{"key_value"}, got)

		got = getDBColumns(types.StateChange{}, collectedFields("homeDomain", "oldHomeDomain", "newHomeDomain"))
		assert.Equal(t, []string{"key_value"}, got)
	})

	t.Run("preserves order across distinct columns", func(t *testing.T) {
		got := getDBColumns(types.StateChange{}, collectedFields("category", "tokenId", "category", "amount"))
		assert.Equal(t, []string{"state_change_category", "token_id", "amount"}, got)
	})

	t.Run("memo also selects memo_type", func(t *testing.T) {
		// Decoding the memo value needs its type, so memo pulls both columns; memoType alone
		// maps through its json tag.
		assert.Equal(t, []string{"memo", "memo_type"}, getDBColumns(types.Transaction{}, collectedFields("memo")))
		assert.Equal(t, []string{"memo_type"}, getDBColumns(types.Transaction{}, collectedFields("memoType")))
		assert.Equal(t, []string{"memo_type", "memo"}, getDBColumns(types.Transaction{}, collectedFields("memoType", "memo")))
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

func TestEncodeMemoFilter(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	hashBytes, err := hex.DecodeString(hash)
	require.NoError(t, err)

	t.Run("nil filter", func(t *testing.T) {
		got, err := encodeMemoFilter(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	valid := []struct {
		name   string
		filter generated.MemoFilter
		want   data.MemoFilter
	}{
		{"text", generated.MemoFilter{Type: generated.MemoTypeText, Value: "hello"}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoText), Value: []byte("hello")}},
		{"text at the 28-byte limit", generated.MemoFilter{Type: generated.MemoTypeText, Value: strings.Repeat("a", 28)}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoText), Value: []byte(strings.Repeat("a", 28))}},
		{"id", generated.MemoFilter{Type: generated.MemoTypeID, Value: "258"}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoId), Value: []byte{0, 0, 0, 0, 0, 0, 1, 2}}},
		{"id max uint64", generated.MemoFilter{Type: generated.MemoTypeID, Value: "18446744073709551615"}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoId), Value: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}}},
		{"hash", generated.MemoFilter{Type: generated.MemoTypeHash, Value: hash}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoHash), Value: hashBytes}},
		{"return", generated.MemoFilter{Type: generated.MemoTypeReturn, Value: hash}, data.MemoFilter{Type: int16(xdr.MemoTypeMemoReturn), Value: hashBytes}},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeMemoFilter(&tc.filter)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.want, *got)
		})
	}

	invalid := []struct {
		name   string
		filter generated.MemoFilter
	}{
		{"text over 28 bytes", generated.MemoFilter{Type: generated.MemoTypeText, Value: strings.Repeat("a", 29)}},
		{"id not a number", generated.MemoFilter{Type: generated.MemoTypeID, Value: "abc"}},
		{"id negative", generated.MemoFilter{Type: generated.MemoTypeID, Value: "-1"}},
		{"id overflows uint64", generated.MemoFilter{Type: generated.MemoTypeID, Value: "18446744073709551616"}},
		{"hash too short", generated.MemoFilter{Type: generated.MemoTypeHash, Value: "abcd"}},
		{"hash not hex", generated.MemoFilter{Type: generated.MemoTypeHash, Value: strings.Repeat("zz", 32)}},
		{"return too long", generated.MemoFilter{Type: generated.MemoTypeReturn, Value: hash + "00"}},
		{"none", generated.MemoFilter{Type: generated.MemoTypeNone, Value: ""}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeMemoFilter(&tc.filter)
			require.Error(t, err)
			assert.Nil(t, got)
			var gqlErr *gqlerror.Error
			require.ErrorAs(t, err, &gqlErr)
			assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		})
	}
}
