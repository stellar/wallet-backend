package wbclient

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// TestBalanceFragmentRequestsAllFields guards against the recurring bug where a field is
// added to an SDK balance struct (and the GraphQL schema + resolver) but not to the query
// fragment: the SDK then never requests it and it silently unmarshals to its zero value.
// It asserts every concrete balance variant's inline fragment requests each of that
// struct's JSON fields (balance/tokenId/tokenType come from the shared fragment prefix).
func TestBalanceFragmentRequestsAllFields(t *testing.T) {
	shared := map[string]bool{"balance": true, "tokenId": true, "tokenType": true}

	variants := []struct {
		typeName string
		sample   any
	}{
		{"NativeBalance", types.NativeBalance{}},
		{"TrustlineBalance", types.TrustlineBalance{}},
		{"SACBalance", types.SACBalance{}},
		{"SEP41Balance", types.SEP41Balance{}},
		{"LiquidityPoolBalance", types.LiquidityPoolBalance{}},
	}

	for _, v := range variants {
		block := inlineFragmentBlock(t, balanceFragments, v.typeName)
		rt := reflect.TypeOf(v.sample)
		for i := 0; i < rt.NumField(); i++ {
			name := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
			if name == "" || name == "-" || shared[name] {
				continue
			}
			assert.Contains(t, block, name,
				"balanceFragments '... on %s' must request %q, else the SDK never fetches it", v.typeName, name)
		}
	}
}

// TestLendingChangeFragmentRequestsAllFields guards against the same drift as
// TestBalanceFragmentRequestsAllFields, for the LendingChange state-change fragment: every
// LendingChange-specific JSON tag (the aliased tokenId/amount/poolId) must appear in the
// fragment's inline selection.
func TestLendingChangeFragmentRequestsAllFields(t *testing.T) {
	block := inlineFragmentBlock(t, stateChangeFragments, "LendingChange")
	for _, tag := range []string{"lendingTokenId", "lendingAmount", "poolId"} {
		assert.Contains(t, block, tag,
			"stateChangeFragments '... on LendingChange' must request %q, else the SDK never fetches it", tag)
	}
}

// TestBlendQueryFieldsRequestAllStructFields guards against the recurring bug where a field is
// added to a Blend response struct (and the GraphQL schema + resolver) but not to the query
// field list: the SDK then never requests it and it silently unmarshals to its zero value.
func TestBlendQueryFieldsRequestAllStructFields(t *testing.T) {
	nested := map[string]bool{"reserves": true, "pools": true, "backstop": true, "q4w": true}

	testCases := []struct {
		name       string
		fieldBlock string
		sample     any
	}{
		{"blendPoolFields/BlendPool", blendPoolFields, types.BlendPool{}},
		{"blendReserveFields/BlendReserve", blendReserveFields, types.BlendReserve{}},
		{"blendAccountPositionsFields/BlendAccountPositions", blendAccountPositionsFields, types.BlendAccountPositions{}},
		{"blendAccountPositionsFields/BlendPoolPosition", blendAccountPositionsFields, types.BlendPoolPosition{}},
		{"blendAccountPositionsFields/BlendBackstopPosition", blendAccountPositionsFields, types.BlendBackstopPosition{}},
		{"blendAccountPositionsFields/BlendQ4W", blendAccountPositionsFields, types.BlendQ4W{}},
		{"blendReservePositionFields/BlendReservePosition", blendReservePositionFields, types.BlendReservePosition{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rt := reflect.TypeOf(tc.sample)
			for i := 0; i < rt.NumField(); i++ {
				name := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
				if name == "" || name == "-" || nested[name] {
					continue
				}
				assert.Contains(t, tc.fieldBlock, name,
					"%s must request %q, else the SDK never fetches it", tc.name, name)
			}
		})
	}
}

// inlineFragmentBlock returns the body of the `... on <typeName> { ... }` inline fragment,
// matching braces so nested selection sets (e.g. reserves { ... }) don't terminate it early.
func inlineFragmentBlock(t *testing.T, fragment, typeName string) string {
	t.Helper()
	marker := "... on " + typeName + " {"
	start := strings.Index(fragment, marker)
	require.GreaterOrEqual(t, start, 0, "fragment must contain %q", marker)

	body := fragment[start+len(marker):]
	depth := 1
	for i, r := range body {
		switch r {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return body[:i]
			}
		}
	}
	t.Fatalf("inline fragment for %s is not closed", typeName)
	return ""
}
