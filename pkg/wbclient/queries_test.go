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

// TestStateChangeFragmentRequestsAllFields is the state-change analogue of
// TestBalanceFragmentRequestsAllFields: it guards against a field being added to a concrete
// state-change struct (and the schema + resolver) without being added to the query fragment,
// which would leave the SDK silently unmarshaling it to its zero value. For each concrete type
// it asserts the inline fragment requests every JSON key the struct declares, skipping the
// shared base fields that come from the fragment prefix. Aliased fields (e.g. balanceTokenId)
// are matched by their alias, which is exactly the struct's JSON tag.
func TestStateChangeFragmentRequestsAllFields(t *testing.T) {
	shared := map[string]bool{
		"category": true, "reason": true, "ingestedAt": true,
		"ledgerCreatedAt": true, "ledgerNumber": true,
	}

	variants := []struct {
		typeName string
		sample   any
	}{
		{"BalanceChange", types.BalanceChange{}},
		{"AccountCreatedChange", types.AccountCreatedChange{}},
		{"AccountMergedChange", types.AccountMergedChange{}},
		{"SignerAddedChange", types.SignerAddedChange{}},
		{"SignerUpdatedChange", types.SignerUpdatedChange{}},
		{"SignerRemovedChange", types.SignerRemovedChange{}},
		{"ThresholdChange", types.ThresholdChange{}},
		{"AccountFlagsChange", types.AccountFlagsChange{}},
		{"HomeDomainSetChange", types.HomeDomainSetChange{}},
		{"HomeDomainUpdatedChange", types.HomeDomainUpdatedChange{}},
		{"HomeDomainClearedChange", types.HomeDomainClearedChange{}},
		{"DataEntryAddedChange", types.DataEntryAddedChange{}},
		{"DataEntryUpdatedChange", types.DataEntryUpdatedChange{}},
		{"DataEntryRemovedChange", types.DataEntryRemovedChange{}},
		{"AllowanceChange", types.AllowanceChange{}},
		{"TrustlineAddedChange", types.TrustlineAddedChange{}},
		{"TrustlineUpdatedChange", types.TrustlineUpdatedChange{}},
		{"TrustlineRemovedChange", types.TrustlineRemovedChange{}},
		{"BalanceAuthorizationChange", types.BalanceAuthorizationChange{}},
		{"BlendSupplyChange", types.BlendSupplyChange{}},
		{"BlendCollateralChange", types.BlendCollateralChange{}},
		{"BlendDebtChange", types.BlendDebtChange{}},
		{"BlendAuctionChange", types.BlendAuctionChange{}},
		{"BlendEmissionsClaimChange", types.BlendEmissionsClaimChange{}},
		{"BlendBackstopEmissionsClaimChange", types.BlendBackstopEmissionsClaimChange{}},
		{"BlendBackstopChange", types.BlendBackstopChange{}},
		{"BlendBackstopQueueChange", types.BlendBackstopQueueChange{}},
	}

	for _, v := range variants {
		block := inlineFragmentBlock(t, stateChangeFragments, v.typeName)
		rt := reflect.TypeOf(v.sample)
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			if field.Anonymous {
				continue // BaseStateChangeFields; its fields are in the shared prefix.
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "" || name == "-" || shared[name] {
				continue
			}
			assert.Contains(t, block, name,
				"stateChangeFragments '... on %s' must request %q, else the SDK never fetches it", v.typeName, name)
		}
	}
}

// TestBlendQueryFieldsRequestAllStructFields guards against the recurring bug where a field is
// added to a Blend response struct (and the GraphQL schema + resolver) but not to the query
// field list: the SDK then never requests it and it silently unmarshals to its zero value.
func TestBlendQueryFieldsRequestAllStructFields(t *testing.T) {
	nested := map[string]bool{
		"reserves": true, "pools": true, "backstop": true, "q4w": true,
		"activeAuctions": true, "bid": true, "lot": true,
	}

	testCases := []struct {
		name       string
		fieldBlock string
		sample     any
	}{
		{"blendPoolFields/BlendPool", blendPoolFields, types.BlendPool{}},
		{"blendReserveFields/BlendReserve", blendReserveFields, types.BlendReserve{}},
		{"blendEarnOptionFields/BlendEarnOption", blendEarnOptionFields, types.BlendEarnOption{}},
		{"blendEarnOptionFields/BlendEarnPoolOption", blendEarnOptionFields, types.BlendEarnPoolOption{}},
		{"blendAccountPositionsFields/BlendAccountPositions", blendAccountPositionsFields, types.BlendAccountPositions{}},
		{"blendAccountPositionsFields/BlendPoolPosition", blendAccountPositionsFields, types.BlendPoolPosition{}},
		{"blendAccountPositionsFields/BlendBackstopPosition", blendAccountPositionsFields, types.BlendBackstopPosition{}},
		{"blendAccountPositionsFields/BlendQ4W", blendAccountPositionsFields, types.BlendQ4W{}},
		{"blendAccountPositionsFields/BlendAuction", blendAccountPositionsFields, types.BlendAuction{}},
		{"blendAccountPositionsFields/BlendAuctionAmount", blendAccountPositionsFields, types.BlendAuctionAmount{}},
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
