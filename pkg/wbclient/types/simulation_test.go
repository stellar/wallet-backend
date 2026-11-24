package types

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_UnmarshalSimulatedStateChangeNode_EveryType decodes one node per
// typename and checks it lands in the matching concrete type with its base
// fields set.
func Test_UnmarshalSimulatedStateChangeNode_EveryType(t *testing.T) {
	samples := []SimulatedStateChangeNode{
		&SimulatedBalanceChange{},
		&SimulatedAccountCreatedChange{},
		&SimulatedAccountMergedChange{},
		&SimulatedSignerAddedChange{},
		&SimulatedSignerUpdatedChange{},
		&SimulatedSignerRemovedChange{},
		&SimulatedThresholdChange{},
		&SimulatedAccountFlagsChange{},
		&SimulatedHomeDomainSetChange{},
		&SimulatedHomeDomainUpdatedChange{},
		&SimulatedHomeDomainClearedChange{},
		&SimulatedDataEntryAddedChange{},
		&SimulatedDataEntryUpdatedChange{},
		&SimulatedDataEntryRemovedChange{},
		&SimulatedAllowanceChange{},
		&SimulatedTrustlineAddedChange{},
		&SimulatedTrustlineUpdatedChange{},
		&SimulatedTrustlineRemovedChange{},
		&SimulatedBalanceAuthorizationChange{},
	}

	for _, sample := range samples {
		typeName := reflect.TypeOf(sample).Elem().Name()
		t.Run(typeName, func(t *testing.T) {
			data, err := json.Marshal(map[string]any{
				"__typename":     typeName,
				"category":       "BALANCE",
				"reason":         "DEBIT",
				"accountAddress": "GACCOUNT",
				"operationIndex": 2,
			})
			require.NoError(t, err)

			node, err := UnmarshalSimulatedStateChangeNode(data)
			require.NoError(t, err)
			assert.IsType(t, sample, node)
			assert.Equal(t, StateChangeCategoryBalance, node.GetCategory())
			assert.Equal(t, StateChangeReasonDebit, node.GetReason())
			assert.Equal(t, "GACCOUNT", node.GetAccountAddress())
			require.NotNil(t, node.GetOperationIndex())
			assert.Equal(t, int32(2), *node.GetOperationIndex())
		})
	}
}

func Test_UnmarshalSimulatedStateChangeNode_VariantFields(t *testing.T) {
	node, err := UnmarshalSimulatedStateChangeNode([]byte(`{
		"__typename": "SimulatedAllowanceChange",
		"category": "ALLOWANCE",
		"reason": "UPDATE",
		"accountAddress": "GOWNER",
		"operationIndex": 0,
		"allowanceTokenId": "CTOKEN",
		"spender": "GSPENDER",
		"amount": "500",
		"expirationLedger": 99
	}`))
	require.NoError(t, err)
	allowance, ok := node.(*SimulatedAllowanceChange)
	require.True(t, ok, "expected *SimulatedAllowanceChange, got %T", node)
	assert.Equal(t, "CTOKEN", allowance.TokenID)
	assert.Equal(t, "GSPENDER", allowance.Spender)
	assert.Equal(t, "500", allowance.Amount)
	assert.Equal(t, uint32(99), allowance.ExpirationLedger)

	node, err = UnmarshalSimulatedStateChangeNode([]byte(`{
		"__typename": "SimulatedBalanceAuthorizationChange",
		"category": "BALANCE_AUTHORIZATION",
		"reason": "SET",
		"accountAddress": "GHOLDER",
		"operationIndex": 0,
		"balanceAuthTokenId": "CTOKEN",
		"balanceAuthFlags": ["AUTHORIZED"]
	}`))
	require.NoError(t, err)
	auth, ok := node.(*SimulatedBalanceAuthorizationChange)
	require.True(t, ok, "expected *SimulatedBalanceAuthorizationChange, got %T", node)
	require.NotNil(t, auth.TokenID)
	assert.Equal(t, "CTOKEN", *auth.TokenID)
	assert.Equal(t, []TrustlineFlag{"AUTHORIZED"}, auth.Flags)
}

func Test_UnmarshalSimulatedStateChangeNode_UnknownTypename(t *testing.T) {
	_, err := UnmarshalSimulatedStateChangeNode([]byte(`{"__typename": "NotAStateChange"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown simulated state change type: NotAStateChange")
}

func Test_SimulatedStateChanges_UnmarshalJSON(t *testing.T) {
	const node = `{"__typename": "SimulatedBalanceChange", "category": "BALANCE", "reason": "DEBIT", "accountAddress": "GSENDER", "balanceTokenId": "CUSDC", "amount": "5"}`
	const token = `{"tokenId": "CUSDC", "decimals": 7}`

	t.Run("decodes state changes and tokens", func(t *testing.T) {
		var got SimulatedStateChanges
		require.NoError(t, json.Unmarshal([]byte(`{"latestLedger": 7, "stateChanges": [`+node+`], "tokens": [`+token+`]}`), &got))
		assert.Equal(t, uint32(7), got.LatestLedger)
		require.Len(t, got.StateChanges, 1)
		assert.IsType(t, &SimulatedBalanceChange{}, got.StateChanges[0])
		assert.Equal(t, []SimulatedTokenMetadata{{TokenID: "CUSDC", Decimals: 7}}, got.Tokens)
	})

	t.Run("marshals with the schema's field names", func(t *testing.T) {
		b, err := json.Marshal(SimulatedStateChanges{
			LatestLedger: 7,
			StateChanges: []SimulatedStateChangeNode{},
			Tokens:       []SimulatedTokenMetadata{{TokenID: "CUSDC", Decimals: 7}},
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"latestLedger": 7, "stateChanges": [], "tokens": [`+token+`]}`, string(b))
	})

	t.Run("empty lists decode to empty slices", func(t *testing.T) {
		var got SimulatedStateChanges
		require.NoError(t, json.Unmarshal([]byte(`{"latestLedger": 7, "stateChanges": [], "tokens": []}`), &got))
		assert.NotNil(t, got.StateChanges)
		assert.Empty(t, got.StateChanges)
		assert.NotNil(t, got.Tokens)
		assert.Empty(t, got.Tokens)
	})

	malformed := []struct {
		name, body, wantErr string
	}{
		{"missing stateChanges", `{"tokens": []}`, "missing required stateChanges field"},
		{"null stateChanges", `{"stateChanges": null, "tokens": []}`, "missing required stateChanges field"},
		{"null state change element", `{"stateChanges": [null], "tokens": []}`, "simulated state change at index 0 is null"},
		{"missing tokens", `{"stateChanges": []}`, "missing required tokens field"},
		{"null tokens", `{"stateChanges": [], "tokens": null}`, "missing required tokens field"},
		{"null token element", `{"stateChanges": [], "tokens": [` + token + `, null]}`, "simulated token at index 1 is null"},
	}
	for _, tc := range malformed {
		t.Run(tc.name+" is rejected", func(t *testing.T) {
			var got SimulatedStateChanges
			err := json.Unmarshal([]byte(tc.body), &got)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
