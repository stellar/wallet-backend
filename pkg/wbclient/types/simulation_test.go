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
