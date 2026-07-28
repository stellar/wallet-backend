package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UnmarshalStateChangeNode_UnknownTypename(t *testing.T) {
	_, err := UnmarshalStateChangeNode([]byte(`{"__typename": "NotAStateChange"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown state change type: NotAStateChange")
}

func Test_UnmarshalStateChangeNode_BalanceChange(t *testing.T) {
	muxed := "12345"
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "BalanceChange",
		"category": "BALANCE",
		"reason": "CREDIT",
		"ledgerNumber": 42,
		"balanceTokenId": "native",
		"amount": "100",
		"toMuxedId": "12345"
	}`))
	require.NoError(t, err)

	bc, ok := node.(*BalanceChange)
	require.True(t, ok, "expected *BalanceChange, got %T", node)
	assert.Equal(t, StateChangeCategoryBalance, bc.GetCategory())
	assert.Equal(t, StateChangeReasonCredit, bc.GetReason())
	assert.Equal(t, uint32(42), bc.GetLedgerNumber())
	assert.Equal(t, "native", bc.TokenID)
	assert.Equal(t, "100", bc.Amount)
	require.NotNil(t, bc.ToMuxedID)
	assert.Equal(t, muxed, *bc.ToMuxedID)
}

// Test_UnmarshalStateChangeNode_AccountCreated covers both ACCOUNT/CREATE shapes that
// AccountCreatedChange serves: a classic account funded by a G-address, and a contract
// deployment whose account is the deployed C-address.
func Test_UnmarshalStateChangeNode_AccountCreated(t *testing.T) {
	created, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AccountCreatedChange",
		"category": "ACCOUNT",
		"reason": "CREATE",
		"creatorAddress": "GFUNDER"
	}`))
	require.NoError(t, err)
	ac, ok := created.(*AccountCreatedChange)
	require.True(t, ok, "expected *AccountCreatedChange, got %T", created)
	assert.Equal(t, "GFUNDER", ac.CreatorAddress)

	deployed, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AccountCreatedChange",
		"category": "ACCOUNT",
		"reason": "CREATE",
		"creatorAddress": "GDEPLOYER"
	}`))
	require.NoError(t, err)
	cd, ok := deployed.(*AccountCreatedChange)
	require.True(t, ok, "expected *AccountCreatedChange, got %T", deployed)
	assert.Equal(t, "GDEPLOYER", cd.CreatorAddress)
}

// Test_UnmarshalStateChangeNode_SignerUpdated verifies both required weights decode,
// including the master-key case where the prior weight is 0 (a locked master key).
func Test_UnmarshalStateChangeNode_SignerUpdated(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "SignerUpdatedChange",
		"category": "SIGNER",
		"reason": "UPDATE",
		"signerAddress": "GSIGNER",
		"oldWeight": 0,
		"newWeight": 5
	}`))
	require.NoError(t, err)

	su, ok := node.(*SignerUpdatedChange)
	require.True(t, ok, "expected *SignerUpdatedChange, got %T", node)
	assert.Equal(t, "GSIGNER", su.SignerAddress)
	assert.Equal(t, int32(0), su.OldWeight, "a master key previously locked at weight 0 carries oldWeight 0")
	assert.Equal(t, int32(5), su.NewWeight)
}

// Test_UnmarshalStateChangeNode_AllowanceChange checks the UInt32 expirationLedger and the
// allowance-specific tokenId alias and spender.
func Test_UnmarshalStateChangeNode_AllowanceChange(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AllowanceChange",
		"category": "ALLOWANCE",
		"reason": "UPDATE",
		"allowanceTokenId": "CTOKEN",
		"spender": "GSPENDER",
		"amount": "999",
		"expirationLedger": 123456
	}`))
	require.NoError(t, err)

	ac, ok := node.(*AllowanceChange)
	require.True(t, ok, "expected *AllowanceChange, got %T", node)
	assert.Equal(t, "CTOKEN", ac.TokenID)
	assert.Equal(t, "GSPENDER", ac.Spender)
	assert.Equal(t, "999", ac.Amount)
	assert.Equal(t, uint32(123456), ac.ExpirationLedger)
}

func Test_UnmarshalStateChangeNode_AccountFlagsChange(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AccountFlagsChange",
		"category": "FLAGS",
		"reason": "SET",
		"accountFlags": ["AUTH_REQUIRED", "AUTH_REVOCABLE"]
	}`))
	require.NoError(t, err)

	fc, ok := node.(*AccountFlagsChange)
	require.True(t, ok, "expected *AccountFlagsChange, got %T", node)
	assert.Equal(t, []AccountFlag{AccountFlagAuthRequired, AccountFlagAuthRevocable}, fc.Flags)
}

// Test_UnmarshalStateChangeNode_BalanceAuthorizationNullFlags verifies the SAC contract-holder
// case where flags is null (no trustline flags) decodes to a nil slice.
func Test_UnmarshalStateChangeNode_BalanceAuthorizationNullFlags(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "BalanceAuthorizationChange",
		"category": "BALANCE_AUTHORIZATION",
		"reason": "SET",
		"balanceAuthTokenId": "CTOKEN",
		"balanceAuthFlags": null
	}`))
	require.NoError(t, err)

	bac, ok := node.(*BalanceAuthorizationChange)
	require.True(t, ok, "expected *BalanceAuthorizationChange, got %T", node)
	require.NotNil(t, bac.TokenID)
	assert.Equal(t, "CTOKEN", *bac.TokenID)
	assert.Nil(t, bac.Flags, "null flags must decode to a nil slice")
}

func Test_UnmarshalStateChangeNode_BalanceAuthorizationWithFlags(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "BalanceAuthorizationChange",
		"category": "BALANCE_AUTHORIZATION",
		"reason": "CLEAR",
		"balanceAuthTokenId": "CTOKEN",
		"balanceAuthFlags": ["AUTHORIZED", "CLAWBACK_ENABLED"]
	}`))
	require.NoError(t, err)

	bac, ok := node.(*BalanceAuthorizationChange)
	require.True(t, ok, "expected *BalanceAuthorizationChange, got %T", node)
	assert.Equal(t, []TrustlineFlag{TrustlineFlagAuthorized, TrustlineFlagClawbackEnabled}, bac.Flags)
}

// Test_UnmarshalStateChangeNode_DataEntryAdded checks the ADD shape, which carries only the
// entry's new value.
func Test_UnmarshalStateChangeNode_DataEntryAdded(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "DataEntryAddedChange",
		"category": "DATA_ENTRY",
		"reason": "ADD",
		"name": "foo",
		"value": "YmFy"
	}`))
	require.NoError(t, err)

	da, ok := node.(*DataEntryAddedChange)
	require.True(t, ok, "expected *DataEntryAddedChange, got %T", node)
	assert.Equal(t, StateChangeCategoryDataEntry, da.GetCategory())
	assert.Equal(t, StateChangeReasonAdd, da.GetReason())
	assert.Equal(t, "foo", da.Name)
	assert.Equal(t, "YmFy", da.Value)
}

// Test_UnmarshalStateChangeNode_DataEntryUpdated checks the UPDATE shape, which carries both
// the previous and the new value.
func Test_UnmarshalStateChangeNode_DataEntryUpdated(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "DataEntryUpdatedChange",
		"category": "DATA_ENTRY",
		"reason": "UPDATE",
		"name": "foo",
		"oldValue": "YmFy",
		"newValue": "YmF6"
	}`))
	require.NoError(t, err)

	du, ok := node.(*DataEntryUpdatedChange)
	require.True(t, ok, "expected *DataEntryUpdatedChange, got %T", node)
	assert.Equal(t, StateChangeReasonUpdate, du.GetReason())
	assert.Equal(t, "foo", du.Name)
	assert.Equal(t, "YmFy", du.OldValue)
	assert.Equal(t, "YmF6", du.NewValue)
}

// Test_UnmarshalStateChangeNode_DataEntryRemoved checks the REMOVE shape, which carries only
// the value the entry held when removed.
func Test_UnmarshalStateChangeNode_DataEntryRemoved(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "DataEntryRemovedChange",
		"category": "DATA_ENTRY",
		"reason": "REMOVE",
		"name": "foo",
		"oldValue": "YmFy"
	}`))
	require.NoError(t, err)

	dr, ok := node.(*DataEntryRemovedChange)
	require.True(t, ok, "expected *DataEntryRemovedChange, got %T", node)
	assert.Equal(t, StateChangeReasonRemove, dr.GetReason())
	assert.Equal(t, "foo", dr.Name)
	assert.Equal(t, "YmFy", dr.OldValue)
}

// Test_UnmarshalStateChangeNode_TrustlineAdded exercises the mutually-exclusive tokenId /
// liquidityPoolId pair: an asset trustline sets tokenId and leaves liquidityPoolId nil.
func Test_UnmarshalStateChangeNode_TrustlineAdded(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "TrustlineAddedChange",
		"category": "TRUSTLINE",
		"reason": "ADD",
		"trustlineAddedTokenId": "CTOKEN",
		"liquidityPoolId": null,
		"limit": "1000"
	}`))
	require.NoError(t, err)

	ta, ok := node.(*TrustlineAddedChange)
	require.True(t, ok, "expected *TrustlineAddedChange, got %T", node)
	require.NotNil(t, ta.TokenID)
	assert.Equal(t, "CTOKEN", *ta.TokenID)
	assert.Nil(t, ta.LiquidityPoolID)
	assert.Equal(t, "1000", ta.Limit)
}
