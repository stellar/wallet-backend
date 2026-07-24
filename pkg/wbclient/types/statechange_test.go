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

// Test_UnmarshalStateChangeNode_FeeChange covers the only concrete type with no associated
// operation: it decodes from its own tokenId alias and carries no toMuxedId.
func Test_UnmarshalStateChangeNode_FeeChange(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "FeeChange",
		"category": "BALANCE",
		"reason": "DEBIT",
		"feeTokenId": "native",
		"amount": "100"
	}`))
	require.NoError(t, err)

	fc, ok := node.(*FeeChange)
	require.True(t, ok, "expected *FeeChange, got %T", node)
	assert.Equal(t, StateChangeCategoryBalance, fc.GetCategory())
	assert.Equal(t, StateChangeReasonDebit, fc.GetReason())
	assert.Equal(t, "native", fc.TokenID)
	assert.Equal(t, "100", fc.Amount)
}

// Test_UnmarshalStateChangeNode_AccountCreatedVsContractDeployed guards the two ACCOUNT/CREATE
// variants that share the same (category, reason) pair but different __typename and payload.
func Test_UnmarshalStateChangeNode_AccountCreatedVsContractDeployed(t *testing.T) {
	created, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AccountCreatedChange",
		"category": "ACCOUNT",
		"reason": "CREATE",
		"funderAddress": "GFUNDER"
	}`))
	require.NoError(t, err)
	ac, ok := created.(*AccountCreatedChange)
	require.True(t, ok, "expected *AccountCreatedChange, got %T", created)
	assert.Equal(t, "GFUNDER", ac.FunderAddress)

	deployed, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "ContractDeployedChange",
		"category": "ACCOUNT",
		"reason": "CREATE",
		"deployerAddress": "GDEPLOYER"
	}`))
	require.NoError(t, err)
	cd, ok := deployed.(*ContractDeployedChange)
	require.True(t, ok, "expected *ContractDeployedChange, got %T", deployed)
	assert.Equal(t, "GDEPLOYER", cd.DeployerAddress)
}

// Test_UnmarshalStateChangeNode_SignerUpdatedNullOldWeight verifies a nullable Int (oldWeight)
// decodes to a nil pointer while the required newWeight is populated.
func Test_UnmarshalStateChangeNode_SignerUpdatedNullOldWeight(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "SignerUpdatedChange",
		"category": "SIGNER",
		"reason": "UPDATE",
		"signerAddress": "GSIGNER",
		"oldWeight": null,
		"newWeight": 5
	}`))
	require.NoError(t, err)

	su, ok := node.(*SignerUpdatedChange)
	require.True(t, ok, "expected *SignerUpdatedChange, got %T", node)
	assert.Equal(t, "GSIGNER", su.SignerAddress)
	assert.Nil(t, su.OldWeight, "null oldWeight must decode to a nil pointer")
	assert.Equal(t, int32(5), su.NewWeight)
}

// Test_UnmarshalStateChangeNode_AllowanceChange checks the UInt32 expirationLedger and the
// allowance-specific tokenId alias and spender.
func Test_UnmarshalStateChangeNode_AllowanceChange(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "AllowanceChange",
		"category": "METADATA",
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

// Test_UnmarshalStateChangeNode_SponsorshipChange verifies the aliased signerAddress and tokenId
// keys land on the right fields without colliding with the signer types' bare signerAddress.
func Test_UnmarshalStateChangeNode_SponsorshipChange(t *testing.T) {
	node, err := UnmarshalStateChangeNode([]byte(`{
		"__typename": "SponsorshipChange",
		"category": "RESERVES",
		"reason": "SPONSOR",
		"sponsorAddress": "GSPONSOR",
		"sponsorshipSignerAddress": "GSIGNER"
	}`))
	require.NoError(t, err)

	sc, ok := node.(*SponsorshipChange)
	require.True(t, ok, "expected *SponsorshipChange, got %T", node)
	require.NotNil(t, sc.SponsorAddress)
	assert.Equal(t, "GSPONSOR", *sc.SponsorAddress)
	require.NotNil(t, sc.SignerAddress)
	assert.Equal(t, "GSIGNER", *sc.SignerAddress)
	assert.Nil(t, sc.SponsoredAddress)
}
