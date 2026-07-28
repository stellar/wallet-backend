// Package types provides type definitions for state changes with proper polymorphic support
package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// StateChangeNode is the interface that all state change types implement.
// This corresponds to the BaseStateChange interface in the GraphQL schema.
type StateChangeNode interface {
	GetCategory() StateChangeCategory
	GetReason() StateChangeReason
	GetIngestedAt() time.Time
	GetLedgerCreatedAt() time.Time
	GetLedgerNumber() uint32
}

// BaseStateChangeFields contains the common fields shared by all state change types.
type BaseStateChangeFields struct {
	Category        StateChangeCategory `json:"category"`
	Reason          StateChangeReason   `json:"reason"`
	IngestedAt      time.Time           `json:"ingestedAt"`
	LedgerCreatedAt time.Time           `json:"ledgerCreatedAt"`
	LedgerNumber    uint32              `json:"ledgerNumber"`
}

// GetCategory returns the state change category.
func (b BaseStateChangeFields) GetCategory() StateChangeCategory {
	return b.Category
}

// GetReason returns the state change reason.
func (b BaseStateChangeFields) GetReason() StateChangeReason {
	return b.Reason
}

// GetIngestedAt returns when the state change was ingested.
func (b BaseStateChangeFields) GetIngestedAt() time.Time {
	return b.IngestedAt
}

// GetLedgerCreatedAt returns when the ledger was created.
func (b BaseStateChangeFields) GetLedgerCreatedAt() time.Time {
	return b.LedgerCreatedAt
}

// GetLedgerNumber returns the ledger number.
func (b BaseStateChangeFields) GetLedgerNumber() uint32 {
	return b.LedgerNumber
}

// The concrete types below mirror the GraphQL schema's BaseStateChange implementations one-to-one.
// Fields whose GraphQL type differs across concrete types that share a name (tokenId, flags, and
// signerAddress) are aliased in the query fragments to distinct response keys; the JSON tags here
// match those aliases. See stateChangeFragments in the wbclient package.

// BalanceChange is a movement of value on the account's token balance.
type BalanceChange struct {
	BaseStateChangeFields
	TokenID   string  `json:"balanceTokenId"`
	Amount    string  `json:"amount"`
	ToMuxedID *string `json:"toMuxedId,omitempty"`
}

// AccountCreatedChange is a classic account creation.
type AccountCreatedChange struct {
	BaseStateChangeFields
	FunderAddress string `json:"funderAddress"`
}

// ContractDeployedChange is a smart-contract deployment.
type ContractDeployedChange struct {
	BaseStateChangeFields
	DeployerAddress string `json:"deployerAddress"`
}

// AccountMergedChange is an account merge.
type AccountMergedChange struct {
	BaseStateChangeFields
	DestinationAddress string `json:"destinationAddress"`
}

// SignerAddedChange is a signer added to the account.
type SignerAddedChange struct {
	BaseStateChangeFields
	SignerAddress string `json:"signerAddress"`
	NewWeight     int32  `json:"newWeight"`
}

// SignerUpdatedChange is an existing signer's weight change.
type SignerUpdatedChange struct {
	BaseStateChangeFields
	SignerAddress string `json:"signerAddress"`
	OldWeight     int32  `json:"oldWeight"`
	NewWeight     int32  `json:"newWeight"`
}

// SignerRemovedChange is a signer removed from the account.
type SignerRemovedChange struct {
	BaseStateChangeFields
	SignerAddress string `json:"signerAddress"`
	OldWeight     int32  `json:"oldWeight"`
}

// ThresholdChange is a signature-threshold change; Reason identifies which threshold changed.
type ThresholdChange struct {
	BaseStateChangeFields
	OldThreshold int32 `json:"oldThreshold"`
	NewThreshold int32 `json:"newThreshold"`
}

// AccountFlagsChange lists account authorization flags set or cleared in one operation.
type AccountFlagsChange struct {
	BaseStateChangeFields
	Flags []AccountFlag `json:"accountFlags"`
}

// HomeDomainChange is a home-domain change on the account.
type HomeDomainChange struct {
	BaseStateChangeFields
	OldHomeDomain string `json:"oldHomeDomain"`
	NewHomeDomain string `json:"newHomeDomain"`
}

// DataEntryChange is a data entry created, updated, or removed on the account.
type DataEntryChange struct {
	BaseStateChangeFields
	Name     string  `json:"name"`
	OldValue *string `json:"oldValue,omitempty"`
	NewValue *string `json:"newValue,omitempty"`
}

// AllowanceChange is a SEP-41 allowance approval.
type AllowanceChange struct {
	BaseStateChangeFields
	TokenID          string `json:"allowanceTokenId"`
	Spender          string `json:"spender"`
	Amount           string `json:"amount"`
	ExpirationLedger uint32 `json:"expirationLedger"`
}

// TrustlineAddedChange is a trustline created. Exactly one of TokenID / LiquidityPoolID is set.
type TrustlineAddedChange struct {
	BaseStateChangeFields
	TokenID         *string `json:"trustlineAddedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
	Limit           string  `json:"limit"`
}

// TrustlineUpdatedChange is a trustline limit update. Exactly one of TokenID / LiquidityPoolID is set.
type TrustlineUpdatedChange struct {
	BaseStateChangeFields
	TokenID         *string `json:"trustlineUpdatedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
	OldLimit        string  `json:"oldLimit"`
	NewLimit        string  `json:"newLimit"`
}

// TrustlineRemovedChange is a trustline removed. Exactly one of TokenID / LiquidityPoolID is set.
type TrustlineRemovedChange struct {
	BaseStateChangeFields
	TokenID         *string `json:"trustlineRemovedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
}

// SponsorshipChange is a base-reserve sponsorship established or released. At most one of the
// entity fields (TokenID, LiquidityPoolID, ClaimableBalanceID, DataName, SignerAddress)
// identifies what is sponsored.
type SponsorshipChange struct {
	BaseStateChangeFields
	SponsoredAddress   *string `json:"sponsoredAddress,omitempty"`
	SponsorAddress     *string `json:"sponsorAddress,omitempty"`
	TokenID            *string `json:"sponsorshipTokenId,omitempty"`
	LiquidityPoolID    *string `json:"liquidityPoolId,omitempty"`
	ClaimableBalanceID *string `json:"claimableBalanceId,omitempty"`
	DataName           *string `json:"dataName,omitempty"`
	SignerAddress      *string `json:"sponsorshipSignerAddress,omitempty"`
}

// BalanceAuthorizationChange is authorization to hold or transact an asset granted or revoked.
// Exactly one of TokenID / LiquidityPoolID is set. Flags is nil for SAC contract-holder
// authorization, which has no trustline flags.
type BalanceAuthorizationChange struct {
	BaseStateChangeFields
	TokenID         *string         `json:"balanceAuthTokenId,omitempty"`
	LiquidityPoolID *string         `json:"liquidityPoolId,omitempty"`
	Flags           []TrustlineFlag `json:"balanceAuthFlags,omitempty"`
}

// stateChangeNodeWrapper is used for unmarshaling polymorphic state change responses.
type stateChangeNodeWrapper struct {
	TypeName string `json:"__typename"`
}

// UnmarshalStateChangeNode unmarshals a JSON state change node into the appropriate concrete type
// based on the __typename field.
func UnmarshalStateChangeNode(data []byte) (StateChangeNode, error) {
	var wrapper stateChangeNodeWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("unmarshaling state change wrapper: %w", err)
	}

	switch wrapper.TypeName {
	case "BalanceChange":
		return unmarshalStateChange[BalanceChange](data)
	case "AccountCreatedChange":
		return unmarshalStateChange[AccountCreatedChange](data)
	case "ContractDeployedChange":
		return unmarshalStateChange[ContractDeployedChange](data)
	case "AccountMergedChange":
		return unmarshalStateChange[AccountMergedChange](data)
	case "SignerAddedChange":
		return unmarshalStateChange[SignerAddedChange](data)
	case "SignerUpdatedChange":
		return unmarshalStateChange[SignerUpdatedChange](data)
	case "SignerRemovedChange":
		return unmarshalStateChange[SignerRemovedChange](data)
	case "ThresholdChange":
		return unmarshalStateChange[ThresholdChange](data)
	case "AccountFlagsChange":
		return unmarshalStateChange[AccountFlagsChange](data)
	case "HomeDomainChange":
		return unmarshalStateChange[HomeDomainChange](data)
	case "DataEntryChange":
		return unmarshalStateChange[DataEntryChange](data)
	case "AllowanceChange":
		return unmarshalStateChange[AllowanceChange](data)
	case "TrustlineAddedChange":
		return unmarshalStateChange[TrustlineAddedChange](data)
	case "TrustlineUpdatedChange":
		return unmarshalStateChange[TrustlineUpdatedChange](data)
	case "TrustlineRemovedChange":
		return unmarshalStateChange[TrustlineRemovedChange](data)
	case "SponsorshipChange":
		return unmarshalStateChange[SponsorshipChange](data)
	case "BalanceAuthorizationChange":
		return unmarshalStateChange[BalanceAuthorizationChange](data)
	default:
		return nil, fmt.Errorf("unknown state change type: %s", wrapper.TypeName)
	}
}

// unmarshalStateChange decodes data into a concrete state change type SC and returns it as a
// StateChangeNode. SC must be a value type whose pointer implements StateChangeNode (every
// concrete type embeds BaseStateChangeFields, whose getters have value receivers, so *SC
// satisfies the interface).
func unmarshalStateChange[SC any](data []byte) (StateChangeNode, error) {
	var sc SC
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, fmt.Errorf("unmarshaling %T: %w", sc, err)
	}
	node, ok := any(&sc).(StateChangeNode)
	if !ok {
		return nil, fmt.Errorf("%T does not implement StateChangeNode", sc)
	}
	return node, nil
}
