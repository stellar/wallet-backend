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

// AccountCreatedChange is a classic account creation or a smart-contract deployment.
// Account holds the created G-address or the deployed C-address; CreatorAddress is the
// funding account or the deploying address respectively.
type AccountCreatedChange struct {
	BaseStateChangeFields
	CreatorAddress string `json:"creatorAddress"`
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

// ThresholdChange is a signature-threshold change; Threshold identifies which threshold changed.
type ThresholdChange struct {
	BaseStateChangeFields
	Threshold    ThresholdLevel `json:"threshold"`
	OldThreshold int32          `json:"oldThreshold"`
	NewThreshold int32          `json:"newThreshold"`
}

// AccountFlagsChange lists account authorization flags set or cleared in one operation.
type AccountFlagsChange struct {
	BaseStateChangeFields
	Flags []AccountFlag `json:"accountFlags"`
}

// HomeDomainSetChange is a home domain set on an account that had none.
type HomeDomainSetChange struct {
	BaseStateChangeFields
	HomeDomain string `json:"homeDomain"`
}

// HomeDomainUpdatedChange is an existing home domain replaced by a different one.
type HomeDomainUpdatedChange struct {
	BaseStateChangeFields
	OldHomeDomain string `json:"oldHomeDomain"`
	NewHomeDomain string `json:"newHomeDomain"`
}

// HomeDomainClearedChange is a home domain removed from the account.
type HomeDomainClearedChange struct {
	BaseStateChangeFields
	OldHomeDomain string `json:"oldHomeDomain"`
}

// DataEntryAddedChange is a data entry created on the account.
type DataEntryAddedChange struct {
	BaseStateChangeFields
	Name  string `json:"name"`
	Value string `json:"value"`
}

// DataEntryUpdatedChange is an existing data entry whose value changed.
type DataEntryUpdatedChange struct {
	BaseStateChangeFields
	Name     string `json:"name"`
	OldValue string `json:"oldValue"`
	NewValue string `json:"newValue"`
}

// DataEntryRemovedChange is a data entry removed from the account.
type DataEntryRemovedChange struct {
	BaseStateChangeFields
	Name     string `json:"name"`
	OldValue string `json:"oldValue"`
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

// BalanceAuthorizationChange is authorization to hold or transact an asset granted or revoked.
// Exactly one of TokenID / LiquidityPoolID is set. Flags is nil for SAC contract-holder
// authorization, which has no trustline flags.
type BalanceAuthorizationChange struct {
	BaseStateChangeFields
	TokenID         *string         `json:"balanceAuthTokenId,omitempty"`
	LiquidityPoolID *string         `json:"liquidityPoolId,omitempty"`
	Flags           []TrustlineFlag `json:"balanceAuthFlags,omitempty"`
}

// BlendSupplyChange is a change to an account's uncollateralized Blend v2
// supply position: (BLEND_SUPPLY, CREDIT | DEBIT).
type BlendSupplyChange struct {
	BaseStateChangeFields
	TokenID string `json:"blendSupplyTokenId"`
	Amount  string `json:"amount"`
	PoolID  string `json:"poolId"`
}

// BlendCollateralChange is a change to an account's collateralized Blend v2
// supply position: (BLEND_COLLATERAL, CREDIT | DEBIT).
type BlendCollateralChange struct {
	BaseStateChangeFields
	TokenID string `json:"blendCollateralTokenId"`
	Amount  string `json:"amount"`
	PoolID  string `json:"poolId"`
}

// BlendDebtChange is a change to an account's Blend v2 debt position:
// (BLEND_DEBT, BORROW | REPAY | FLASH_LOAN | BAD_DEBT | BURN).
type BlendDebtChange struct {
	BaseStateChangeFields
	TokenID string `json:"blendDebtTokenId"`
	Amount  string `json:"amount"`
	PoolID  string `json:"poolId"`
}

// BlendAuctionAmount is one asset's raw protocol-token amount within a filled
// auction's bid or lot.
type BlendAuctionAmount struct {
	AssetContractID string `json:"assetContractId"`
	Amount          string `json:"amount"`
}

// BlendAuctionChange is one side of a filled Blend v2 Dutch auction:
// (BLEND_AUCTION, FILL).
type BlendAuctionChange struct {
	BaseStateChangeFields
	PoolID       string               `json:"poolId"`
	AuctionType  string               `json:"auctionType"`
	FillPercent  int32                `json:"fillPercent"`
	Counterparty string               `json:"counterparty"`
	Lot          []BlendAuctionAmount `json:"lot"`
	Bid          []BlendAuctionAmount `json:"bid"`
}

// BlendEmissionsClaimChange is BLND emissions claimed from a Blend v2 pool:
// (BLEND_EMISSIONS, CLAIM). TokenID is nil only on networks with no known
// BLND SAC.
type BlendEmissionsClaimChange struct {
	BaseStateChangeFields
	TokenID *string `json:"blendEmissionsTokenId,omitempty"`
	Amount  string  `json:"amount"`
	PoolID  string  `json:"poolId"`
}

// BlendBackstopEmissionsClaimChange is backstop emissions claimed and
// auto-restaked as Comet LP tokens: (BLEND_BACKSTOP_EMISSIONS, CLAIM).
type BlendBackstopEmissionsClaimChange struct {
	BaseStateChangeFields
	Amount string `json:"amount"`
}

// BlendBackstopChange is a change to an account's Blend v2 backstop deposit
// for one pool, in Comet LP tokens: (BLEND_BACKSTOP, CREDIT | DEBIT).
type BlendBackstopChange struct {
	BaseStateChangeFields
	Amount string `json:"amount"`
	PoolID string `json:"poolId"`
}

// BlendBackstopQueueChange is a queue-for-withdrawal entry added to or removed
// from an account's Blend v2 backstop position: (BLEND_BACKSTOP_QUEUE, ADD | REMOVE).
type BlendBackstopQueueChange struct {
	BaseStateChangeFields
	Amount string `json:"amount"`
	PoolID string `json:"poolId"`
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
	case "HomeDomainSetChange":
		return unmarshalStateChange[HomeDomainSetChange](data)
	case "HomeDomainUpdatedChange":
		return unmarshalStateChange[HomeDomainUpdatedChange](data)
	case "HomeDomainClearedChange":
		return unmarshalStateChange[HomeDomainClearedChange](data)
	case "DataEntryAddedChange":
		return unmarshalStateChange[DataEntryAddedChange](data)
	case "DataEntryUpdatedChange":
		return unmarshalStateChange[DataEntryUpdatedChange](data)
	case "DataEntryRemovedChange":
		return unmarshalStateChange[DataEntryRemovedChange](data)
	case "AllowanceChange":
		return unmarshalStateChange[AllowanceChange](data)
	case "TrustlineAddedChange":
		return unmarshalStateChange[TrustlineAddedChange](data)
	case "TrustlineUpdatedChange":
		return unmarshalStateChange[TrustlineUpdatedChange](data)
	case "TrustlineRemovedChange":
		return unmarshalStateChange[TrustlineRemovedChange](data)
	case "BalanceAuthorizationChange":
		return unmarshalStateChange[BalanceAuthorizationChange](data)
	case "BlendSupplyChange":
		return unmarshalStateChange[BlendSupplyChange](data)
	case "BlendCollateralChange":
		return unmarshalStateChange[BlendCollateralChange](data)
	case "BlendDebtChange":
		return unmarshalStateChange[BlendDebtChange](data)
	case "BlendAuctionChange":
		return unmarshalStateChange[BlendAuctionChange](data)
	case "BlendEmissionsClaimChange":
		return unmarshalStateChange[BlendEmissionsClaimChange](data)
	case "BlendBackstopEmissionsClaimChange":
		return unmarshalStateChange[BlendBackstopEmissionsClaimChange](data)
	case "BlendBackstopChange":
		return unmarshalStateChange[BlendBackstopChange](data)
	case "BlendBackstopQueueChange":
		return unmarshalStateChange[BlendBackstopQueueChange](data)
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
