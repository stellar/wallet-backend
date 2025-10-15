// Package types provides type definitions for state changes with proper polymorphic support
package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// StateChangeNode is the interface that all state change types implement
// This corresponds to the BaseStateChange interface in the GraphQL schema
type StateChangeNode interface {
	GetType() StateChangeCategory
	GetReason() StateChangeReason
	GetIngestedAt() time.Time
	GetLedgerCreatedAt() time.Time
	GetLedgerNumber() uint32
	GetAccountID() string
}

// BaseStateChangeFields contains the common fields shared by all state change types
type BaseStateChangeFields struct {
	Type            StateChangeCategory `json:"type"`
	Reason          StateChangeReason   `json:"reason"`
	IngestedAt      time.Time           `json:"ingestedAt"`
	LedgerCreatedAt time.Time           `json:"ledgerCreatedAt"`
	LedgerNumber    uint32              `json:"ledgerNumber"`
	Account         Account             `json:"account"`
}

// GetType returns the state change category
func (b BaseStateChangeFields) GetType() StateChangeCategory {
	return b.Type
}

// GetReason returns the state change reason
func (b BaseStateChangeFields) GetReason() StateChangeReason {
	return b.Reason
}

// GetIngestedAt returns when the state change was ingested
func (b BaseStateChangeFields) GetIngestedAt() time.Time {
	return b.IngestedAt
}

// GetLedgerCreatedAt returns when the ledger was created
func (b BaseStateChangeFields) GetLedgerCreatedAt() time.Time {
	return b.LedgerCreatedAt
}

// GetLedgerNumber returns the ledger number
func (b BaseStateChangeFields) GetLedgerNumber() uint32 {
	return b.LedgerNumber
}

// GetAccountID returns the account address
func (b BaseStateChangeFields) GetAccountID() string {
	return b.Account.Address
}

// StandardBalanceChange represents a standard balance state change
type StandardBalanceChange struct {
	BaseStateChangeFields
	TokenID string `json:"tokenId"`
	Amount  string `json:"amount"`
}

// AccountChange represents an account state change
type AccountChange struct {
	BaseStateChangeFields
	FunderAddress *string `json:"funderAddress,omitempty"`
}

// SignerChange represents a signer state change
type SignerChange struct {
	BaseStateChangeFields
	SignerAddress *string `json:"signerAddress,omitempty"`
	SignerWeights *string `json:"signerWeights,omitempty"`
}

// SignerThresholdsChange represents a signer thresholds state change
type SignerThresholdsChange struct {
	BaseStateChangeFields
	Thresholds string `json:"thresholds"`
}

// MetadataChange represents a metadata state change
type MetadataChange struct {
	BaseStateChangeFields
	KeyValue string `json:"metadataKeyValue"`
}

// FlagsChange represents a flags state change
type FlagsChange struct {
	BaseStateChangeFields
	Flags []string `json:"flags"`
}

// TrustlineChange represents a trustline state change
type TrustlineChange struct {
	BaseStateChangeFields
	TokenID  *string `json:"tokenId,omitempty"`
	Limit    *string `json:"limit,omitempty"`
	KeyValue *string `json:"trustlineKeyValue,omitempty"`
}

// ReservesChange represents a reserves state change
type ReservesChange struct {
	BaseStateChangeFields
	SponsoredAddress *string `json:"sponsoredAddress,omitempty"`
	SponsorAddress   *string `json:"sponsorAddress,omitempty"`
	KeyValue         *string `json:"reservesKeyValue,omitempty"`
}

// BalanceAuthorizationChange represents a balance authorization state change
type BalanceAuthorizationChange struct {
	BaseStateChangeFields
	TokenID  *string  `json:"tokenId,omitempty"`
	Flags    []string `json:"flags"`
	KeyValue *string  `json:"balanceAuthKeyValue,omitempty"`
}

// stateChangeNodeWrapper is used for unmarshaling polymorphic state change responses
type stateChangeNodeWrapper struct {
	TypeName string `json:"__typename"`
}

// UnmarshalStateChangeNode unmarshals a JSON state change node into the appropriate concrete type
// based on the __typename field
func UnmarshalStateChangeNode(data []byte) (StateChangeNode, error) {
	// First, extract the __typename to determine which type to unmarshal into
	var wrapper stateChangeNodeWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("unmarshaling state change wrapper: %w", err)
	}

	// Unmarshal into the appropriate concrete type based on __typename
	switch wrapper.TypeName {
	case "StandardBalanceChange":
		var sc StandardBalanceChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling StandardBalanceChange: %w", err)
		}
		return &sc, nil

	case "AccountChange":
		var sc AccountChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling AccountChange: %w", err)
		}
		return &sc, nil

	case "SignerChange":
		var sc SignerChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling SignerChange: %w", err)
		}
		return &sc, nil

	case "SignerThresholdsChange":
		var sc SignerThresholdsChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling SignerThresholdsChange: %w", err)
		}
		return &sc, nil

	case "MetadataChange":
		var sc MetadataChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling MetadataChange: %w", err)
		}
		return &sc, nil

	case "FlagsChange":
		var sc FlagsChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling FlagsChange: %w", err)
		}
		return &sc, nil

	case "TrustlineChange":
		var sc TrustlineChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling TrustlineChange: %w", err)
		}
		return &sc, nil

	case "ReservesChange":
		var sc ReservesChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling ReservesChange: %w", err)
		}
		return &sc, nil

	case "BalanceAuthorizationChange":
		var sc BalanceAuthorizationChange
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("unmarshaling BalanceAuthorizationChange: %w", err)
		}
		return &sc, nil

	default:
		return nil, fmt.Errorf("unknown state change type: %s", wrapper.TypeName)
	}
}
