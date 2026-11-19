package types

import (
	"encoding/json"
	"fmt"
)

// SimulatedStateChanges is the result of simulating an unsubmitted transaction.
type SimulatedStateChanges struct {
	// LatestLedger is the ledger the simulation was evaluated against.
	LatestLedger uint32
	// StateChanges are the changes the transaction would produce.
	StateChanges []SimulatedStateChangeNode
	// Tokens is display metadata for the tokens the state changes reference.
	// A token the server has never indexed has no entry.
	Tokens []SimulatedTokenMetadata
}

// UnmarshalJSON decodes each state change into its concrete type.
func (s *SimulatedStateChanges) UnmarshalJSON(data []byte) error {
	var raw struct {
		LatestLedger uint32                   `json:"latestLedger"`
		StateChanges []json.RawMessage        `json:"stateChanges"`
		Tokens       []SimulatedTokenMetadata `json:"tokens"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshaling simulated state changes: %w", err)
	}

	stateChanges := make([]SimulatedStateChangeNode, 0, len(raw.StateChanges))
	for _, rawNode := range raw.StateChanges {
		node, err := UnmarshalSimulatedStateChangeNode(rawNode)
		if err != nil {
			return err
		}
		stateChanges = append(stateChanges, node)
	}

	s.LatestLedger = raw.LatestLedger
	s.StateChanges = stateChanges
	s.Tokens = raw.Tokens
	return nil
}

// SimulatedTokenMetadata is display metadata for one token. Code and Issuer
// are set for Stellar-asset-contract tokens; Name and Symbol are whatever the
// contract reports.
type SimulatedTokenMetadata struct {
	TokenID  string  `json:"tokenId"`
	Code     *string `json:"code,omitempty"`
	Issuer   *string `json:"issuer,omitempty"`
	Name     *string `json:"name,omitempty"`
	Symbol   *string `json:"symbol,omitempty"`
	Decimals uint32  `json:"decimals"`
}

// SimulatedStateChangeNode is the interface every simulated state change type
// implements. It corresponds to BaseSimulatedStateChange in the GraphQL schema.
type SimulatedStateChangeNode interface {
	GetCategory() StateChangeCategory
	GetReason() StateChangeReason
	GetAccountAddress() string
	GetOperationIndex() *int32
}

// BaseSimulatedStateChangeFields holds the fields every simulated state change
// shares. OperationIndex is the zero-based index of the operation that would
// produce the change; it is nil only for the transaction-fee row.
type BaseSimulatedStateChangeFields struct {
	Category       StateChangeCategory `json:"category"`
	Reason         StateChangeReason   `json:"reason"`
	AccountAddress string              `json:"accountAddress"`
	OperationIndex *int32              `json:"operationIndex,omitempty"`
}

// GetCategory returns the state change category.
func (b BaseSimulatedStateChangeFields) GetCategory() StateChangeCategory {
	return b.Category
}

// GetReason returns the state change reason.
func (b BaseSimulatedStateChangeFields) GetReason() StateChangeReason {
	return b.Reason
}

// GetAccountAddress returns the address of the account whose state would change.
func (b BaseSimulatedStateChangeFields) GetAccountAddress() string {
	return b.AccountAddress
}

// GetOperationIndex returns the index of the operation that would produce the
// change, or nil for the transaction-fee row.
func (b BaseSimulatedStateChangeFields) GetOperationIndex() *int32 {
	return b.OperationIndex
}

// The concrete types below mirror the history state change types field for
// field, with the same JSON keys (including the aliased ones), so a caller can
// map both with the same code.

// SimulatedBalanceChange is a movement of value on the account's token balance.
type SimulatedBalanceChange struct {
	BaseSimulatedStateChangeFields
	TokenID   string  `json:"balanceTokenId"`
	Amount    string  `json:"amount"`
	ToMuxedID *string `json:"toMuxedId,omitempty"`
}

// SimulatedAccountCreatedChange is a classic account creation or a contract deployment.
type SimulatedAccountCreatedChange struct {
	BaseSimulatedStateChangeFields
	CreatorAddress string `json:"creatorAddress"`
}

// SimulatedAccountMergedChange is an account merge.
type SimulatedAccountMergedChange struct {
	BaseSimulatedStateChangeFields
	DestinationAddress string `json:"destinationAddress"`
}

// SimulatedSignerAddedChange is a signer added to the account.
type SimulatedSignerAddedChange struct {
	BaseSimulatedStateChangeFields
	SignerAddress string `json:"signerAddress"`
	NewWeight     int32  `json:"newWeight"`
}

// SimulatedSignerUpdatedChange is an existing signer's weight change.
type SimulatedSignerUpdatedChange struct {
	BaseSimulatedStateChangeFields
	SignerAddress string `json:"signerAddress"`
	OldWeight     int32  `json:"oldWeight"`
	NewWeight     int32  `json:"newWeight"`
}

// SimulatedSignerRemovedChange is a signer removed from the account.
type SimulatedSignerRemovedChange struct {
	BaseSimulatedStateChangeFields
	SignerAddress string `json:"signerAddress"`
	OldWeight     int32  `json:"oldWeight"`
}

// SimulatedThresholdChange is a signature-threshold change.
type SimulatedThresholdChange struct {
	BaseSimulatedStateChangeFields
	Threshold    ThresholdLevel `json:"threshold"`
	OldThreshold int32          `json:"oldThreshold"`
	NewThreshold int32          `json:"newThreshold"`
}

// SimulatedAccountFlagsChange lists account flags set or cleared in one operation.
type SimulatedAccountFlagsChange struct {
	BaseSimulatedStateChangeFields
	Flags []AccountFlag `json:"accountFlags"`
}

// SimulatedHomeDomainSetChange is a home domain set on an account that had none.
type SimulatedHomeDomainSetChange struct {
	BaseSimulatedStateChangeFields
	HomeDomain string `json:"homeDomain"`
}

// SimulatedHomeDomainUpdatedChange is a home domain replaced by a different one.
type SimulatedHomeDomainUpdatedChange struct {
	BaseSimulatedStateChangeFields
	OldHomeDomain string `json:"oldHomeDomain"`
	NewHomeDomain string `json:"newHomeDomain"`
}

// SimulatedHomeDomainClearedChange is a home domain removed from the account.
type SimulatedHomeDomainClearedChange struct {
	BaseSimulatedStateChangeFields
	OldHomeDomain string `json:"oldHomeDomain"`
}

// SimulatedDataEntryAddedChange is a data entry created on the account.
type SimulatedDataEntryAddedChange struct {
	BaseSimulatedStateChangeFields
	Name  string `json:"name"`
	Value string `json:"value"`
}

// SimulatedDataEntryUpdatedChange is a data entry whose value changed.
type SimulatedDataEntryUpdatedChange struct {
	BaseSimulatedStateChangeFields
	Name     string `json:"name"`
	OldValue string `json:"oldValue"`
	NewValue string `json:"newValue"`
}

// SimulatedDataEntryRemovedChange is a data entry removed from the account.
type SimulatedDataEntryRemovedChange struct {
	BaseSimulatedStateChangeFields
	Name     string `json:"name"`
	OldValue string `json:"oldValue"`
}

// SimulatedAllowanceChange is a SEP-41 allowance approval.
type SimulatedAllowanceChange struct {
	BaseSimulatedStateChangeFields
	TokenID          string `json:"allowanceTokenId"`
	Spender          string `json:"spender"`
	Amount           string `json:"amount"`
	ExpirationLedger uint32 `json:"expirationLedger"`
}

// SimulatedTrustlineAddedChange is a trustline created. Exactly one of TokenID / LiquidityPoolID is set.
type SimulatedTrustlineAddedChange struct {
	BaseSimulatedStateChangeFields
	TokenID         *string `json:"trustlineAddedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
	Limit           string  `json:"limit"`
}

// SimulatedTrustlineUpdatedChange is a trustline limit update. Exactly one of TokenID / LiquidityPoolID is set.
type SimulatedTrustlineUpdatedChange struct {
	BaseSimulatedStateChangeFields
	TokenID         *string `json:"trustlineUpdatedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
	OldLimit        string  `json:"oldLimit"`
	NewLimit        string  `json:"newLimit"`
}

// SimulatedTrustlineRemovedChange is a trustline removed. Exactly one of TokenID / LiquidityPoolID is set.
type SimulatedTrustlineRemovedChange struct {
	BaseSimulatedStateChangeFields
	TokenID         *string `json:"trustlineRemovedTokenId,omitempty"`
	LiquidityPoolID *string `json:"liquidityPoolId,omitempty"`
}

// SimulatedBalanceAuthorizationChange is authorization to hold or transact an
// asset granted or revoked. Exactly one of TokenID / LiquidityPoolID is set.
// Flags is nil for SAC contract-holder authorization.
type SimulatedBalanceAuthorizationChange struct {
	BaseSimulatedStateChangeFields
	TokenID         *string         `json:"balanceAuthTokenId,omitempty"`
	LiquidityPoolID *string         `json:"liquidityPoolId,omitempty"`
	Flags           []TrustlineFlag `json:"balanceAuthFlags,omitempty"`
}

// UnmarshalSimulatedStateChangeNode unmarshals a JSON simulated state change
// into its concrete type, chosen by the __typename field.
func UnmarshalSimulatedStateChangeNode(data []byte) (SimulatedStateChangeNode, error) {
	var wrapper stateChangeNodeWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("unmarshaling simulated state change wrapper: %w", err)
	}

	switch wrapper.TypeName {
	case "SimulatedBalanceChange":
		return unmarshalSimulatedStateChange[SimulatedBalanceChange](data)
	case "SimulatedAccountCreatedChange":
		return unmarshalSimulatedStateChange[SimulatedAccountCreatedChange](data)
	case "SimulatedAccountMergedChange":
		return unmarshalSimulatedStateChange[SimulatedAccountMergedChange](data)
	case "SimulatedSignerAddedChange":
		return unmarshalSimulatedStateChange[SimulatedSignerAddedChange](data)
	case "SimulatedSignerUpdatedChange":
		return unmarshalSimulatedStateChange[SimulatedSignerUpdatedChange](data)
	case "SimulatedSignerRemovedChange":
		return unmarshalSimulatedStateChange[SimulatedSignerRemovedChange](data)
	case "SimulatedThresholdChange":
		return unmarshalSimulatedStateChange[SimulatedThresholdChange](data)
	case "SimulatedAccountFlagsChange":
		return unmarshalSimulatedStateChange[SimulatedAccountFlagsChange](data)
	case "SimulatedHomeDomainSetChange":
		return unmarshalSimulatedStateChange[SimulatedHomeDomainSetChange](data)
	case "SimulatedHomeDomainUpdatedChange":
		return unmarshalSimulatedStateChange[SimulatedHomeDomainUpdatedChange](data)
	case "SimulatedHomeDomainClearedChange":
		return unmarshalSimulatedStateChange[SimulatedHomeDomainClearedChange](data)
	case "SimulatedDataEntryAddedChange":
		return unmarshalSimulatedStateChange[SimulatedDataEntryAddedChange](data)
	case "SimulatedDataEntryUpdatedChange":
		return unmarshalSimulatedStateChange[SimulatedDataEntryUpdatedChange](data)
	case "SimulatedDataEntryRemovedChange":
		return unmarshalSimulatedStateChange[SimulatedDataEntryRemovedChange](data)
	case "SimulatedAllowanceChange":
		return unmarshalSimulatedStateChange[SimulatedAllowanceChange](data)
	case "SimulatedTrustlineAddedChange":
		return unmarshalSimulatedStateChange[SimulatedTrustlineAddedChange](data)
	case "SimulatedTrustlineUpdatedChange":
		return unmarshalSimulatedStateChange[SimulatedTrustlineUpdatedChange](data)
	case "SimulatedTrustlineRemovedChange":
		return unmarshalSimulatedStateChange[SimulatedTrustlineRemovedChange](data)
	case "SimulatedBalanceAuthorizationChange":
		return unmarshalSimulatedStateChange[SimulatedBalanceAuthorizationChange](data)
	default:
		return nil, fmt.Errorf("unknown simulated state change type: %s", wrapper.TypeName)
	}
}

func unmarshalSimulatedStateChange[SC any](data []byte) (SimulatedStateChangeNode, error) {
	var sc SC
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, fmt.Errorf("unmarshaling %T: %w", sc, err)
	}
	node, ok := any(&sc).(SimulatedStateChangeNode)
	if !ok {
		return nil, fmt.Errorf("%T does not implement SimulatedStateChangeNode", sc)
	}
	return node, nil
}
