// State change builder for creating token transfer state changes
// Provides a fluent interface for constructing state changes with proper field validation
package processors

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

// StateChangeBuilder provides a fluent interface for creating state changes
type StateChangeBuilder struct {
	base           types.StateChange
	metricsService MetricsServiceInterface
}

// NewStateChangeBuilder creates a new builder with base state change fields.
// txID is the transaction TOID, used as the default ToID for fee-based state changes.
// For operation-based state changes, call WithOperationID() to override ToID.
func NewStateChangeBuilder(ledgerCloseTime int64, txID int64, metricsService MetricsServiceInterface) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			ToID:            txID,
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
		},
		metricsService: metricsService,
	}
}

// WithCategory sets the state change category
func (b *StateChangeBuilder) WithCategory(category types.StateChangeCategory) *StateChangeBuilder {
	b.base.StateChangeCategory = category
	return b
}

// WithReason sets the state change reason
func (b *StateChangeBuilder) WithReason(reason types.StateChangeReason) *StateChangeBuilder {
	b.base.StateChangeReason = &reason
	return b
}

// WithThresholds sets the thresholds
func (b *StateChangeBuilder) WithThresholds(thresholds map[string]any) *StateChangeBuilder {
	b.base.Thresholds = types.NullableJSONB(thresholds)
	return b
}

// WithTrustlineLimit sets the trustline limit
func (b *StateChangeBuilder) WithTrustlineLimit(limit map[string]any) *StateChangeBuilder {
	b.base.TrustlineLimit = types.NullableJSONB(limit)
	return b
}

// WithFlags sets the flags
func (b *StateChangeBuilder) WithFlags(flags []string) *StateChangeBuilder {
	b.base.Flags = types.NullableJSON(flags)
	return b
}

// WithAccount sets the account ID
func (b *StateChangeBuilder) WithAccount(accountID string) *StateChangeBuilder {
	b.base.AccountID = accountID
	return b
}

// WithSigner sets the signer and the weights
func (b *StateChangeBuilder) WithSigner(signer string, weights map[string]any) *StateChangeBuilder {
	b.base.SignerAccountID = utils.SQLNullString(signer)
	b.base.SignerWeights = types.NullableJSONB(weights)
	return b
}

// WithDeployer sets the deployer account ID, usually associated with a contract deployment.
func (b *StateChangeBuilder) WithDeployer(deployer string) *StateChangeBuilder {
	b.base.DeployerAccountID = utils.SQLNullString(deployer)
	return b
}

// WithFunder sets the funder account ID
func (b *StateChangeBuilder) WithFunder(funder string) *StateChangeBuilder {
	b.base.FunderAccountID = utils.SQLNullString(funder)
	return b
}

// WithSponsor sets the sponsor
func (b *StateChangeBuilder) WithSponsor(sponsor string) *StateChangeBuilder {
	b.base.SponsorAccountID = utils.SQLNullString(sponsor)
	return b
}

// WithKeyValue sets the key value
func (b *StateChangeBuilder) WithKeyValue(valueMap map[string]any) *StateChangeBuilder {
	b.base.KeyValue = types.NullableJSONB(valueMap)
	return b
}

// WithAmount sets the amount
func (b *StateChangeBuilder) WithAmount(amount string) *StateChangeBuilder {
	b.base.Amount = utils.SQLNullString(amount)
	return b
}

// WithToken sets the token ID using the contract address
func (b *StateChangeBuilder) WithToken(contractAddress string) *StateChangeBuilder {
	b.base.TokenID = utils.SQLNullString(contractAddress)
	return b
}

// WithTrustlineAsset sets the trustline asset
func (b *StateChangeBuilder) WithTrustlineAsset(asset string) *StateChangeBuilder {
	b.base.TrustlineAsset = asset
	return b
}

// WithTokenType sets the token type (SAC or CUSTOM)
func (b *StateChangeBuilder) WithTokenType(tokenType types.ContractType) *StateChangeBuilder {
	b.base.ContractType = tokenType
	return b
}

// WithSponsoredAccountID sets the sponsored account ID for a sponsorship state change
func (b *StateChangeBuilder) WithSponsoredAccountID(sponsoredAccountID string) *StateChangeBuilder {
	b.base.SponsoredAccountID = utils.SQLNullString(sponsoredAccountID)
	return b
}

// WithOperationID sets the ToID to the operation ID for operation-based state changes.
// This overrides the default txID set in the constructor.
func (b *StateChangeBuilder) WithOperationID(operationID int64) *StateChangeBuilder {
	b.base.ToID = operationID
	return b
}

// Build returns the constructed state change.
func (b *StateChangeBuilder) Build() types.StateChange {
	b.base.SortKey = b.generateSortKey()
	return b.base
}

// generateSortKey creates a deterministic string representation of a state change for sorting purposes.
func (b *StateChangeBuilder) generateSortKey() string {
	reason := ""
	if b.base.StateChangeReason != nil {
		reason = string(*b.base.StateChangeReason)
	}

	// For JSON fields, marshal to get a canonical string
	signerWeights, err := json.Marshal(b.base.SignerWeights)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal signer weights: %v", err))
	}
	thresholds, err := json.Marshal(b.base.Thresholds)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal thresholds: %v", err))
	}
	flags, err := json.Marshal(b.base.Flags)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal flags: %v", err))
	}
	keyValue, err := json.Marshal(b.base.KeyValue)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal key value: %v", err))
	}

	return fmt.Sprintf(
		"%d:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s",
		b.base.ToID,
		b.base.StateChangeCategory,
		reason,
		b.base.AccountID,
		b.base.TokenID.String,
		b.base.Amount.String,
		b.base.OfferID.String,
		b.base.SignerAccountID.String,
		b.base.SpenderAccountID.String,
		b.base.SponsoredAccountID.String,
		b.base.SponsorAccountID.String,
		string(signerWeights),
		string(thresholds),
		string(flags),
		string(keyValue),
	)
}

func (b *StateChangeBuilder) Clone() *StateChangeBuilder {
	return &StateChangeBuilder{
		base:           b.base,
		metricsService: b.metricsService,
	}
}
