// State change builder for creating token transfer state changes
// Provides a fluent interface for constructing state changes with proper field validation
package processors

import (
	"database/sql"
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

// NewStateChangeBuilder creates a new builder with base state change fields
func NewStateChangeBuilder(ledgerNumber uint32, ledgerCloseTime int64, txID int64, metricsService MetricsServiceInterface) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			LedgerNumber:    ledgerNumber,
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
			IngestedAt:      time.Now(),
			TxID:            txID,
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

// WithThreshold sets the threshold old and new values directly
func (b *StateChangeBuilder) WithThreshold(oldValue, newValue *int16) *StateChangeBuilder {
	if oldValue != nil {
		b.base.ThresholdOld = sql.NullInt16{Int16: *oldValue, Valid: true}
	}
	if newValue != nil {
		b.base.ThresholdNew = sql.NullInt16{Int16: *newValue, Valid: true}
	}
	return b
}

// WithTrustlineLimit sets the trustline limit old and new values directly
func (b *StateChangeBuilder) WithTrustlineLimit(oldValue, newValue *string) *StateChangeBuilder {
	if oldValue != nil {
		b.base.TrustlineLimitOld = utils.SQLNullString(*oldValue)
	}
	if newValue != nil {
		b.base.TrustlineLimitNew = utils.SQLNullString(*newValue)
	}
	return b
}

// WithFlags sets the flags as a bitmask from a slice of flag names
func (b *StateChangeBuilder) WithFlags(flags []string) *StateChangeBuilder {
	if len(flags) > 0 {
		bitmask := types.EncodeFlagsToBitmask(flags)
		b.base.Flags = sql.NullInt16{Int16: bitmask, Valid: true}
	}
	return b
}

// WithAccount sets the account ID
func (b *StateChangeBuilder) WithAccount(accountID string) *StateChangeBuilder {
	b.base.AccountID = accountID
	return b
}

// WithSigner sets the signer account ID and the weights directly
func (b *StateChangeBuilder) WithSigner(signer string, oldWeight, newWeight *int16) *StateChangeBuilder {
	b.base.SignerAccountID = utils.SQLNullString(signer)
	if oldWeight != nil {
		b.base.SignerWeightOld = sql.NullInt16{Int16: *oldWeight, Valid: true}
	}
	if newWeight != nil {
		b.base.SignerWeightNew = sql.NullInt16{Int16: *newWeight, Valid: true}
	}
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

// WithKeyValue sets the key value JSONB field for truly variable data (data entries, home domain)
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

// WithOperationID sets the operation ID
func (b *StateChangeBuilder) WithOperationID(operationID int64) *StateChangeBuilder {
	b.base.OperationID = operationID
	return b
}

// WithClaimableBalanceID sets the claimable balance ID for sponsorship state changes
func (b *StateChangeBuilder) WithClaimableBalanceID(balanceID string) *StateChangeBuilder {
	b.base.ClaimableBalanceID = utils.SQLNullString(balanceID)
	return b
}

// WithLiquidityPoolID sets the liquidity pool ID for trustline and sponsorship state changes
func (b *StateChangeBuilder) WithLiquidityPoolID(poolID string) *StateChangeBuilder {
	b.base.LiquidityPoolID = utils.SQLNullString(poolID)
	return b
}

// WithSponsoredData sets the data entry name for data sponsorship state changes
func (b *StateChangeBuilder) WithSponsoredData(dataName string) *StateChangeBuilder {
	b.base.SponsoredData = utils.SQLNullString(dataName)
	return b
}

// Build returns the constructed state change
func (b *StateChangeBuilder) Build() types.StateChange {
	// ToID always stores the transaction's to_id.
	// For operation changes: derive from operation_id using TOID bitmasking (clear lower 12 bits)
	// For fee changes (no operation_id): use the transaction's to_id directly
	if b.base.OperationID != 0 {
		b.base.ToID = b.base.OperationID &^ 0xFFF
	} else {
		b.base.ToID = b.base.TxID
	}
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
	keyValue, err := json.Marshal(b.base.KeyValue)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal key value: %v", err))
	}

	return fmt.Sprintf(
		"%d:%s:%s:%s:%s:%s:%s:%s:%s:%s:%d:%d:%d:%d:%s:%s:%d:%s",
		b.base.ToID,
		b.base.StateChangeCategory,
		reason,
		b.base.AccountID,
		b.base.TokenID.String,
		b.base.Amount.String,
		b.base.SignerAccountID.String,
		b.base.SpenderAccountID.String,
		b.base.SponsoredAccountID.String,
		b.base.SponsorAccountID.String,
		b.base.SignerWeightOld.Int16,
		b.base.SignerWeightNew.Int16,
		b.base.ThresholdOld.Int16,
		b.base.ThresholdNew.Int16,
		b.base.TrustlineLimitOld.String,
		b.base.TrustlineLimitNew.String,
		b.base.Flags.Int16,
		string(keyValue),
	)
}

func (b *StateChangeBuilder) Clone() *StateChangeBuilder {
	return &StateChangeBuilder{
		base:           b.base,
		metricsService: b.metricsService,
	}
}
