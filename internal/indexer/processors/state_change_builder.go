// State change builder for creating token transfer state changes
// Provides a fluent interface for constructing state changes with proper field validation
package processors

import (
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

// StateChangeBuilder provides a fluent interface for creating state changes
type StateChangeBuilder struct {
	base types.StateChange
}

// NewStateChangeBuilder creates a new builder with base state change fields
func NewStateChangeBuilder(ledgerNumber uint32, ledgerCloseTime int64, txHash string) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			LedgerNumber:    ledgerNumber,
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
			IngestedAt:      time.Now(),
			TxHash:          txHash,
		},
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

// WithClaimableBalance sets the claimable balance ID
func (b *StateChangeBuilder) WithClaimableBalance(balanceID string) *StateChangeBuilder {
	b.base.ClaimableBalanceID = utils.SQLNullString(balanceID)
	return b
}

// WithLiquidityPool sets the liquidity pool ID
func (b *StateChangeBuilder) WithLiquidityPool(poolID string) *StateChangeBuilder {
	b.base.LiquidityPoolID = utils.SQLNullString(poolID)
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

// Build returns the constructed state change
func (b *StateChangeBuilder) Build() types.StateChange {
	b.base.ID = b.generateID()
	return b.base
}

// Clone creates a new builder with the same base state change fields
func (b *StateChangeBuilder) Clone() *StateChangeBuilder {
	return &StateChangeBuilder{
		base: b.base,
	}
}

func (b *StateChangeBuilder) generateID() string {
	return fmt.Sprintf("%d-%s", b.base.OperationID, b.base.AccountID)
}
