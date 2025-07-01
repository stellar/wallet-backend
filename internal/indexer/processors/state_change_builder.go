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
func NewStateChangeBuilder(ledgerNumber uint32, ledgerCloseTime int64, txID int64) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			LedgerNumber:    ledgerNumber,
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
			IngestedAt:      time.Now(),
			TransactionID:   txID,
		},
	}
}

// WithCategory sets the state change category
func (b *StateChangeBuilder) WithCategory(category types.StateChangeCategory) *StateChangeBuilder {
	b.base.StateChangeCategory = category
	return b
}

// WithAccount sets the account ID
func (b *StateChangeBuilder) WithAccount(accountID string) *StateChangeBuilder {
	b.base.AccountID = accountID
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
	return fmt.Sprintf("%s-%d-%d-%d", b.base.AccountID, b.base.LedgerNumber, b.base.TransactionID, b.base.OperationID)
}
