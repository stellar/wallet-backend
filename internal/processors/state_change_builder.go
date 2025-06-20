// State change builder for creating token transfer state changes
// Provides a fluent interface for constructing state changes with proper field validation
package processors

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/stellar/go/asset"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// StateChangeBuilder provides a fluent interface for creating state changes
type StateChangeBuilder struct {
	base types.StateChange
}

// NewStateChangeBuilder creates a new builder with base state change fields
func NewStateChangeBuilder(ledgerNumber uint32, ledgerCloseTime int64, txHash, opID string) *StateChangeBuilder {
	return &StateChangeBuilder{
		base: types.StateChange{
			LedgerNumber:    int64(ledgerNumber),
			LedgerCreatedAt: time.Unix(ledgerCloseTime, 0),
			IngestedAt:      time.Now(),
			TxHash:          txHash,
			OperationID:     opID,
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
	b.base.Amount = sql.NullString{String: amount}
	return b
}

// WithAsset sets the asset or contract
func (b *StateChangeBuilder) WithAsset(asset *asset.Asset, contractAddress string) *StateChangeBuilder {
	if asset != nil {
		if asset.GetNative() {
			b.base.Token = sql.NullString{String: "native"}
		} else if issuedAsset := asset.GetIssuedAsset(); issuedAsset != nil {
			b.base.Token = sql.NullString{String: fmt.Sprintf("%s:%s", issuedAsset.GetAssetCode(), issuedAsset.GetIssuer())}
		}
	} else {
		b.base.ContractID = sql.NullString{String: contractAddress}
	}
	return b
}

// WithClaimableBalance sets the claimable balance ID
func (b *StateChangeBuilder) WithClaimableBalance(balanceID string) *StateChangeBuilder {
	b.base.ClaimableBalanceID = sql.NullString{String: balanceID, Valid: true}
	return b
}

// WithLiquidityPool sets the liquidity pool ID
func (b *StateChangeBuilder) WithLiquidityPool(poolID string) *StateChangeBuilder {
	b.base.LiquidityPoolID = sql.NullString{String: poolID, Valid: true}
	return b
}

// Build returns the constructed state change
func (b *StateChangeBuilder) Build() types.StateChange {
	return b.base
}
