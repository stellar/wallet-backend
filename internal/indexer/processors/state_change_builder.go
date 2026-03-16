// State change builder for creating token transfer state changes
// Provides a fluent interface for constructing state changes with proper field validation
package processors

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
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
			ToID:            txID,
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
	b.base.StateChangeReason = reason
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
	b.base.AccountID = types.AddressBytea(accountID)
	return b
}

// WithSigner sets the signer account ID and the weights directly
func (b *StateChangeBuilder) WithSigner(signer string, oldWeight, newWeight *int16) *StateChangeBuilder {
	b.base.SignerAccountID = utils.NullAddressBytea(signer)
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
	b.base.DeployerAccountID = utils.NullAddressBytea(deployer)
	return b
}

// WithFunder sets the funder account ID
func (b *StateChangeBuilder) WithFunder(funder string) *StateChangeBuilder {
	b.base.FunderAccountID = utils.NullAddressBytea(funder)
	return b
}

// WithSponsor sets the sponsor
func (b *StateChangeBuilder) WithSponsor(sponsor string) *StateChangeBuilder {
	b.base.SponsorAccountID = utils.NullAddressBytea(sponsor)
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
	b.base.TokenID = utils.NullAddressBytea(contractAddress)
	return b
}

// WithTokenType sets the token type (SAC or CUSTOM)
func (b *StateChangeBuilder) WithTokenType(tokenType types.ContractType) *StateChangeBuilder {
	b.base.ContractType = tokenType
	return b
}

// WithSponsoredAccountID sets the sponsored account ID for a sponsorship state change
func (b *StateChangeBuilder) WithSponsoredAccountID(sponsoredAccountID string) *StateChangeBuilder {
	b.base.SponsoredAccountID = utils.NullAddressBytea(sponsoredAccountID)
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

// Build returns the constructed state change with a deterministic content-based ID.
func (b *StateChangeBuilder) Build() types.StateChange {
	b.base.StateChangeID = b.computeHashID()
	return b.base
}

// computeHashID produces a deterministic FNV-64a hash of all content fields.
// It uses binary writes with null-tags (0x00 = NULL, 0x01 = present) to avoid
// ambiguity between NULL and zero-value fields.
func (b *StateChangeBuilder) computeHashID() int64 {
	h := fnv.New64a()

	// Non-nullable fields
	writeInt64(h, b.base.ToID)
	hashString(h, string(b.base.StateChangeCategory))
	hashString(h, string(b.base.StateChangeReason))
	hashString(h, string(b.base.AccountID))
	writeInt64(h, b.base.OperationID)

	// Nullable fields (addresses coerced to sql.NullString for uniform handling)
	writeNullString(h, b.base.TokenID.NullString())
	writeNullString(h, b.base.SignerAccountID.NullString())
	writeNullString(h, b.base.SpenderAccountID.NullString())
	writeNullString(h, b.base.SponsoredAccountID.NullString())
	writeNullString(h, b.base.SponsorAccountID.NullString())
	writeNullString(h, b.base.DeployerAccountID.NullString())
	writeNullString(h, b.base.FunderAccountID.NullString())
	writeNullString(h, b.base.Amount)
	writeNullString(h, b.base.ClaimableBalanceID)
	writeNullString(h, b.base.LiquidityPoolID)
	writeNullString(h, b.base.SponsoredData)
	writeNullString(h, b.base.TrustlineLimitOld)
	writeNullString(h, b.base.TrustlineLimitNew)

	// Nullable int16s
	writeNullInt16(h, b.base.SignerWeightOld)
	writeNullInt16(h, b.base.SignerWeightNew)
	writeNullInt16(h, b.base.ThresholdOld)
	writeNullInt16(h, b.base.ThresholdNew)
	writeNullInt16(h, b.base.Flags)

	// JSONB (canonical JSON marshal)
	keyValue, err := json.Marshal(b.base.KeyValue)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal key value: %v", err))
	}
	hashString(h, string(keyValue))

	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF)
}

// Binary hash helpers — each writes a null-tag byte followed by length-prefixed data.

func hashString(h hash.Hash, s string) {
	// Length prefix prevents collision between adjacent fields
	// (e.g. "ab"+"cd" vs "a"+"bcd").
	_, err := fmt.Fprint(h, len(s))
	if err != nil {
		panic(fmt.Sprintf("hashing state change string %q: %v", s, err))
	}
	h.Write([]byte{':'})
	h.Write([]byte(s))
}

func writeInt64(h hash.Hash, v int64) {
	hashString(h, strconv.FormatInt(v, 10))
}

func writeNullString(h hash.Hash, n sql.NullString) {
	if !n.Valid {
		h.Write([]byte{0x00})
		return
	}
	h.Write([]byte{0x01})
	hashString(h, n.String)
}

func writeNullInt16(h hash.Hash, n sql.NullInt16) {
	if !n.Valid {
		h.Write([]byte{0x00})
		return
	}
	h.Write([]byte{0x01})
	hashString(h, strconv.FormatInt(int64(n.Int16), 10))
}

// Clone returns a shallow copy of the builder, sharing the same metrics service.
func (b *StateChangeBuilder) Clone() *StateChangeBuilder {
	return &StateChangeBuilder{
		base:           b.base,
		metricsService: b.metricsService,
	}
}
