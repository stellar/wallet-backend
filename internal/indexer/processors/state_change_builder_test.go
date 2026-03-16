package processors

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

// noopMetrics satisfies MetricsServiceInterface for tests without requiring full mock machinery.
type noopMetrics struct{}

func (noopMetrics) ObserveStateChangeProcessingDuration(string, float64) {}

// baseBuilder returns a fully-populated builder for testing.
// Every content field is set so that single-field mutations produce a unique hash.
func baseBuilder() *StateChangeBuilder {
	b := NewStateChangeBuilder(1, 1000, 42, noopMetrics{})
	b.WithCategory(types.StateChangeCategoryBalance).
		WithReason(types.StateChangeReasonCredit).
		WithAccount("GABC").
		WithOperationID(999).
		WithToken("CTOKEN").
		WithAmount("100").
		WithSigner("GSIGNER", int16Ptr(1), int16Ptr(2)).
		WithThreshold(int16Ptr(10), int16Ptr(20)).
		WithTrustlineLimit(strPtr("500"), strPtr("1000")).
		WithFlags([]string{"auth_required"}).
		WithKeyValue(map[string]any{"key": "value"}).
		WithDeployer("GDEPLOYER").
		WithFunder("GFUNDER").
		WithClaimableBalanceID("cb123").
		WithLiquidityPoolID("lp456").
		WithSponsoredData("mydata").
		WithSponsoredAccountID("GSPONSORED").
		WithSponsor("GSPONSOR")

	// SpenderAccountID has no builder method — set directly.
	b.base.SpenderAccountID = utils.NullAddressBytea("GSPENDER")

	return b
}

func int16Ptr(v int16) *int16 { return &v }

func TestStateChangeBuilder_Build_Determinism(t *testing.T) {
	sc1 := baseBuilder().Build()
	sc2 := baseBuilder().Build()

	assert.Equal(t, sc1.StateChangeID, sc2.StateChangeID,
		"identical inputs must produce the same StateChangeID")
}

func TestStateChangeBuilder_Build_Uniqueness(t *testing.T) {
	baseID := baseBuilder().Build().StateChangeID

	tests := []struct {
		name   string
		mutate func(b *StateChangeBuilder)
	}{
		{"category", func(b *StateChangeBuilder) { b.WithCategory(types.StateChangeCategorySigner) }},
		{"reason", func(b *StateChangeBuilder) { b.WithReason(types.StateChangeReasonDebit) }},
		{"account", func(b *StateChangeBuilder) { b.WithAccount("GOTHER") }},
		{"operationID", func(b *StateChangeBuilder) { b.WithOperationID(1000) }},
		{"token", func(b *StateChangeBuilder) { b.WithToken("COTHER") }},
		{"amount", func(b *StateChangeBuilder) { b.WithAmount("200") }},
		{"signerAccountID", func(b *StateChangeBuilder) { b.WithSigner("GOTHER", int16Ptr(1), int16Ptr(2)) }},
		{"signerWeightOld", func(b *StateChangeBuilder) { b.WithSigner("GSIGNER", int16Ptr(99), int16Ptr(2)) }},
		{"signerWeightNew", func(b *StateChangeBuilder) { b.WithSigner("GSIGNER", int16Ptr(1), int16Ptr(99)) }},
		{"thresholdOld", func(b *StateChangeBuilder) { b.WithThreshold(int16Ptr(99), int16Ptr(20)) }},
		{"thresholdNew", func(b *StateChangeBuilder) { b.WithThreshold(int16Ptr(10), int16Ptr(99)) }},
		{"trustlineLimitOld", func(b *StateChangeBuilder) { b.WithTrustlineLimit(strPtr("999"), strPtr("1000")) }},
		{"trustlineLimitNew", func(b *StateChangeBuilder) { b.WithTrustlineLimit(strPtr("500"), strPtr("999")) }},
		{"flags", func(b *StateChangeBuilder) { b.WithFlags([]string{"auth_revocable"}) }},
		{"keyValue", func(b *StateChangeBuilder) { b.WithKeyValue(map[string]any{"key": "other"}) }},
		{"deployer", func(b *StateChangeBuilder) { b.WithDeployer("GOTHER") }},
		{"funder", func(b *StateChangeBuilder) { b.WithFunder("GOTHER") }},
		{"claimableBalanceID", func(b *StateChangeBuilder) { b.WithClaimableBalanceID("cb999") }},
		{"liquidityPoolID", func(b *StateChangeBuilder) { b.WithLiquidityPoolID("lp999") }},
		{"sponsoredData", func(b *StateChangeBuilder) { b.WithSponsoredData("otherdata") }},
		{"sponsoredAccountID", func(b *StateChangeBuilder) { b.WithSponsoredAccountID("GOTHER") }},
		{"sponsorAccountID", func(b *StateChangeBuilder) { b.WithSponsor("GOTHER") }},
		{"spenderAccountID", func(b *StateChangeBuilder) {
			b.base.SpenderAccountID = utils.NullAddressBytea("GOTHER")
		}},
		{"toID", func(b *StateChangeBuilder) { b.base.ToID = 99 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := baseBuilder()
			tc.mutate(b)
			mutatedID := b.Build().StateChangeID

			assert.NotEqual(t, baseID, mutatedID,
				"changing %s must produce a different StateChangeID", tc.name)
		})
	}
}

func TestStateChangeBuilder_Build_NullVsZero(t *testing.T) {
	t.Run("NullInt16", func(t *testing.T) {
		// NULL (Valid: false)
		bNull := baseBuilder()
		bNull.base.SignerWeightOld = sql.NullInt16{Valid: false}
		nullID := bNull.Build().StateChangeID

		// Zero (Valid: true, Int16: 0)
		bZero := baseBuilder()
		bZero.base.SignerWeightOld = sql.NullInt16{Int16: 0, Valid: true}
		zeroID := bZero.Build().StateChangeID

		assert.NotEqual(t, nullID, zeroID,
			"NullInt16 NULL vs zero-value must produce different IDs")
	})

	t.Run("NullString", func(t *testing.T) {
		bNull := baseBuilder()
		bNull.base.Amount = sql.NullString{Valid: false}
		nullID := bNull.Build().StateChangeID

		bZero := baseBuilder()
		bZero.base.Amount = sql.NullString{String: "", Valid: true}
		zeroID := bZero.Build().StateChangeID

		assert.NotEqual(t, nullID, zeroID,
			"NullString NULL vs empty-string must produce different IDs")
	})

	t.Run("NullAddressBytea", func(t *testing.T) {
		bNull := baseBuilder()
		bNull.base.TokenID = types.NullAddressBytea{Valid: false}
		nullID := bNull.Build().StateChangeID

		bZero := baseBuilder()
		bZero.base.TokenID = types.NullAddressBytea{AddressBytea: "", Valid: true}
		zeroID := bZero.Build().StateChangeID

		assert.NotEqual(t, nullID, zeroID,
			"NullAddressBytea NULL vs empty must produce different IDs")
	})
}

func TestStateChangeBuilder_Build_PositiveID(t *testing.T) {
	sc := baseBuilder().Build()
	assert.Greater(t, sc.StateChangeID, int64(0),
		"StateChangeID must be positive (high bit masked off)")
}

func TestStateChangeBuilder_FluentAPI(t *testing.T) {
	t.Run("chainable", func(t *testing.T) {
		b := NewStateChangeBuilder(1, 1000, 42, noopMetrics{})

		// Each With* method should return the same builder pointer.
		require.Same(t, b, b.WithCategory(types.StateChangeCategoryBalance))
		require.Same(t, b, b.WithReason(types.StateChangeReasonCredit))
		require.Same(t, b, b.WithAccount("GABC"))
		require.Same(t, b, b.WithOperationID(1))
		require.Same(t, b, b.WithToken("CTOKEN"))
		require.Same(t, b, b.WithAmount("100"))
		require.Same(t, b, b.WithSigner("GSIGNER", int16Ptr(1), int16Ptr(2)))
		require.Same(t, b, b.WithThreshold(int16Ptr(10), int16Ptr(20)))
		require.Same(t, b, b.WithTrustlineLimit(strPtr("500"), strPtr("1000")))
		require.Same(t, b, b.WithFlags([]string{"auth_required"}))
		require.Same(t, b, b.WithKeyValue(map[string]any{"k": "v"}))
		require.Same(t, b, b.WithDeployer("GDEPLOYER"))
		require.Same(t, b, b.WithFunder("GFUNDER"))
		require.Same(t, b, b.WithClaimableBalanceID("cb"))
		require.Same(t, b, b.WithLiquidityPoolID("lp"))
		require.Same(t, b, b.WithSponsoredData("data"))
		require.Same(t, b, b.WithSponsoredAccountID("GSPONSORED"))
		require.Same(t, b, b.WithSponsor("GSPONSOR"))
	})

	t.Run("field values", func(t *testing.T) {
		sc := NewStateChangeBuilder(5, 2000, 77, noopMetrics{}).
			WithCategory(types.StateChangeCategorySigner).
			WithReason(types.StateChangeReasonAdd).
			WithAccount("GACC").
			WithOperationID(555).
			WithToken("CTOK").
			WithAmount("42").
			WithDeployer("GDEP").
			WithFunder("GFUN").
			WithClaimableBalanceID("cb1").
			WithLiquidityPoolID("lp1").
			WithSponsoredData("sd1").
			Build()

		assert.Equal(t, types.StateChangeCategorySigner, sc.StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonAdd, sc.StateChangeReason)
		assert.Equal(t, types.AddressBytea("GACC"), sc.AccountID)
		assert.Equal(t, int64(555), sc.OperationID)
		assert.Equal(t, "CTOK", string(sc.TokenID.AddressBytea))
		assert.True(t, sc.TokenID.Valid)
		assert.Equal(t, "42", sc.Amount.String)
		assert.True(t, sc.Amount.Valid)
		assert.Equal(t, "GDEP", string(sc.DeployerAccountID.AddressBytea))
		assert.Equal(t, "GFUN", string(sc.FunderAccountID.AddressBytea))
		assert.Equal(t, "cb1", sc.ClaimableBalanceID.String)
		assert.Equal(t, "lp1", sc.LiquidityPoolID.String)
		assert.Equal(t, "sd1", sc.SponsoredData.String)
	})
}
