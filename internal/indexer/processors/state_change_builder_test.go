package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// noopMetrics satisfies MetricsServiceInterface for tests without requiring full mock machinery.
type noopMetrics struct{}

func (noopMetrics) ObserveStateChangeProcessingDuration(string, float64) {}

// baseBuilder returns a fully-populated builder for testing.
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

	return b
}

func int16Ptr(v int16) *int16 { return &v }

func TestStateChangeBuilder_Build_RandomID(t *testing.T) {
	sc1 := baseBuilder().Build()
	sc2 := baseBuilder().Build()

	assert.NotEqual(t, sc1.StateChangeID, sc2.StateChangeID,
		"each Build() call must produce a unique StateChangeID")
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
