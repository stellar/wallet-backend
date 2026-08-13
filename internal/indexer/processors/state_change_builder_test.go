package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func int16Ptr(v int16) *int16 { return &v }

func TestStateChangeBuilder_FluentAPI(t *testing.T) {
	t.Run("branching copies the builder", func(t *testing.T) {
		base := NewStateChangeBuilder(1, 1000, 42).WithCategory(types.StateChangeCategoryBalance)

		// Processors branch one operation-level builder into many state changes by
		// plain assignment, so a With* call must leave the builder it was called on
		// untouched and the branches independent of each other.
		credit := base.WithReason(types.StateChangeReasonCredit).WithAccount("GCREDIT")
		debit := base.WithReason(types.StateChangeReasonDebit).WithAccount("GDEBIT")

		assert.Equal(t, types.StateChangeReasonCredit, credit.Build().StateChangeReason)
		assert.Equal(t, types.AddressBytea("GCREDIT"), credit.Build().AccountID)
		assert.Equal(t, types.StateChangeReasonDebit, debit.Build().StateChangeReason)
		assert.Equal(t, types.AddressBytea("GDEBIT"), debit.Build().AccountID)

		assert.Empty(t, base.Build().StateChangeReason)
		assert.Empty(t, base.Build().AccountID)
		assert.Equal(t, types.StateChangeCategoryBalance, base.Build().StateChangeCategory)
	})

	t.Run("chainable", func(t *testing.T) {
		sc := NewStateChangeBuilder(1, 1000, 42).
			WithCategory(types.StateChangeCategoryBalance).
			WithReason(types.StateChangeReasonCredit).
			WithAccount("GABC").
			WithOperationID(1).
			WithToken("CTOKEN").
			WithAmount("100").
			WithSigner("GSIGNER", int16Ptr(1), int16Ptr(2)).
			WithThreshold(int16Ptr(10), int16Ptr(20)).
			WithTrustlineLimit(strPtr("500"), strPtr("1000")).
			WithFlags([]string{"auth_required"}).
			WithKeyValue(map[string]any{"k": "v"}).
			WithCreator("GCREATOR").
			WithLiquidityPoolID("lp").
			Build()

		assert.Equal(t, types.AddressBytea("GSIGNER"), sc.SignerAccountID.AddressBytea)
		assert.Equal(t, int16(1), sc.SignerWeightOld.Int16)
		assert.Equal(t, int16(2), sc.SignerWeightNew.Int16)
		assert.Equal(t, int16(10), sc.ThresholdOld.Int16)
		assert.Equal(t, int16(20), sc.ThresholdNew.Int16)
		assert.Equal(t, "500", sc.TrustlineLimitOld.String)
		assert.Equal(t, "1000", sc.TrustlineLimitNew.String)
		assert.Equal(t, types.EncodeFlagsToBitmask([]string{"auth_required"}), sc.Flags.Int16)
		assert.Equal(t, types.NullableJSONB{"k": "v"}, sc.KeyValue)
	})

	t.Run("field values", func(t *testing.T) {
		sc := NewStateChangeBuilder(5, 2000, 77).
			WithCategory(types.StateChangeCategorySigner).
			WithReason(types.StateChangeReasonAdd).
			WithAccount("GACC").
			WithOperationID(555).
			WithToken("CTOK").
			WithAmount("42").
			WithCreator("GCREAT").
			WithLiquidityPoolID("lp1").
			Build()

		assert.Equal(t, types.StateChangeCategorySigner, sc.StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonAdd, sc.StateChangeReason)
		assert.Equal(t, types.AddressBytea("GACC"), sc.AccountID)
		assert.Equal(t, int64(555), sc.OperationID)
		assert.Equal(t, "CTOK", string(sc.TokenID.AddressBytea))
		assert.True(t, sc.TokenID.Valid)
		assert.Equal(t, "42", sc.Amount.String)
		assert.True(t, sc.Amount.Valid)
		assert.Equal(t, "GCREAT", string(sc.CreatorAccountID.AddressBytea))
		assert.Equal(t, "lp1", sc.LiquidityPoolID.String)
	})
}
