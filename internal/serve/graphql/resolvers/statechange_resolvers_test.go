package resolvers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

// TestConvertStateChangeTypes is the dispatch matrix: every valid (category, reason)
// pair — plus the two column discriminators (BALANCE operation-vs-fee, ACCOUNT/CREATE
// deployer-vs-funder) — must resolve to exactly one concrete GraphQL model, and every
// invalid (category, reason) pair must surface an error naming the pair rather than a
// nil node (which would violate the non-null StateChangeEdge.node contract).
func TestConvertStateChangeTypes(t *testing.T) {
	validCases := []struct {
		name string
		sc   types.StateChange
		want any
	}{
		// BALANCE: the OperationID discriminator splits fee rows (op 0) from balance rows.
		{
			name: "BALANCE debit with no operation is a fee",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonDebit, OperationID: 0},
			want: &types.FeeChangeModel{},
		},
		{
			name: "BALANCE credit with no operation is a fee",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonCredit, OperationID: 0},
			want: &types.FeeChangeModel{},
		},
		{
			name: "BALANCE debit with an operation is a balance change",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonDebit, OperationID: 12345},
			want: &types.BalanceChangeModel{},
		},
		{
			name: "BALANCE mint with an operation is a balance change",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonMint, OperationID: 12345},
			want: &types.BalanceChangeModel{},
		},
		// ACCOUNT/CREATE: the DeployerAccountID discriminator splits contract deploys from account creation.
		{
			name: "ACCOUNT create with a deployer is a contract deployment",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryAccount, StateChangeReason: types.StateChangeReasonCreate, DeployerAccountID: types.NullAddressBytea{AddressBytea: types.AddressBytea(sharedTestAccountAddress), Valid: true}},
			want: &types.ContractDeployedChangeModel{},
		},
		{
			name: "ACCOUNT create without a deployer is an account creation",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryAccount, StateChangeReason: types.StateChangeReasonCreate},
			want: &types.AccountCreatedChangeModel{},
		},
		{
			name: "ACCOUNT merge",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryAccount, StateChangeReason: types.StateChangeReasonMerge},
			want: &types.AccountMergedChangeModel{},
		},
		// SIGNER: one model per reason.
		{
			name: "SIGNER add",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategorySigner, StateChangeReason: types.StateChangeReasonAdd},
			want: &types.SignerAddedChangeModel{},
		},
		{
			name: "SIGNER update",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategorySigner, StateChangeReason: types.StateChangeReasonUpdate},
			want: &types.SignerUpdatedChangeModel{},
		},
		{
			name: "SIGNER remove",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategorySigner, StateChangeReason: types.StateChangeReasonRemove},
			want: &types.SignerRemovedChangeModel{},
		},
		// SIGNATURE_THRESHOLD / FLAGS / RESERVES / BALANCE_AUTHORIZATION: any reason maps to one model.
		{
			name: "SIGNATURE_THRESHOLD low",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategorySignatureThreshold, StateChangeReason: types.StateChangeReasonLow},
			want: &types.ThresholdChangeModel{},
		},
		{
			name: "SIGNATURE_THRESHOLD high",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategorySignatureThreshold, StateChangeReason: types.StateChangeReasonHigh},
			want: &types.ThresholdChangeModel{},
		},
		{
			name: "FLAGS set",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryFlags, StateChangeReason: types.StateChangeReasonSet},
			want: &types.AccountFlagsChangeModel{},
		},
		{
			name: "FLAGS clear",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryFlags, StateChangeReason: types.StateChangeReasonClear},
			want: &types.AccountFlagsChangeModel{},
		},
		// METADATA: one model per reason.
		{
			name: "METADATA home domain",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryMetadata, StateChangeReason: types.StateChangeReasonHomeDomain},
			want: &types.HomeDomainChangeModel{},
		},
		{
			name: "METADATA data entry",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryMetadata, StateChangeReason: types.StateChangeReasonDataEntry},
			want: &types.DataEntryChangeModel{},
		},
		{
			name: "METADATA update is a SEP-41 allowance",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryMetadata, StateChangeReason: types.StateChangeReasonUpdate},
			want: &types.AllowanceChangeModel{},
		},
		// TRUSTLINE: one model per reason.
		{
			name: "TRUSTLINE add",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryTrustline, StateChangeReason: types.StateChangeReasonAdd},
			want: &types.TrustlineAddedChangeModel{},
		},
		{
			name: "TRUSTLINE update",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryTrustline, StateChangeReason: types.StateChangeReasonUpdate},
			want: &types.TrustlineUpdatedChangeModel{},
		},
		{
			name: "TRUSTLINE remove",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryTrustline, StateChangeReason: types.StateChangeReasonRemove},
			want: &types.TrustlineRemovedChangeModel{},
		},
		{
			name: "RESERVES sponsor",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryReserves, StateChangeReason: types.StateChangeReasonSponsor},
			want: &types.SponsorshipChangeModel{},
		},
		{
			name: "RESERVES unsponsor",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryReserves, StateChangeReason: types.StateChangeReasonUnsponsor},
			want: &types.SponsorshipChangeModel{},
		},
		{
			name: "BALANCE_AUTHORIZATION set",
			sc:   types.StateChange{StateChangeCategory: types.StateChangeCategoryBalanceAuthorization, StateChangeReason: types.StateChangeReasonSet},
			want: &types.BalanceAuthorizationChangeModel{},
		},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertStateChangeTypes(tc.sc)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.IsType(t, tc.want, got)
		})
	}

	// Invalid (category, reason) pairs: reasons outside a category's switch arms have no
	// concrete type and must error, naming both category and reason.
	errorCases := []struct {
		name     string
		sc       types.StateChange
		category string
		reason   string
	}{
		{
			name:     "ACCOUNT with a balance reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryAccount, StateChangeReason: types.StateChangeReasonDebit},
			category: "ACCOUNT",
			reason:   "DEBIT",
		},
		{
			name:     "BALANCE with an account reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonMerge, OperationID: 12345},
			category: "BALANCE",
			reason:   "MERGE",
		},
		{
			name:     "BALANCE with an account reason and no operation is not a fee",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryBalance, StateChangeReason: types.StateChangeReasonMerge, OperationID: 0},
			category: "BALANCE",
			reason:   "MERGE",
		},
		{
			name:     "SIGNATURE_THRESHOLD with a sponsor reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategorySignatureThreshold, StateChangeReason: types.StateChangeReasonSponsor},
			category: "SIGNATURE_THRESHOLD",
			reason:   "SPONSOR",
		},
		{
			name:     "FLAGS with a threshold reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryFlags, StateChangeReason: types.StateChangeReasonLow},
			category: "FLAGS",
			reason:   "LOW",
		},
		{
			name:     "RESERVES with a flags reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryReserves, StateChangeReason: types.StateChangeReasonSet},
			category: "RESERVES",
			reason:   "SET",
		},
		{
			name:     "BALANCE_AUTHORIZATION with a sponsor reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryBalanceAuthorization, StateChangeReason: types.StateChangeReasonSponsor},
			category: "BALANCE_AUTHORIZATION",
			reason:   "SPONSOR",
		},
		{
			name:     "SIGNER with a flags reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategorySigner, StateChangeReason: types.StateChangeReasonSet},
			category: "SIGNER",
			reason:   "SET",
		},
		{
			name:     "METADATA with an account reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryMetadata, StateChangeReason: types.StateChangeReasonCreate},
			category: "METADATA",
			reason:   "CREATE",
		},
		{
			name:     "TRUSTLINE with a merge reason",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategoryTrustline, StateChangeReason: types.StateChangeReasonMerge},
			category: "TRUSTLINE",
			reason:   "MERGE",
		},
		{
			name:     "unknown category",
			sc:       types.StateChange{StateChangeCategory: types.StateChangeCategory("MYSTERY"), StateChangeReason: types.StateChangeReasonAdd},
			category: "MYSTERY",
			reason:   "ADD",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertStateChangeTypes(tc.sc)
			require.Error(t, err)
			assert.Nil(t, got, "an unmapped row must not produce a node")
			assert.Contains(t, err.Error(), tc.category)
			assert.Contains(t, err.Error(), tc.reason)
		})
	}
}

func TestSignerResolvers_WeightNullability(t *testing.T) {
	ctx := context.Background()

	t.Run("SignerAddedChange exposes only newWeight (required)", func(t *testing.T) {
		r := &signerAddedChangeResolver{&Resolver{}}

		obj := &types.SignerAddedChangeModel{StateChange: types.StateChange{SignerWeightNew: sql.NullInt16{Int16: 5, Valid: true}}}
		w, err := r.NewWeight(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, int32(5), w)

		missing := &types.SignerAddedChangeModel{StateChange: types.StateChange{SignerWeightNew: sql.NullInt16{Valid: false}}}
		_, err = r.NewWeight(ctx, missing)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "newWeight")
	})

	t.Run("SignerRemovedChange exposes only oldWeight (nullable)", func(t *testing.T) {
		r := &signerRemovedChangeResolver{&Resolver{}}

		obj := &types.SignerRemovedChangeModel{StateChange: types.StateChange{SignerWeightOld: sql.NullInt16{Int16: 3, Valid: true}}}
		w, err := r.OldWeight(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, w)
		assert.Equal(t, int32(3), *w)

		missing := &types.SignerRemovedChangeModel{StateChange: types.StateChange{SignerWeightOld: sql.NullInt16{Valid: false}}}
		w, err = r.OldWeight(ctx, missing)
		require.NoError(t, err)
		assert.Nil(t, w)
	})

	t.Run("SignerUpdatedChange has nullable oldWeight and required newWeight", func(t *testing.T) {
		r := &signerUpdatedChangeResolver{&Resolver{}}

		obj := &types.SignerUpdatedChangeModel{StateChange: types.StateChange{
			SignerWeightOld: sql.NullInt16{Valid: false},
			SignerWeightNew: sql.NullInt16{Int16: 7, Valid: true},
		}}
		old, err := r.OldWeight(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, old, "an updated signer whose prior weight is unknown surfaces null oldWeight")

		newW, err := r.NewWeight(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, int32(7), newW)
	})
}

func TestThresholdChangeResolver(t *testing.T) {
	ctx := context.Background()
	r := &thresholdChangeResolver{&Resolver{}}

	t.Run("oldThreshold is nil when absent, newThreshold required", func(t *testing.T) {
		obj := &types.ThresholdChangeModel{StateChange: types.StateChange{
			ThresholdOld: sql.NullInt16{Valid: false},
			ThresholdNew: sql.NullInt16{Int16: 2, Valid: true},
		}}
		old, err := r.OldThreshold(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, old)

		newT, err := r.NewThreshold(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, int32(2), newT)
	})

	t.Run("newThreshold errors when null", func(t *testing.T) {
		obj := &types.ThresholdChangeModel{StateChange: types.StateChange{ThresholdNew: sql.NullInt16{Valid: false}}}
		_, err := r.NewThreshold(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "newThreshold")
	})
}

func TestHomeDomainChangeResolver(t *testing.T) {
	ctx := context.Background()
	r := &homeDomainChangeResolver{&Resolver{}}

	t.Run("extracts old and new from key_value", func(t *testing.T) {
		obj := &types.HomeDomainChangeModel{StateChange: types.StateChange{
			KeyValue: types.NullableJSONB{"home_domain": map[string]any{"old": "a.com", "new": "b.com"}},
		}}
		old, err := r.OldHomeDomain(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, old)
		assert.Equal(t, "a.com", *old)

		newD, err := r.NewHomeDomain(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, newD)
		assert.Equal(t, "b.com", *newD)
	})

	t.Run("old is nil when only new is present", func(t *testing.T) {
		obj := &types.HomeDomainChangeModel{StateChange: types.StateChange{
			KeyValue: types.NullableJSONB{"home_domain": map[string]any{"new": "b.com"}},
		}}
		old, err := r.OldHomeDomain(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, old)

		newD, err := r.NewHomeDomain(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, newD)
		assert.Equal(t, "b.com", *newD)
	})
}

func TestDataEntryChangeResolver(t *testing.T) {
	ctx := context.Background()
	r := &dataEntryChangeResolver{&Resolver{}}

	t.Run("name is the single key, old/new nested under it", func(t *testing.T) {
		obj := &types.DataEntryChangeModel{StateChange: types.StateChange{
			KeyValue: types.NullableJSONB{"config.setting": map[string]any{"old": "v1", "new": "v2"}},
		}}
		name, err := r.Name(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "config.setting", name)

		old, err := r.OldValue(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, old)
		assert.Equal(t, "v1", *old)

		newV, err := r.NewValue(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, newV)
		assert.Equal(t, "v2", *newV)
	})

	t.Run("name errors when key_value is empty", func(t *testing.T) {
		obj := &types.DataEntryChangeModel{StateChange: types.StateChange{KeyValue: types.NullableJSONB{}}}
		_, err := r.Name(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "name")
	})
}

func TestAllowanceChangeResolver_ExpirationLedger(t *testing.T) {
	ctx := context.Background()
	r := &allowanceChangeResolver{&Resolver{}}

	t.Run("reads live_until_ledger as float64 from key_value", func(t *testing.T) {
		// JSONB unmarshal exposes numbers as float64, which is what the resolver must accept.
		obj := &types.AllowanceChangeModel{StateChange: types.StateChange{
			KeyValue: types.NullableJSONB{"live_until_ledger": float64(1234567)},
		}}
		exp, err := r.ExpirationLedger(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, uint32(1234567), exp)
	})

	t.Run("errors when live_until_ledger is missing", func(t *testing.T) {
		obj := &types.AllowanceChangeModel{StateChange: types.StateChange{KeyValue: types.NullableJSONB{"other": float64(1)}}}
		_, err := r.ExpirationLedger(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expirationLedger")
	})

	t.Run("errors when live_until_ledger is out of uint32 range", func(t *testing.T) {
		obj := &types.AllowanceChangeModel{StateChange: types.StateChange{KeyValue: types.NullableJSONB{"live_until_ledger": float64(-1)}}}
		_, err := r.ExpirationLedger(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range")
	})
}

func TestFeeChangeResolver_OperationAlwaysNil(t *testing.T) {
	// Fees are charged per transaction, not per operation, so the schema declares
	// FeeChange.operation non-existent; the resolver must return (nil, nil) unconditionally,
	// even when the row happens to carry an operation id.
	ctx := context.Background()
	r := &feeChangeResolver{&Resolver{}}
	obj := &types.FeeChangeModel{StateChange: types.StateChange{OperationID: 999}}

	op, err := r.Operation(ctx, obj)
	require.NoError(t, err)
	assert.Nil(t, op)
}

func TestBalanceChangeResolver_RequiredTokenID(t *testing.T) {
	ctx := context.Background()
	r := &balanceChangeResolver{&Resolver{}}

	t.Run("returns the strkey when present", func(t *testing.T) {
		obj := &types.BalanceChangeModel{StateChange: types.StateChange{
			TokenID: types.NullAddressBytea{AddressBytea: types.AddressBytea(MainnetNativeContractAddress), Valid: true},
		}}
		tokenID, err := r.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, MainnetNativeContractAddress, tokenID)
	})

	t.Run("errors when the backing column is null", func(t *testing.T) {
		obj := &types.BalanceChangeModel{StateChange: types.StateChange{TokenID: types.NullAddressBytea{Valid: false}}}
		_, err := r.TokenID(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tokenId")
	})
}

func TestRequiredStringResolvers_ErrorWhenNull(t *testing.T) {
	ctx := context.Background()

	t.Run("BalanceChange amount", func(t *testing.T) {
		r := &balanceChangeResolver{&Resolver{}}
		obj := &types.BalanceChangeModel{StateChange: types.StateChange{Amount: sql.NullString{Valid: false}}}
		_, err := r.Amount(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "amount")
	})

	t.Run("TrustlineAddedChange limit", func(t *testing.T) {
		r := &trustlineAddedChangeResolver{&Resolver{}}
		obj := &types.TrustlineAddedChangeModel{StateChange: types.StateChange{TrustlineLimitNew: sql.NullString{Valid: false}}}
		_, err := r.Limit(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("TrustlineUpdatedChange oldLimit and newLimit", func(t *testing.T) {
		r := &trustlineUpdatedChangeResolver{&Resolver{}}
		obj := &types.TrustlineUpdatedChangeModel{StateChange: types.StateChange{}}
		_, err := r.OldLimit(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "oldLimit")
		_, err = r.NewLimit(ctx, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "newLimit")
	})

	t.Run("valid amount resolves", func(t *testing.T) {
		r := &feeChangeResolver{&Resolver{}}
		obj := &types.FeeChangeModel{StateChange: types.StateChange{Amount: sql.NullString{String: "100", Valid: true}}}
		amount, err := r.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "100", amount)
	})
}

func TestBalanceAuthorizationChangeResolver_Flags(t *testing.T) {
	ctx := context.Background()
	r := &balanceAuthorizationChangeResolver{&Resolver{}}

	t.Run("nil when the flags column is null (SAC contract-holder authorization)", func(t *testing.T) {
		obj := &types.BalanceAuthorizationChangeModel{StateChange: types.StateChange{Flags: sql.NullInt16{Valid: false}}}
		flags, err := r.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, flags)
	})

	t.Run("decodes only the trustline bits in fixed order", func(t *testing.T) {
		// authorized (1) | clawback_enabled (32) = 33
		obj := &types.BalanceAuthorizationChangeModel{StateChange: types.StateChange{
			Flags: sql.NullInt16{Int16: types.FlagBitAuthorized | types.FlagBitClawbackEnabled, Valid: true},
		}}
		flags, err := r.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, []types.TrustlineFlag{types.TrustlineFlagAuthorized, types.TrustlineFlagClawbackEnabled}, flags)
	})
}

func TestAccountFlagsChangeResolver_Flags(t *testing.T) {
	ctx := context.Background()
	r := &accountFlagsChangeResolver{&Resolver{}}

	t.Run("nil when the flags column is null", func(t *testing.T) {
		obj := &types.AccountFlagsChangeModel{StateChange: types.StateChange{Flags: sql.NullInt16{Valid: false}}}
		flags, err := r.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, flags)
	})

	t.Run("decodes only the account bits in fixed order", func(t *testing.T) {
		// auth_required (2) | auth_revocable (4) = 6
		obj := &types.AccountFlagsChangeModel{StateChange: types.StateChange{
			Flags: sql.NullInt16{Int16: types.FlagBitAuthRequired | types.FlagBitAuthRevocable, Valid: true},
		}}
		flags, err := r.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, []types.AccountFlag{types.AccountFlagAuthRequired, types.AccountFlagAuthRevocable}, flags)
	})
}

// The remaining tests exercise the shared BaseStateChange resolvers (account/operation/
// transaction) through one concrete type, balanceChangeResolver, and the end-to-end
// conversion from a real state_changes row into its GraphQL model. They need the package
// test DB set up by TestMain.

func TestStateChangeResolver_Account(t *testing.T) {
	r := &balanceChangeResolver{&Resolver{}}

	t.Run("success", func(t *testing.T) {
		obj := &types.BalanceChangeModel{StateChange: types.StateChange{
			AccountID:           types.AddressBytea(sharedTestAccountAddress),
			StateChangeCategory: types.StateChangeCategoryBalance,
		}}
		account, err := r.Account(context.Background(), obj)
		require.NoError(t, err)
		assert.Equal(t, sharedTestAccountAddress, string(account.StellarAddress))
	})

	t.Run("nil state change panics", func(t *testing.T) {
		assert.Panics(t, func() {
			_, _ = r.Account(context.Background(), nil) //nolint:errcheck
		})
	})

	t.Run("empty account_id returns error", func(t *testing.T) {
		obj := &types.BalanceChangeModel{StateChange: types.StateChange{
			AccountID:           "",
			StateChangeCategory: types.StateChangeCategoryBalance,
		}}
		account, err := r.Account(context.Background(), obj)
		require.Error(t, err)
		assert.Nil(t, account)
		assert.Contains(t, err.Error(), "state change has no account_id")
	})
}

func TestStateChangeResolver_Operation(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	r := &balanceChangeResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
	}}
	opID := toid.New(1000, 1, 1).ToInt64()
	txToID := opID &^ 0xFFF // Derive transaction to_id from operation_id using TOID bitmask
	obj := &types.BalanceChangeModel{StateChange: types.StateChange{
		ToID:                txToID,
		OperationID:         opID,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		LedgerCreatedAt:     sharedTestLedgerCreatedAt,
	}}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := r.Operation(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, opID, op.ID)
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = r.Operation(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("non-existent operation is a data-integrity error", func(t *testing.T) {
		nonExistent := &types.BalanceChangeModel{StateChange: types.StateChange{
			ToID:                9999,
			OperationID:         0,
			StateChangeID:       1,
			StateChangeCategory: types.StateChangeCategoryBalance,
		}}
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := r.Operation(ctx, nonExistent)
		require.Error(t, err, "operation is non-null on every type using this resolver; a loader miss must surface, not nullify")
		assert.Contains(t, err.Error(), "not found")
		assert.Nil(t, op)
	})
}

func TestStateChangeResolver_Transaction(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	r := &balanceChangeResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
	}}
	obj := &types.BalanceChangeModel{StateChange: types.StateChange{
		ToID:                toid.New(1000, 1, 0).ToInt64(),
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		LedgerCreatedAt:     sharedTestLedgerCreatedAt,
	}}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := r.Transaction(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, testTxHash1, tx.Hash.String())
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = r.Transaction(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("non-existent transaction is a data-integrity error", func(t *testing.T) {
		nonExistent := &types.BalanceChangeModel{StateChange: types.StateChange{
			ToID:                9999,
			StateChangeID:       1,
			StateChangeCategory: types.StateChangeCategoryBalance,
		}}
		loaders := dataloaders.NewDataloaders(r.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := r.Transaction(ctx, nonExistent)
		require.Error(t, err, "transaction is non-null on every concrete type; a loader miss must surface, not nullify")
		assert.Contains(t, err.Error(), "not found")
		assert.Nil(t, tx)
	})
}

func TestAccountResolver_SEP41TransferSurfacesAsBalanceChange(t *testing.T) {
	// A SEP-41 transfer becomes a state_changes row with category=BALANCE, reason=CREDIT, a
	// token id, an amount, a non-zero operation id, and a to_muxed_id. Through Account.stateChanges
	// it must come back as the concrete BalanceChangeModel (operation-sourced, so not a FeeChange)
	// with those fields intact.
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	execTestDB(t, `DELETE FROM state_changes WHERE account_id = $1::bytea`, mustAddressBytes(t, acct))
	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM state_changes WHERE account_id = $1::bytea`, mustAddressBytes(t, acct))
	})

	execTestDB(t, `
		INSERT INTO state_changes (
			to_id, state_change_id, state_change_category, state_change_reason,
			ledger_created_at, ledger_number, account_id, operation_id,
			token_id, amount, to_muxed_id
		) VALUES ($1, $2, $3, $4, NOW(), $5, $6::bytea, $7, $8::bytea, $9, $10)
	`,
		int64(42<<32), int64(1),
		string(types.StateChangeCategoryBalance), string(types.StateChangeReasonCredit),
		uint32(100), mustAddressBytes(t, acct), int64((42<<32)|1),
		mustAddressBytes(t, contractAddr), "500",
		"18446744073709551615", // u64 max, proves the TEXT column handles values >2^63
	)

	m := metrics.NewMetrics(prometheus.NewRegistry())

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
		metrics: m,
	}}

	ctx := getTestCtx("stateChanges", []string{
		"category", "reason", "tokenId", "amount", "toMuxedId", "ledgerNumber",
	})

	first := int32(10)
	conn, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &first, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Len(t, conn.Edges, 1)

	bc, ok := conn.Edges[0].Node.(*types.BalanceChangeModel)
	require.True(t, ok, "edge[0] should be BalanceChangeModel, got %T", conn.Edges[0].Node)
	assert.Equal(t, types.StateChangeCategoryBalance, bc.StateChangeCategory)
	assert.Equal(t, types.StateChangeReasonCredit, bc.StateChangeReason)
	assert.Equal(t, contractAddr, bc.TokenID.String())
	assert.True(t, bc.Amount.Valid)
	assert.Equal(t, "500", bc.Amount.String)
	assert.True(t, bc.ToMuxedID.Valid)
	assert.Equal(t, "18446744073709551615", bc.ToMuxedID.String)
}
