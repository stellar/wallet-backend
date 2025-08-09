package resolvers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestStateChangeResolver_NullableStringFields(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	ctx := context.Background()

	t.Run("all valid", func(t *testing.T) {
		obj := &types.StateChange{
			TokenID:            sql.NullString{String: "token1", Valid: true},
			Amount:             sql.NullString{String: "100.5", Valid: true},
			ClaimableBalanceID: sql.NullString{String: "cb1", Valid: true},
			LiquidityPoolID:    sql.NullString{String: "lp1", Valid: true},
			OfferID:            sql.NullString{String: "offer1", Valid: true},
			SignerAccountID:    sql.NullString{String: "G-SIGNER", Valid: true},
			SpenderAccountID:   sql.NullString{String: "G-SPENDER", Valid: true},
			SponsoredAccountID: sql.NullString{String: "G-SPONSORED", Valid: true},
			SponsorAccountID:   sql.NullString{String: "G-SPONSOR", Valid: true},
		}

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "token1", *tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "100.5", *amount)

		cbID, err := resolver.ClaimableBalanceID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "cb1", *cbID)

		lpID, err := resolver.LiquidityPoolID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "lp1", *lpID)

		offerID, err := resolver.OfferID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "offer1", *offerID)

		signer, err := resolver.SignerAccountID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "G-SIGNER", *signer)

		spender, err := resolver.SpenderAccountID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "G-SPENDER", *spender)

		sponsored, err := resolver.SponsoredAccountID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "G-SPONSORED", *sponsored)

		sponsor, err := resolver.SponsorAccountID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "G-SPONSOR", *sponsor)
	})

	t.Run("all null", func(t *testing.T) {
		obj := &types.StateChange{} // All fields are zero-valued (Valid: false)

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, amount)

		cbID, err := resolver.ClaimableBalanceID(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, cbID)
	})
}

func TestStateChangeResolver_JSONFields(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	ctx := context.Background()

	t.Run("signer weights", func(t *testing.T) {
		obj := &types.StateChange{
			SignerWeights: types.NullableJSONB{"weight": 1},
		}
		expectedJSON, err := json.Marshal(obj.SignerWeights)
		require.NoError(t, err)

		jsonStr, err := resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), *jsonStr)

		obj.SignerWeights = nil
		jsonStr, err = resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, jsonStr)
	})

	t.Run("thresholds", func(t *testing.T) {
		obj := &types.StateChange{
			Thresholds: types.NullableJSONB{"low": 1, "med": 2},
		}
		expectedJSON, err := json.Marshal(obj.Thresholds)
		require.NoError(t, err)

		jsonStr, err := resolver.Thresholds(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), *jsonStr)
	})

	t.Run("flags", func(t *testing.T) {
		obj := &types.StateChange{
			Flags: types.NullableJSON{"auth_required", "auth_revocable"},
		}
		flags, err := resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, []string{"auth_required", "auth_revocable"}, flags)

		obj.Flags = nil
		flags, err = resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Empty(t, flags)
	})

	t.Run("key value", func(t *testing.T) {
		obj := &types.StateChange{
			KeyValue: types.NullableJSONB{"key": "value"},
		}
		expectedJSON, err := json.Marshal(obj.KeyValue)
		require.NoError(t, err)

		jsonStr, err := resolver.KeyValue(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), *jsonStr)
	})
}

func TestStateChangeResolver_Operation(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	parentSC := &types.StateChange{ToID: 1, StateChangeOrder: 1}
	expectedStateChangeID := "1-1"

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([]*types.Operation, []error) {
			assert.Equal(t, []dataloaders.OperationColumnsKey{{StateChangeID: expectedStateChangeID, Columns: "operations.id"}}, keys)
			return []*types.Operation{{ID: 99}}, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, parentSC)
		require.NoError(t, err)
		assert.Equal(t, int64(99), op.ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([]*types.Operation, []error) {
			return nil, []error{errors.New("op fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.Operation(ctx, parentSC)
		require.Error(t, err)
		assert.EqualError(t, err, "op fetch error")
	})
}

func TestStateChangeResolver_Transaction(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	parentSC := &types.StateChange{ToID: 2, StateChangeOrder: 3}
	expectedStateChangeID := "2-3"

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([]*types.Transaction, []error) {
			assert.Equal(t, []dataloaders.TransactionColumnsKey{{StateChangeID: expectedStateChangeID, Columns: "transactions.hash"}}, keys)
			return []*types.Transaction{{Hash: "tx-abc"}}, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, parentSC)
		require.NoError(t, err)
		assert.Equal(t, "tx-abc", tx.Hash)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([]*types.Transaction, []error) {
			return nil, []error{errors.New("tx fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		_, err := resolver.Transaction(ctx, parentSC)
		require.Error(t, err)
		assert.EqualError(t, err, "tx fetch error")
	})
}
