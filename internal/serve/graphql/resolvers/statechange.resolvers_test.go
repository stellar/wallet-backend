package resolvers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vikstrous/dataloadgen"
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

		tokenID, _ := resolver.TokenID(ctx, obj)
		assert.Equal(t, "token1", *tokenID)

		amount, _ := resolver.Amount(ctx, obj)
		assert.Equal(t, "100.5", *amount)

		cbID, _ := resolver.ClaimableBalanceID(ctx, obj)
		assert.Equal(t, "cb1", *cbID)

		lpID, _ := resolver.LiquidityPoolID(ctx, obj)
		assert.Equal(t, "lp1", *lpID)

		offerID, _ := resolver.OfferID(ctx, obj)
		assert.Equal(t, "offer1", *offerID)

		signer, _ := resolver.SignerAccountID(ctx, obj)
		assert.Equal(t, "G-SIGNER", *signer)

		spender, _ := resolver.SpenderAccountID(ctx, obj)
		assert.Equal(t, "G-SPENDER", *spender)

		sponsored, _ := resolver.SponsoredAccountID(ctx, obj)
		assert.Equal(t, "G-SPONSORED", *sponsored)

		sponsor, _ := resolver.SponsorAccountID(ctx, obj)
		assert.Equal(t, "G-SPONSOR", *sponsor)
	})

	t.Run("all null", func(t *testing.T) {
		obj := &types.StateChange{} // All fields are zero-valued (Valid: false)

		tokenID, _ := resolver.TokenID(ctx, obj)
		assert.Nil(t, tokenID)

		amount, _ := resolver.Amount(ctx, obj)
		assert.Nil(t, amount)

		cbID, _ := resolver.ClaimableBalanceID(ctx, obj)
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
		expectedJSON, _ := json.Marshal(obj.SignerWeights)

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
		expectedJSON, _ := json.Marshal(obj.Thresholds)

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
		expectedJSON, _ := json.Marshal(obj.KeyValue)

		jsonStr, err := resolver.KeyValue(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), *jsonStr)
	})
}

func TestStateChangeResolver_Operation(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	parentSC := &types.StateChange{ID: "test-sc-id"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([]*types.Operation, []error) {
			assert.Equal(t, []string{"test-sc-id"}, keys)
			return []*types.Operation{{ID: 99}}, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, parentSC)
		require.NoError(t, err)
		assert.Equal(t, int64(99), op.ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([]*types.Operation, []error) {
			return nil, []error{errors.New("op fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		_, err := resolver.Operation(ctx, parentSC)
		require.Error(t, err)
		assert.EqualError(t, err, "op fetch error")
	})
}

func TestStateChangeResolver_Transaction(t *testing.T) {
	resolver := &stateChangeResolver{&Resolver{}}
	parentSC := &types.StateChange{ID: "test-sc-id"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([]*types.Transaction, []error) {
			assert.Equal(t, []string{"test-sc-id"}, keys)
			return []*types.Transaction{{Hash: "tx-abc"}}, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, parentSC)
		require.NoError(t, err)
		assert.Equal(t, "tx-abc", tx.Hash)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([]*types.Transaction, []error) {
			return nil, []error{errors.New("tx fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionByStateChangeIDLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		_, err := resolver.Transaction(ctx, parentSC)
		require.Error(t, err)
		assert.EqualError(t, err, "tx fetch error")
	})
}
