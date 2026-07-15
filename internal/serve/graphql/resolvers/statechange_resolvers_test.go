package resolvers

import (
	"context"
	"database/sql"
	"encoding/json"
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

func TestStateChangeResolver_NullableStringFields(t *testing.T) {
	resolver := &standardBalanceChangeResolver{&Resolver{}}
	ctx := context.Background()

	t.Run("all valid", func(t *testing.T) {
		obj := &types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				TokenID: types.NullAddressBytea{AddressBytea: types.AddressBytea(MainnetNativeContractAddress), Valid: true},
				Amount:  sql.NullString{String: "100.5", Valid: true},
			},
		}

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, MainnetNativeContractAddress, tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "100.5", amount)
	})

	t.Run("all null", func(t *testing.T) {
		obj := &types.StandardBalanceStateChangeModel{} // All fields are zero-valued (Valid: false)

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "", tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "", amount)
	})
}

func TestStateChangeResolver_TypedFields(t *testing.T) {
	ctx := context.Background()

	t.Run("signer weights with values", func(t *testing.T) {
		resolver := &signerChangeResolver{&Resolver{}}
		obj := &types.SignerStateChangeModel{
			StateChange: types.StateChange{
				SignerWeightOld: sql.NullInt16{Int16: 10, Valid: true},
				SignerWeightNew: sql.NullInt16{Int16: 5, Valid: true},
			},
		}

		jsonStr, err := resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, jsonStr)
		assert.JSONEq(t, `{"old": 10, "new": 5}`, *jsonStr)
	})

	t.Run("signer weights null when both invalid", func(t *testing.T) {
		resolver := &signerChangeResolver{&Resolver{}}
		obj := &types.SignerStateChangeModel{
			StateChange: types.StateChange{
				SignerWeightOld: sql.NullInt16{Valid: false},
				SignerWeightNew: sql.NullInt16{Valid: false},
			},
		}

		jsonStr, err := resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, jsonStr)
	})

	t.Run("thresholds", func(t *testing.T) {
		resolver := &signerThresholdsChangeResolver{&Resolver{}}
		obj := &types.SignerThresholdsStateChangeModel{
			StateChange: types.StateChange{
				ThresholdOld: sql.NullInt16{Int16: 1, Valid: true},
				ThresholdNew: sql.NullInt16{Int16: 2, Valid: true},
			},
		}

		jsonStr, err := resolver.Thresholds(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, `{"old": "1", "new": "2"}`, jsonStr)
	})

	t.Run("flags with bitmask", func(t *testing.T) {
		resolver := &flagsChangeResolver{&Resolver{}}
		// Bitmask for auth_required (2) | auth_revocable (4) = 6
		obj := &types.FlagsStateChangeModel{
			StateChange: types.StateChange{
				Flags: sql.NullInt16{Int16: 6, Valid: true},
			},
		}
		flags, err := resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"auth_required", "auth_revocable"}, flags)
	})

	t.Run("flags empty when invalid", func(t *testing.T) {
		resolver := &flagsChangeResolver{&Resolver{}}
		obj := &types.FlagsStateChangeModel{
			StateChange: types.StateChange{
				Flags: sql.NullInt16{Valid: false},
			},
		}
		flags, err := resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Empty(t, flags)
	})

	t.Run("balance authorization flags with bitmask", func(t *testing.T) {
		resolver := &balanceAuthorizationChangeResolver{&Resolver{}}
		// Bitmask for authorized (1) | clawback_enabled (32) = 33
		obj := &types.BalanceAuthorizationStateChangeModel{
			StateChange: types.StateChange{
				Flags: sql.NullInt16{Int16: 33, Valid: true},
			},
		}
		flags, err := resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"authorized", "clawback_enabled"}, flags)
	})

	t.Run("trustline limit with values", func(t *testing.T) {
		resolver := &trustlineChangeResolver{&Resolver{}}
		obj := &types.TrustlineStateChangeModel{
			StateChange: types.StateChange{
				TrustlineLimitOld: sql.NullString{String: "1000000", Valid: true},
				TrustlineLimitNew: sql.NullString{String: "2000000", Valid: true},
			},
		}

		jsonStr, err := resolver.Limit(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, jsonStr)
		assert.JSONEq(t, `{"old": "1000000", "new": "2000000"}`, *jsonStr)
	})

	t.Run("trustline limit null when both invalid", func(t *testing.T) {
		resolver := &trustlineChangeResolver{&Resolver{}}
		obj := &types.TrustlineStateChangeModel{
			StateChange: types.StateChange{
				TrustlineLimitOld: sql.NullString{Valid: false},
				TrustlineLimitNew: sql.NullString{Valid: false},
			},
		}

		jsonStr, err := resolver.Limit(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, jsonStr)
	})

	t.Run("trustline limit with partial values", func(t *testing.T) {
		resolver := &trustlineChangeResolver{&Resolver{}}
		obj := &types.TrustlineStateChangeModel{
			StateChange: types.StateChange{
				TrustlineLimitOld: sql.NullString{Valid: false},
				TrustlineLimitNew: sql.NullString{String: "5000000", Valid: true},
			},
		}

		jsonStr, err := resolver.Limit(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, jsonStr)
		assert.JSONEq(t, `{"old": null, "new": "5000000"}`, *jsonStr)
	})

	t.Run("trustline liquidity pool id valid", func(t *testing.T) {
		resolver := &trustlineChangeResolver{&Resolver{}}
		obj := &types.TrustlineStateChangeModel{
			StateChange: types.StateChange{
				LiquidityPoolID: sql.NullString{String: "abc123poolid", Valid: true},
			},
		}

		result, err := resolver.LiquidityPoolID(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "abc123poolid", *result)
	})

	t.Run("trustline liquidity pool id null when invalid", func(t *testing.T) {
		resolver := &trustlineChangeResolver{&Resolver{}}
		obj := &types.TrustlineStateChangeModel{
			StateChange: types.StateChange{
				LiquidityPoolID: sql.NullString{Valid: false},
			},
		}

		result, err := resolver.LiquidityPoolID(ctx, obj)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("key value", func(t *testing.T) {
		resolver := &metadataChangeResolver{&Resolver{}}
		obj := &types.MetadataStateChangeModel{
			StateChange: types.StateChange{
				KeyValue: types.NullableJSONB{"key": "value"},
			},
		}
		expectedJSON, err := json.Marshal(obj.KeyValue)
		require.NoError(t, err)

		jsonStr, err := resolver.KeyValue(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), jsonStr)
	})
}

func TestStateChangeResolver_AccountAddressFields(t *testing.T) {
	ctx := context.Background()
	resolver := &accountChangeResolver{&Resolver{}}

	t.Run("funder and deployer populated", func(t *testing.T) {
		obj := &types.AccountStateChangeModel{
			StateChange: types.StateChange{
				FunderAccountID:   types.NullAddressBytea{AddressBytea: types.AddressBytea(sharedTestAccountAddress), Valid: true},
				DeployerAccountID: types.NullAddressBytea{AddressBytea: types.AddressBytea(sharedTestAccountAddress), Valid: true},
			},
		}

		funder, err := resolver.FunderAddress(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, funder)
		assert.Equal(t, sharedTestAccountAddress, *funder)

		deployer, err := resolver.DeployerAddress(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, deployer)
		assert.Equal(t, sharedTestAccountAddress, *deployer)
	})

	t.Run("classic account change and deployerAddress is null", func(t *testing.T) {
		obj := &types.AccountStateChangeModel{}

		deployer, err := resolver.DeployerAddress(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, deployer)
	})

	t.Run("merge: destination is surfaced", func(t *testing.T) {
		obj := &types.AccountStateChangeModel{
			StateChange: types.StateChange{
				DestinationAccountID: types.NullAddressBytea{AddressBytea: types.AddressBytea(sharedTestAccountAddress), Valid: true},
			},
		}

		destination, err := resolver.DestinationAddress(ctx, obj)
		require.NoError(t, err)
		require.NotNil(t, destination)
		assert.Equal(t, sharedTestAccountAddress, *destination)
	})

	t.Run("non-merge account change: destinationAddress is null", func(t *testing.T) {
		obj := &types.AccountStateChangeModel{}

		destination, err := resolver.DestinationAddress(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, destination)
	})
}

func TestStateChangeResolver_Account(t *testing.T) {
	resolver := &standardBalanceChangeResolver{&Resolver{}}

	t.Run("success", func(t *testing.T) {
		parentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				AccountID:           types.AddressBytea(sharedTestAccountAddress),
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		ctx := context.Background()

		account, err := resolver.Account(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, sharedTestAccountAddress, string(account.StellarAddress))
	})

	t.Run("nil state change panics", func(t *testing.T) {
		ctx := context.Background()

		assert.Panics(t, func() {
			_, _ = resolver.Account(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with empty account_id returns error", func(t *testing.T) {
		emptySC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				AccountID:           "",
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		ctx := context.Background()

		account, err := resolver.Account(ctx, &emptySC)
		require.Error(t, err)
		assert.Nil(t, account)
		assert.Contains(t, err.Error(), "state change has no account_id")
	})
}

func TestStateChangeResolver_Operation(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &standardBalanceChangeResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:      testDBConnectionPool,
				Metrics: m.DB,
			},
		},
	}}
	opID := toid.New(1000, 1, 1).ToInt64()
	txToID := opID &^ 0xFFF // Derive transaction to_id from operation_id using TOID bitmask
	parentSC := types.StandardBalanceStateChangeModel{
		StateChange: types.StateChange{
			ToID:                txToID,
			OperationID:         opID,
			StateChangeID:       1,
			StateChangeCategory: types.StateChangeCategoryBalance,
			LedgerCreatedAt:     sharedTestLedgerCreatedAt,
		},
	}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), op.ID)
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Operation(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with non-existent operation", func(t *testing.T) {
		nonExistentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				ToID:                9999,
				OperationID:         0,
				StateChangeID:       1,
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, &nonExistentSC)
		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, op)
	})
}

func TestStateChangeResolver_Transaction(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &standardBalanceChangeResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:      testDBConnectionPool,
				Metrics: m.DB,
			},
		},
	}}
	parentSC := types.StandardBalanceStateChangeModel{
		StateChange: types.StateChange{
			ToID:                toid.New(1000, 1, 0).ToInt64(),
			StateChangeID:       1,
			StateChangeCategory: types.StateChangeCategoryBalance,
			LedgerCreatedAt:     sharedTestLedgerCreatedAt,
		},
	}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, testTxHash1, tx.Hash.String())
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Transaction(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with non-existent transaction", func(t *testing.T) {
		nonExistentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				ToID:                9999,
				StateChangeID:       1,
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		loaders := dataloaders.NewDataloaders(resolver.models, m.Dataloader)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, &nonExistentSC)
		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, tx)
	})
}

func TestAccountResolver_SEP41TransferSurfacesAsStandardBalanceChange(t *testing.T) {
	// Sep-41 transfer → state_changes row with category=BALANCE, reason=CREDIT, tokenId set,
	// amount set, and (new) to_muxed_id set. Through Account.stateChanges this should come
	// back as a StandardBalanceChange with the expected fields.
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	// Minimal state_changes row. No FK to operations/transactions because the hypertable
	// already removed those FKs (see migrations).
	// Token column is BYTEA-encoded strkey via NullAddressBytea. Use pgx binding via driver.Valuer
	// by going through the raw SQL: `encode(decode(...), 'hex')` is messy; just encode the 33-byte
	// BYTEA directly here.
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
		"18446744073709551615", // u64 max, proves TEXT column handles >2^63
	)

	m := metrics.NewMetrics(prometheus.NewRegistry())

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
		metrics: m,
	}}

	ctx := getTestCtx("stateChanges", []string{
		"type", "reason", "tokenId", "amount", "toMuxedId", "ledgerNumber",
	})

	first := int32(10)
	conn, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &first, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Len(t, conn.Edges, 1)

	bc, ok := conn.Edges[0].Node.(*types.StandardBalanceStateChangeModel)
	require.True(t, ok, "edge[0] should be StandardBalanceStateChangeModel, got %T", conn.Edges[0].Node)
	assert.Equal(t, types.StateChangeCategoryBalance, bc.StateChangeCategory)
	assert.Equal(t, types.StateChangeReasonCredit, bc.StateChangeReason)
	assert.Equal(t, contractAddr, bc.TokenID.String())
	assert.True(t, bc.Amount.Valid)
	assert.Equal(t, "500", bc.Amount.String)
	assert.True(t, bc.ToMuxedID.Valid)
	assert.Equal(t, "18446744073709551615", bc.ToMuxedID.String)
}
