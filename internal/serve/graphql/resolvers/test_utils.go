package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stellar/go/toid"
	"github.com/stretchr/testify/require"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

func getTestCtx(table string, columns []string) context.Context {
	opCtx := &graphql.OperationContext{
		Operation: &ast.OperationDefinition{
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name:         table,
					SelectionSet: ast.SelectionSet{},
				},
			},
		},
	}
	ctx := graphql.WithOperationContext(context.Background(), opCtx)
	var selections ast.SelectionSet
	for _, fieldName := range columns {
		selections = append(selections, &ast.Field{Name: fieldName})
	}
	fieldCtx := &graphql.FieldContext{
		Field: graphql.CollectedField{
			Selections: selections,
		},
	}
	ctx = graphql.WithFieldContext(ctx, fieldCtx)
	return ctx
}

func setupDB(ctx context.Context, t *testing.T, dbConnectionPool db.ConnectionPool) {
	testLedger := int32(1000)
	parentAccount := &types.Account{StellarAddress: "test-account"}
	txns := make([]*types.Transaction, 0, 4)
	ops := make([]*types.Operation, 0, 8)
	opIdx := 1
	for i := range 4 {
		txn := &types.Transaction{
			Hash:            fmt.Sprintf("tx%d", i+1),
			ToID:            toid.New(testLedger, int32(i+1), 0).ToInt64(),
			EnvelopeXDR:     fmt.Sprintf("envelope%d", i+1),
			ResultXDR:       fmt.Sprintf("result%d", i+1),
			MetaXDR:         fmt.Sprintf("meta%d", i+1),
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		}
		txns = append(txns, txn)

		// Add 2 operations for each transaction
		for j := range 2 {
			ops = append(ops, &types.Operation{
				ID:              toid.New(testLedger, int32(i+1), int32(j+1)).ToInt64(),
				TxHash:          txn.Hash,
				OperationType:   "payment",
				OperationXDR:    fmt.Sprintf("opxdr%d", opIdx),
				LedgerNumber:    1,
				LedgerCreatedAt: time.Now(),
			})
			opIdx++
		}
	}

	// Create 2 state changes per operation (20 total: 2 per operation × 8 operations + 4 fee state changes)
	stateChanges := make([]*types.StateChange, 0, 20)
	creditReason := types.StateChangeReasonCredit
	for _, op := range ops {
		for scOrder := range 2 {
			stateChanges = append(stateChanges, &types.StateChange{
				ToID:                op.ID,
				StateChangeOrder:    int64(scOrder + 1),
				StateChangeCategory: types.StateChangeCategoryBalance,
				StateChangeReason:   &creditReason,
				TxHash:              op.TxHash,
				OperationID:         op.ID,
				AccountID:           parentAccount.StellarAddress,
				LedgerCreatedAt:     time.Now(),
				LedgerNumber:        1,
			})
		}
	}
	// Create fee state changes per transaction
	debitReason := types.StateChangeReasonDebit
	for _, txn := range txns {
		stateChanges = append(stateChanges, &types.StateChange{
			ToID:                txn.ToID,
			StateChangeOrder:    int64(1),
			StateChangeCategory: types.StateChangeCategoryBalance,
			StateChangeReason:   &debitReason,
			TxHash:              txn.Hash,
			AccountID:           parentAccount.StellarAddress,
			LedgerCreatedAt:     time.Now(),
			LedgerNumber:        1000,
		})
	}

	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO accounts (stellar_address) VALUES ($1)`,
			parentAccount.StellarAddress)
		require.NoError(t, err)

		for _, txn := range txns {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				txn.Hash, txn.ToID, txn.EnvelopeXDR, txn.ResultXDR, txn.MetaXDR, txn.LedgerNumber, txn.LedgerCreatedAt)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx,
				`INSERT INTO transactions_accounts (tx_hash, account_id) VALUES ($1, $2)`,
				txn.Hash, parentAccount.StellarAddress)
			require.NoError(t, err)
		}

		for _, op := range ops {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
				op.ID, op.TxHash, op.OperationType, op.OperationXDR, op.LedgerNumber, op.LedgerCreatedAt)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx,
				`INSERT INTO operations_accounts (operation_id, account_id) VALUES ($1, $2)`,
				op.ID, parentAccount.StellarAddress)
			require.NoError(t, err)
		}

		for _, sc := range stateChanges {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO state_changes (to_id, state_change_order, state_change_category, tx_hash, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				sc.ToID, sc.StateChangeOrder, sc.StateChangeCategory, sc.TxHash, sc.OperationID, sc.AccountID, sc.LedgerCreatedAt, sc.LedgerNumber)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, dbErr)
}

func cleanUpDB(ctx context.Context, t *testing.T, dbConnectionPool db.ConnectionPool) {
	_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes`)
	require.NoError(t, err)
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
	require.NoError(t, err)
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
	require.NoError(t, err)
}

func extractStateChangeIDs(sc generated.BaseStateChange) types.StateChangeCursor {
	switch v := sc.(type) {
	case *types.BalanceStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.AccountStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.SponsorshipStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.TrustlineStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.SignerStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.SignerThresholdsStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.FlagsStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.MetadataStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	case *types.BalanceAuthorizationStateChangeModel:
		return types.StateChangeCursor{
			ToID:             v.ToID,
			StateChangeOrder: v.StateChangeOrder,
		}
	}
	return types.StateChangeCursor{}
}
