package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/vektah/gqlparser/v2/ast"
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
	parentAccount := &types.Account{StellarAddress: "test-account"}
	txns := make([]*types.Transaction, 0, 4)
	ops := make([]*types.Operation, 0, 8)
	opIdx := 0
	for i := range 4 {
		txns = append(txns, &types.Transaction{
			Hash:            fmt.Sprintf("tx%d", i+1),
			ToID:            int64(i + 1),
			EnvelopeXDR:     fmt.Sprintf("envelope%d", i+1),
			ResultXDR:       fmt.Sprintf("result%d", i+1),
			MetaXDR:         fmt.Sprintf("meta%d", i+1),
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		})

		// Append 4 operations for each transaction
		for range 2 {
			ops = append(ops, &types.Operation{
				ID:              int64(opIdx + 1),
				TxHash:          fmt.Sprintf("tx%d", i+1),
				OperationType:   "payment",
				OperationXDR:    fmt.Sprintf("opxdr%d", opIdx+1),
				LedgerNumber:    1,
				LedgerCreatedAt: time.Now(),
			})
			opIdx++
		}
	}

	stateChanges := make([]*types.StateChange, 0, 4)
	for i := range 4 {
		stateChanges = append(stateChanges, &types.StateChange{
			ID:                  fmt.Sprintf("sc%d", i+1),
			StateChangeCategory: types.StateChangeCategoryCredit,
			TxHash:              fmt.Sprintf("tx%d", i+1),
			OperationID:         int64(i + 1),
			AccountID:           parentAccount.StellarAddress,
			LedgerCreatedAt:     time.Now(),
			LedgerNumber:        1,
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
				`INSERT INTO state_changes (id, state_change_category, tx_hash, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				sc.ID, sc.StateChangeCategory, sc.TxHash, sc.OperationID, sc.AccountID, sc.LedgerCreatedAt, sc.LedgerNumber)
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
