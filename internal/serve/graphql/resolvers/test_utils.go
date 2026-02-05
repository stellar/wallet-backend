package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/toid"
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

func ptr[T any](v T) *T {
	return &v
}

// sharedTestAccountAddress is a fixed test address used by tests that rely on setupDB.
// It's generated once and reused to ensure test data consistency.
var sharedTestAccountAddress = keypair.MustRandom().Address()

// sharedNonExistentAccountAddress is a valid Stellar address that doesn't exist in the test DB.
var sharedNonExistentAccountAddress = keypair.MustRandom().Address()

// Test transaction hashes used by setupDB (64-char hex strings for BYTEA storage)
// These must match the pattern in setupDB: fmt.Sprintf("...487%x", i) where i = 0,1,2,3
var (
	testTxHash1 = "3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4870"
	testTxHash2 = "3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4871"
	testTxHash3 = "3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4872"
	testTxHash4 = "3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4873"
)

func setupDB(ctx context.Context, t *testing.T, dbConnectionPool db.ConnectionPool) {
	testLedger := int32(1000)
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}
	txns := make([]*types.Transaction, 0, 4)
	ops := make([]*types.Operation, 0, 8)
	opIdx := 1
	for i := range 4 {
		txn := &types.Transaction{
			Hash:            types.HashBytea(fmt.Sprintf("3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa487%x", i)),
			ToID:            toid.New(testLedger, int32(i+1), 0).ToInt64(),
			EnvelopeXDR:     ptr(fmt.Sprintf("envelope%d", i+1)),
			FeeCharged:      int64(100 * (i + 1)),
			ResultCode:      "TransactionResultCodeTxSuccess",
			MetaXDR:         ptr(fmt.Sprintf("meta%d", i+1)),
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
			IsFeeBump:       false,
		}
		txns = append(txns, txn)

		// Add 2 operations for each transaction
		for j := range 2 {
			ops = append(ops, &types.Operation{
				ID:              toid.New(testLedger, int32(i+1), int32(j+1)).ToInt64(),
				OperationType:   "PAYMENT",
				OperationXDR:    types.XDRBytea(fmt.Sprintf("opxdr%d", opIdx)),
				ResultCode:      "op_success",
				Successful:      true,
				LedgerNumber:    1,
				LedgerCreatedAt: time.Now(),
			})
			opIdx++
		}
	}

	// Create 2 state changes per operation (20 total: 2 per operation × 8 operations + 4 fee state changes)
	stateChanges := make([]*types.StateChange, 0, 20)
	creditReason := types.StateChangeReasonCredit
	addReason := types.StateChangeReasonAdd
	for i, op := range ops {
		for scOrder := range 2 {
			// Vary the categories and reasons for different operations
			category := types.StateChangeCategoryBalance
			reason := &creditReason

			// Make some state changes use SIGNER category and ADD reason
			if i%2 == 0 && scOrder == 1 {
				category = types.StateChangeCategorySigner
				reason = &addReason
			}

			stateChanges = append(stateChanges, &types.StateChange{
				ToID:                op.ID &^ 0xFFF, // Derive transaction to_id from operation_id using TOID bitmask
				StateChangeOrder:    int64(scOrder + 1),
				StateChangeCategory: category,
				StateChangeReason:   reason,
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
				`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				txn.Hash, txn.ToID, txn.EnvelopeXDR, txn.FeeCharged, txn.ResultCode, txn.MetaXDR, txn.LedgerNumber, txn.LedgerCreatedAt, txn.IsFeeBump)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx,
				`INSERT INTO transactions_accounts (tx_to_id, account_id) VALUES ($1, $2)`,
				txn.ToID, parentAccount.StellarAddress)
			require.NoError(t, err)
		}

		for _, op := range ops {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				op.ID, op.OperationType, op.OperationXDR, op.ResultCode, op.Successful, op.LedgerNumber, op.LedgerCreatedAt)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx,
				`INSERT INTO operations_accounts (operation_id, account_id) VALUES ($1, $2)`,
				op.ID, parentAccount.StellarAddress)
			require.NoError(t, err)
		}

		for _, sc := range stateChanges {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO state_changes (to_id, state_change_order, state_change_category, state_change_reason, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				sc.ToID, sc.StateChangeOrder, sc.StateChangeCategory, sc.StateChangeReason, sc.OperationID, sc.AccountID, sc.LedgerCreatedAt, sc.LedgerNumber)
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
	case types.StateChangeCursorGetter:
		return v.GetCursor()
	default:
		return types.StateChangeCursor{}
	}
}
