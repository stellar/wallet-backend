package dataloaders

import (
	"context"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type TransactionColumnsKey struct {
	AccountID     string
	OperationID   int64
	StateChangeID string
	Columns       string
	Limit         *int32
	After         *int64
}

// txByOperationIDLoader creates a dataloader for fetching transactions by operation ID
// This prevents N+1 queries when multiple operations request their transaction
// The loader batches multiple operation IDs into a single database query
func transactionByOperationIDLoader(models *data.Models) *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []TransactionColumnsKey) ([]*types.TransactionWithOperationID, error) {
			operationIDs := make([]int64, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				operationIDs[i] = key.OperationID
			}
			return models.Transactions.BatchGetByOperationIDs(ctx, operationIDs, columns)
		},
		func(item *types.TransactionWithOperationID) int64 {
			return item.OperationID
		},
		func(key TransactionColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.TransactionWithOperationID) types.Transaction {
			return item.Transaction
		},
	)
}

// transactionByStateChangeIDLoader creates a dataloader for fetching transactions by state change ID
// This prevents N+1 queries when multiple state changes request their transactions
// The loader batches multiple state change IDs into a single database query
func transactionByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []TransactionColumnsKey) ([]*types.TransactionWithStateChangeID, error) {
			stateChangeIDs := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				stateChangeIDs[i] = key.StateChangeID
			}
			return models.Transactions.BatchGetByStateChangeIDs(ctx, stateChangeIDs, columns)
		},
		func(item *types.TransactionWithStateChangeID) string {
			return item.StateChangeID
		},
		func(key TransactionColumnsKey) string {
			return key.StateChangeID
		},
		func(item *types.TransactionWithStateChangeID) types.Transaction {
			return item.Transaction
		},
	)
}
