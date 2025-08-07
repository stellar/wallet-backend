package dataloaders

import (
	"context"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type OperationColumnsKey struct {
	TxHash        string
	AccountID     string
	StateChangeID string
	Columns       string
	Cursor        *int64
	Limit         *int32
}

// OperationsByTxHashLoader creates a dataloader for fetching operations by transaction hash
// This prevents N+1 queries when multiple transactions request their operations
// The loader batches multiple transaction hashes into a single database query
func OperationsByTxHashLoader(models *data.Models) *dataloadgen.Loader[OperationColumnsKey, []*types.OperationWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []OperationColumnsKey) ([]*types.OperationWithCursor, error) {
			txHashes := make([]string, 0, len(keys))
			columns := keys[0].Columns
			cursors := make([]*int64, 0, len(keys))
			limit := keys[0].Limit
			for _, key := range keys {
				txHashes = append(txHashes, key.TxHash)
				cursors = append(cursors, key.Cursor)
			}
			return models.Operations.BatchGetByTxHashes(ctx, txHashes, columns, limit, cursors)
		},
		func(item *types.OperationWithCursor) string {
			return item.TxHash
		},
		func(key OperationColumnsKey) string {
			return key.TxHash
		},
		func(item *types.OperationWithCursor) types.OperationWithCursor {
			return *item
		},
	)
}

// operationByStateChangeIDLoader creates a dataloader for fetching operations by state change ID
// This prevents N+1 queries when multiple state changes request their operations
// The loader batches multiple state change IDs into a single database query
func operationByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[OperationColumnsKey, *types.Operation] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []OperationColumnsKey) ([]*types.OperationWithStateChangeID, error) {
			stateChangeIDs := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				stateChangeIDs[i] = key.StateChangeID
			}
			return models.Operations.BatchGetByStateChangeIDs(ctx, stateChangeIDs, columns)
		},
		func(item *types.OperationWithStateChangeID) string {
			return item.StateChangeID
		},
		func(key OperationColumnsKey) string {
			return key.StateChangeID
		},
		func(item *types.OperationWithStateChangeID) types.Operation {
			return item.Operation
		},
	)
}
