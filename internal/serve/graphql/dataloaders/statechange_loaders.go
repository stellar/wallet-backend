package dataloaders

import (
	"context"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type StateChangeColumnsKey struct {
	AccountID   string
	OperationID int64
	TxHash      string
	Columns     string
	Limit       *int32
	Cursor      *data.StateChangeCursor
}

// stateChangesByTxHashLoader creates a dataloader for fetching state changes by transaction hash
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple transaction hashes into a single database query
func StateChangesByTxHashLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChangeWithCursor, error) {
			txHashes := make([]string, 0, len(keys))
			columns := keys[0].Columns
			limit := keys[0].Limit
			cursors := make([]*data.StateChangeCursor, 0, len(keys))
			for _, key := range keys {
				txHashes = append(txHashes, key.TxHash)
				cursors = append(cursors, key.Cursor)
			}
			return models.StateChanges.BatchGetByTxHashes(ctx, txHashes, columns, limit, cursors)
		},
		func(item *types.StateChangeWithCursor) string {
			return item.TxHash
		},
		func(key StateChangeColumnsKey) string {
			return key.TxHash
		},
		func(item *types.StateChangeWithCursor) types.StateChangeWithCursor {
			return *item
		},
	)
}

// stateChangesByOperationIDLoader creates a dataloader for fetching state changes by operation ID
// This prevents N+1 queries when multiple operations request their state changes
// The loader batches multiple operation IDs into a single database query
func stateChangesByOperationIDLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChange] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChange, error) {
			operationIDs := make([]int64, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				operationIDs[i] = key.OperationID
			}
			return models.StateChanges.BatchGetByOperationIDs(ctx, operationIDs, columns)
		},
		func(item *types.StateChange) int64 {
			return item.OperationID
		},
		func(key StateChangeColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.StateChange) types.StateChange {
			return *item
		},
	)
}
