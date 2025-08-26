package dataloaders

import (
	"context"
	"fmt"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// TODO: this should be configurable via config
	MaxStateChangesPerBatch = 10
)

type StateChangeColumnsKey struct {
	TxHash      string
	AccountID   string
	OperationID int64
	Columns     string
	Limit       *int32
	Cursor      *types.StateChangeCursor
	SortOrder   data.SortOrder
}

// stateChangesByTxHashLoader creates a dataloader for fetching state changes by transaction hash
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple transaction hashes into a single database query
func stateChangesByTxHashLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChangeWithCursor, error) {
			// Add the tx_hash column since that will be used as the primary key to group the state changes
			// in the final result.
			columns := keys[0].Columns
			if columns != "" {
				columns = fmt.Sprintf("%s, tx_hash", columns)
			}
			sortOrder := keys[0].SortOrder
			limit := keys[0].Limit

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.StateChanges.BatchGetByTxHash(ctx, keys[0].TxHash, columns, limit, keys[0].Cursor, sortOrder)
			}

			txHashes := make([]string, len(keys))
			maxLimit := min(*limit, MaxStateChangesPerBatch)
			for i, key := range keys {
				txHashes[i] = key.TxHash
			}
			return models.StateChanges.BatchGetByTxHashes(ctx, txHashes, columns, &maxLimit, sortOrder)
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
func stateChangesByOperationIDLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChangeWithCursor, error) {
			// Add the operation_id column since that will be used as the primary key to group the state changes
			// in the final result.
			columns := keys[0].Columns
			if columns != "" {
				columns = fmt.Sprintf("%s, operation_id", columns)
			}
			sortOrder := keys[0].SortOrder
			limit := keys[0].Limit

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.StateChanges.BatchGetByOperationID(ctx, keys[0].OperationID, columns, limit, keys[0].Cursor, sortOrder)
			}

			operationIDs := make([]int64, len(keys))
			for i, key := range keys {
				operationIDs[i] = key.OperationID
			}
			return models.StateChanges.BatchGetByOperationIDs(ctx, operationIDs, columns, limit, sortOrder)
		},
		func(item *types.StateChangeWithCursor) int64 {
			return item.OperationID
		},
		func(key StateChangeColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.StateChangeWithCursor) types.StateChangeWithCursor {
			return *item
		},
	)
}
