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
	ToID        int64
	AccountID   string
	OperationID int64
	Columns     string
	Limit       *int32
	Cursor      *types.StateChangeCursor
	SortOrder   data.SortOrder
}

// stateChangesByToIDLoader creates a dataloader for fetching state changes by to_id
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple to_ids into a single database query
func stateChangesByToIDLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChangeWithCursor, error) {
			// Add the to_id column since that will be used as the primary key to group the state changes
			// in the final result.
			columns := keys[0].Columns
			if columns != "" {
				columns = fmt.Sprintf("%s, to_id", columns)
			}
			sortOrder := keys[0].SortOrder
			limit := keys[0].Limit

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.StateChanges.BatchGetByToID(ctx, keys[0].ToID, columns, limit, keys[0].Cursor, sortOrder)
			}

			toIDs := make([]int64, len(keys))
			maxLimit := min(*limit, MaxStateChangesPerBatch)
			for i, key := range keys {
				toIDs[i] = key.ToID
			}
			return models.StateChanges.BatchGetByToIDs(ctx, toIDs, columns, &maxLimit, sortOrder)
		},
		func(item *types.StateChangeWithCursor) int64 {
			return item.StateChange.ToID
		},
		func(key StateChangeColumnsKey) int64 {
			return key.ToID
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
			return item.StateChange.OperationID
		},
		func(key StateChangeColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.StateChangeWithCursor) types.StateChangeWithCursor {
			return *item
		},
	)
}
