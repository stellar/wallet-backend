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
}

// stateChangesByTxHashLoader creates a dataloader for fetching state changes by transaction hash
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple transaction hashes into a single database query
func stateChangesByTxHashLoader(models *data.Models) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChange] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []StateChangeColumnsKey) ([]*types.StateChange, error) {
			txHashes := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				txHashes[i] = key.TxHash
			}
			return models.StateChanges.BatchGetByTxHashes(ctx, txHashes, columns)
		},
		func(item *types.StateChange) string {
			return item.TxHash
		},
		func(key StateChangeColumnsKey) string {
			return key.TxHash
		},
		func(item *types.StateChange) types.StateChange {
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
