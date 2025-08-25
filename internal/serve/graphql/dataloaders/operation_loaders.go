package dataloaders

import (
	"context"
	"fmt"
	"strings"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	MaxOperationsPerBatch = 10
)

type OperationColumnsKey struct {
	TxHash        string
	AccountID     string
	StateChangeID string
	Columns       string
	Limit         *int32
	Cursor        *int64
	SortOrder     data.SortOrder
}

// opByTxHashLoader creates a dataloader for fetching operations by transaction hash
// This prevents N+1 queries when multiple transactions request their operations
// The loader batches multiple transaction hashes into a single database query
func operationsByTxHashLoader(models *data.Models) *dataloadgen.Loader[OperationColumnsKey, []*types.OperationWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []OperationColumnsKey) ([]*types.OperationWithCursor, error) {
			// Add the tx_hash column (if not requested) since that will be used as the primary key to group the operations
			// in the final result.
			columns := keys[0].Columns
			if columns != "" && !strings.Contains(columns, "tx_hash") {
				columns = fmt.Sprintf("%s, tx_hash", columns)
			}
			sortOrder := keys[0].SortOrder
			limit := keys[0].Limit

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.Operations.BatchGetByTxHash(ctx, keys[0].TxHash, columns, limit, keys[0].Cursor, sortOrder)
			}

			txHashes := make([]string, len(keys))
			maxLimit := min(*limit, MaxOperationsPerBatch)
			for i, key := range keys {
				txHashes[i] = key.TxHash
			}
			return models.Operations.BatchGetByTxHashes(ctx, txHashes, columns, &maxLimit, sortOrder)
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
			columns := keys[0].Columns
			scIDs := make([]string, len(keys))
			for i, key := range keys {
				scIDs[i] = key.StateChangeID
			}
			scToIDs, scOrders, err := parseStateChangeIDs(scIDs)
			if err != nil {
				return nil, fmt.Errorf("parsing state change IDs: %w", err)
			}
			return models.Operations.BatchGetByStateChangeIDs(ctx, scToIDs, scOrders, columns)
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
