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
	MaxOperationsPerBatch = 10
)

type OperationColumnsKey struct {
	ToID          int64
	AccountID     string
	StateChangeID string
	Columns       string
	Limit         *int32
	Cursor        *int64
	SortOrder     data.SortOrder
}

// operationsByToIDLoader creates a dataloader for fetching operations by transaction ToID.
// This prevents N+1 queries when multiple transactions request their operations.
// The loader batches multiple transaction ToIDs into a single database query.
//
// Operations belong to a transaction when: tx_to_id < operation_id < tx_to_id + 4096
//
// To group results back by transaction, we derive tx_to_id from each operation:
//
//	tx_to_id = operation.ID &^ 0xFFF
//
// This is the inverse of the query: given an operation, find its parent transaction.
func operationsByToIDLoader(models *data.Models) *dataloadgen.Loader[OperationColumnsKey, []*types.OperationWithCursor] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []OperationColumnsKey) ([]*types.OperationWithCursor, error) {
			columns := keys[0].Columns
			sortOrder := keys[0].SortOrder
			limit := keys[0].Limit

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.Operations.BatchGetByToID(ctx, keys[0].ToID, columns, limit, keys[0].Cursor, sortOrder)
			}

			toIDs := make([]int64, len(keys))
			maxLimit := min(*limit, MaxOperationsPerBatch)
			for i, key := range keys {
				toIDs[i] = key.ToID
			}
			return models.Operations.BatchGetByToIDs(ctx, toIDs, columns, &maxLimit, sortOrder)
		},
		func(item *types.OperationWithCursor) int64 {
			// Derive tx_to_id from operation ID using TOID bit masking
			// Operation ID encodes (ledger, tx_order, op_index). Setting op_index to 0 gives tx_to_id.
			// The op_index uses the lower 12 bits (0xFFF), so masking them out gives the tx_to_id.
			return item.ID &^ 0xFFF
		},
		func(key OperationColumnsKey) int64 {
			return key.ToID
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
