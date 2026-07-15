package dataloaders

import (
	"context"
	"fmt"
	"time"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type StateChangeColumnsKey struct {
	ToID            int64
	AccountID       string
	OperationID     int64
	Columns         string
	Limit           *int32
	Cursor          *types.StateChangeCursor
	SortOrder       data.SortOrder
	LedgerCreatedAt time.Time // parent transaction's ledger time; pins the partition column for account-scoped loads
}

// stateChangesByToIDLoader creates a dataloader for fetching state changes by to_id
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple to_ids into a single database query
func stateChangesByToIDLoader(models *data.Models, m *metrics.DataloaderMetrics) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
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
			if limit == nil || *limit <= 0 {
				return nil, fmt.Errorf("state changes loader requires a positive limit")
			}

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.StateChanges.BatchGetByToID(ctx, keys[0].ToID, keys[0].LedgerCreatedAt, columns, limit, keys[0].Cursor, sortOrder)
			}

			toIDs := make([]int64, len(keys))
			ledgerCreatedAts := make([]time.Time, len(keys))
			for i, key := range keys {
				toIDs[i] = key.ToID
				ledgerCreatedAts[i] = key.LedgerCreatedAt
			}
			return models.StateChanges.BatchGetByToIDs(ctx, toIDs, ledgerCreatedAts, columns, limit, sortOrder)
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
		stateChangeColumnsKeyShape,
		"StateChangesByToIDLoader",
		m,
	)
}

// stateChangeColumnsKeyShape is the query shape for one-to-many state-change loaders keyed by
// StateChangeColumnsKey (see stateChangesByToIDLoader, stateChangesByOperationIDLoader): Columns,
// Limit, Cursor and SortOrder all determine the SQL statement the fetcher builds, so any two keys
// differing in one of these fields must land in different batch groups.
func stateChangeColumnsKeyShape(key StateChangeColumnsKey) QueryShape {
	shape := QueryShape{Columns: key.Columns, SortOrder: key.SortOrder}
	if key.Limit != nil {
		shape.Limit = *key.Limit
		shape.HasLimit = true
	}
	if key.Cursor != nil {
		shape.Cursor = *key.Cursor
		shape.HasCursor = true
	}
	return shape
}

// stateChangesByOperationIDLoader creates a dataloader for fetching state changes by operation ID
// This prevents N+1 queries when multiple operations request their state changes
// The loader batches multiple operation IDs into a single database query
func stateChangesByOperationIDLoader(models *data.Models, m *metrics.DataloaderMetrics) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor] {
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
			if limit == nil || *limit <= 0 {
				return nil, fmt.Errorf("state changes loader requires a positive limit")
			}

			// If there is only one key, we can use a simpler query without resorting to the CTE expressions.
			// Also, when a single key is requested, we can allow using normal cursor based pagination.
			if len(keys) == 1 {
				return models.StateChanges.BatchGetByOperationID(ctx, keys[0].OperationID, keys[0].LedgerCreatedAt, columns, limit, keys[0].Cursor, sortOrder)
			}

			operationIDs := make([]int64, len(keys))
			ledgerCreatedAts := make([]time.Time, len(keys))
			for i, key := range keys {
				operationIDs[i] = key.OperationID
				ledgerCreatedAts[i] = key.LedgerCreatedAt
			}
			return models.StateChanges.BatchGetByOperationIDs(ctx, operationIDs, ledgerCreatedAts, columns, limit, sortOrder)
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
		stateChangeColumnsKeyShape,
		"StateChangesByOperationIDLoader",
		m,
	)
}

// accountStateChangesByToIDLoader batches account-scoped state-change lookups by transaction ToID,
// grouping the batch by account so a multi-account request never cross-contaminates edges (see
// newAccountScopedLoader). BatchGetAccountStateChangesByToIDs forces the to_id grouping key into the
// SELECT via prepareColumnsWithID, so — unlike stateChangesByToIDLoader — no manual column injection
// is needed here.
func accountStateChangesByToIDLoader(models *data.Models, m *metrics.DataloaderMetrics) *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChange] {
	return newAccountScopedLoader(
		models.StateChanges.BatchGetAccountStateChangesByToIDs,
		func(key StateChangeColumnsKey) string { return key.AccountID },
		func(key StateChangeColumnsKey) string { return key.Columns },
		func(key StateChangeColumnsKey) int64 { return key.ToID },
		func(key StateChangeColumnsKey) time.Time { return key.LedgerCreatedAt },
		func(item *types.StateChange) int64 { return item.ToID },
		"AccountStateChangesByToIDLoader",
		m,
	)
}
