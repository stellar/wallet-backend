// GraphQL DataLoaders package - implements efficient batching for GraphQL resolvers
// DataLoaders solve the N+1 query problem by batching multiple requests into single database queries
// This is essential for GraphQL performance when resolving relationship fields
package dataloaders

import (
	"context"
	"time"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Dataloaders struct holds all dataloader instances for GraphQL resolvers
// Each dataloader batches requests for a specific type of data relationship
// GraphQL resolvers use these to efficiently load related data
type Dataloaders struct {
	// OperationsByTxHashLoader batches requests for operations by transaction hash
	// Used by Transaction.operations field resolver to prevent N+1 queries
	OperationsByTxHashLoader *dataloadgen.Loader[OperationColumnsKey, []*types.OperationWithCursor]

	// AccountsByTxHashLoader batches requests for accounts by transaction hash
	// Used by Transaction.accounts field resolver to prevent N+1 queries
	AccountsByTxHashLoader *dataloadgen.Loader[AccountColumnsKey, []*types.Account]

	// StateChangesByTxHashLoader batches requests for state changes by transaction hash
	// Used by Transaction.stateChanges field resolver to prevent N+1 queries
	StateChangesByTxHashLoader *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor]

	// TransactionsByOperationIDLoader batches requests for transactions by operation ID
	// Used by Operation.transaction field resolver to prevent N+1 queries
	TransactionsByOperationIDLoader *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction]

	// AccountsByOperationIDLoader batches requests for accounts by operation ID
	// Used by Operation.accounts field resolver to prevent N+1 queries
	AccountsByOperationIDLoader *dataloadgen.Loader[AccountColumnsKey, []*types.Account]

	// StateChangesByOperationIDLoader batches requests for state changes by operation ID
	// Used by Operation.stateChanges field resolver to prevent N+1 queries
	StateChangesByOperationIDLoader *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChange]

	// OperationByStateChangeIDLoader batches requests for operations by state change ID
	// Used by StateChange.operation field resolver to prevent N+1 queries
	OperationByStateChangeIDLoader *dataloadgen.Loader[OperationColumnsKey, *types.Operation]

	// TransactionByStateChangeIDLoader batches requests for transactions by state change ID
	// Used by StateChange.transaction field resolver to prevent N+1 queries
	TransactionByStateChangeIDLoader *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction]
}

// NewDataloaders creates a new instance of all dataloaders
// This is called during GraphQL server initialization
// The dataloaders are then injected into GraphQL context by middleware
// GraphQL resolvers access these loaders to batch database queries efficiently
func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader:         OperationsByTxHashLoader(models),
		OperationByStateChangeIDLoader:   operationByStateChangeIDLoader(models),
		TransactionByStateChangeIDLoader: transactionByStateChangeIDLoader(models),
		TransactionsByOperationIDLoader:  transactionByOperationIDLoader(models),
		StateChangesByTxHashLoader:       StateChangesByTxHashLoader(models),
		StateChangesByOperationIDLoader:  stateChangesByOperationIDLoader(models),
		AccountsByTxHashLoader:           accountsByTxHashLoader(models),
		AccountsByOperationIDLoader:      accountsByOperationIDLoader(models),
	}
}

// newOneToManyLoader is a generic helper function that creates a dataloader for one-to-many relationships.
// It abstracts the common pattern of fetching items, grouping them by a key, and returning them in the
// order of the original keys. This reduces boilerplate code in dataloader definitions.
//
// Parameters:
//   - fetcher: A function that fetches all items for a given set of keys.
//   - getKey: A function that extracts the grouping key from an item.
//
// Returns:
//   - A configured dataloadgen.Loader for one-to-many relationships.
func newOneToManyLoader[K comparable, PK comparable, V any, T any](
	fetcher func(ctx context.Context, keys []K) ([]T, error),
	getPKFromItem func(item T) PK,
	getPKFromKey func(key K) PK,
	transform func(item T) V,
) *dataloadgen.Loader[K, []*V] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []K) ([][]*V, []error) {
			items, err := fetcher(ctx, keys)
			if err != nil {
				// if the fetcher function returns an error, we'll return it for each key.
				// this is a requirement for dataloadgen, which expects an error for each key.
				errors := make([]error, len(keys))
				for i := range keys {
					errors[i] = err
				}
				return nil, errors
			}

			// An item is the actual data from the database that we want to return.
			// The key contains the primary key and the set of columns to return.
			//
			// For e.g. if we want to get all operations for a transaction, the key will
			// be the a struct containing the transaction hash and the columns to return.
			// The items will be a slice of operations which will be grouped by the primary key, which is the transaction hash.
			// We can do this by creating a map with the primary key as the key and the items as the value.
			// We can then return the items in the order of the keys.
			itemsByPK := make(map[PK][]*V)
			for _, item := range items {
				key := getPKFromItem(item)
				transformedItem := transform(item)
				itemsByPK[key] = append(itemsByPK[key], &transformedItem)
			}

			// Build result in the order of the keys.
			result := make([][]*V, len(keys))
			for i, key := range keys {
				result[i] = itemsByPK[getPKFromKey(key)]
			}

			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// newOneToOneLoader is a generic helper function that creates a dataloader for one-to-one relationships.
// It abstracts the common pattern of fetching a single item for each key and returning them in the
// order of the original keys. This is useful for relationships where each key maps to exactly one item.
//
// Parameters:
//   - fetcher: A function that fetches all items for a given set of keys.
//   - getKey: A function that extracts the grouping key from an item.
//   - setKey: A function that associates a fetched item with its corresponding key. This is necessary
//     because the fetcher may not return items in the same order as the input keys.
//
// Returns:
//   - A configured dataloadgen.Loader for one-to-one relationships.
func newOneToOneLoader[K comparable, PK comparable, V any, T any](
	fetcher func(ctx context.Context, keys []K) ([]T, error),
	getPKFromItem func(item T) PK,
	getPKFromKey func(key K) PK,
	transform func(item T) V,
) *dataloadgen.Loader[K, *V] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []K) ([]*V, []error) {
			items, err := fetcher(ctx, keys)
			if err != nil {
				errors := make([]error, len(keys))
				for i := range keys {
					errors[i] = err
				}
				return nil, errors
			}

			itemsByKey := make(map[PK]*V)
			for _, item := range items {
				key := getPKFromItem(item)
				transformedItem := transform(item)
				itemsByKey[key] = &transformedItem
			}

			result := make([]*V, len(keys))
			for i, key := range keys {
				result[i] = itemsByKey[getPKFromKey(key)]
			}

			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}
