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

type OperationColumnsWithTxHashKey struct {
	TxHash string
	Columns string
}

type TransactionColumnsWithAccountKey struct {
	AccountID string
	Columns string
}

// Dataloaders struct holds all dataloader instances for GraphQL resolvers
// Each dataloader batches requests for a specific type of data relationship
// GraphQL resolvers use these to efficiently load related data
type Dataloaders struct {
	// OperationsByTxHashLoader batches requests for operations by transaction hash
	// Used by Transaction.operations field resolver to prevent N+1 queries
	OperationsByTxHashLoader *dataloadgen.Loader[OperationColumnsWithTxHashKey, []*types.Operation]

	// TransactionsByAccountLoader batches requests for transactions by account address
	// Used by Account.transactions field resolver to prevent N+1 queries
	TransactionsByAccountLoader *dataloadgen.Loader[TransactionColumnsWithAccountKey, []*types.Transaction]

	// OperationsByAccountLoader batches requests for operations by account address
	// Used by Account.operations field resolver to prevent N+1 queries
	OperationsByAccountLoader *dataloadgen.Loader[string, []*types.Operation]

	// StateChangesByAccountLoader batches requests for state changes by account address
	// Used by Account.statechanges field resolver to prevent N+1 queries
	StateChangesByAccountLoader *dataloadgen.Loader[string, []*types.StateChange]

	// AccountsByTxHashLoader batches requests for accounts by transaction hash
	// Used by Transaction.accounts field resolver to prevent N+1 queries
	AccountsByTxHashLoader *dataloadgen.Loader[string, []*types.Account]

	// StateChangesByTxHashLoader batches requests for state changes by transaction hash
	// Used by Transaction.stateChanges field resolver to prevent N+1 queries
	StateChangesByTxHashLoader *dataloadgen.Loader[string, []*types.StateChange]

	// TransactionsByOperationIDLoader batches requests for transactions by operation ID
	// Used by Operation.transaction field resolver to prevent N+1 queries
	TransactionsByOperationIDLoader *dataloadgen.Loader[int64, *types.Transaction]

	// AccountsByOperationIDLoader batches requests for accounts by operation ID
	// Used by Operation.accounts field resolver to prevent N+1 queries
	AccountsByOperationIDLoader *dataloadgen.Loader[int64, []*types.Account]

	// StateChangesByOperationIDLoader batches requests for state changes by operation ID
	// Used by Operation.stateChanges field resolver to prevent N+1 queries
	StateChangesByOperationIDLoader *dataloadgen.Loader[int64, []*types.StateChange]

	// OperationByStateChangeIDLoader batches requests for operations by state change ID
	// Used by StateChange.operation field resolver to prevent N+1 queries
	OperationByStateChangeIDLoader *dataloadgen.Loader[string, *types.Operation]

	// TransactionByStateChangeIDLoader batches requests for transactions by state change ID
	// Used by StateChange.transaction field resolver to prevent N+1 queries
	TransactionByStateChangeIDLoader *dataloadgen.Loader[string, *types.Transaction]
}

// NewDataloaders creates a new instance of all dataloaders
// This is called during GraphQL server initialization
// The dataloaders are then injected into GraphQL context by middleware
// GraphQL resolvers access these loaders to batch database queries efficiently
func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader:         operationsByTxHashLoader(models),
		TransactionsByAccountLoader:      transactionsByAccountLoader(models),
		// OperationsByAccountLoader:        operationsByAccountLoader(models),
		// StateChangesByAccountLoader:      stateChangesByAccountLoader(models),
		// AccountsByTxHashLoader:           accountsByTxHashLoader(models),
		// StateChangesByTxHashLoader:       stateChangesByTxHashLoader(models),
		// TransactionsByOperationIDLoader:  txByOperationIDLoader(models),
		// AccountsByOperationIDLoader:      accountsByOperationIDLoader(models),
		// StateChangesByOperationIDLoader:  stateChangesByOperationIDLoader(models),
		// OperationByStateChangeIDLoader:   operationByStateChangeIDLoader(models),
		// TransactionByStateChangeIDLoader: transactionByStateChangeIDLoader(models),
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
func newOneToOneLoader[K comparable, V any, T any](
	fetcher func(ctx context.Context, keys []K) ([]T, error),
	getKey func(item T) K,
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

			itemsByKey := make(map[K]*V)
			for _, item := range items {
				key := getKey(item)
				transformedItem := transform(item)
				itemsByKey[key] = &transformedItem
			}
			result := make([]*V, len(keys))
			for i, key := range keys {
				result[i] = itemsByKey[key]
			}

			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// opByTxHashLoader creates a dataloader for fetching operations by transaction hash
// This prevents N+1 queries when multiple transactions request their operations
// The loader batches multiple transaction hashes into a single database query
func operationsByTxHashLoader(models *data.Models) *dataloadgen.Loader[OperationColumnsWithTxHashKey, []*types.Operation] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []OperationColumnsWithTxHashKey) ([]*types.Operation, error) {
			txHashes := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				txHashes[i] = key.TxHash
			}
			return models.Operations.BatchGetByTxHashes(ctx, txHashes, columns)
		},
		func(item *types.Operation) string {
			return item.TxHash
		},
		func(key OperationColumnsWithTxHashKey) string {
			return key.TxHash
		},
		func(item *types.Operation) types.Operation {
			return *item
		},
	)
}

// txByAccountLoader creates a dataloader for fetching transactions by account address
// This prevents N+1 queries when multiple accounts request their transactions
// The loader batches multiple account addresses into a single database query
func transactionsByAccountLoader(models *data.Models) *dataloadgen.Loader[TransactionColumnsWithAccountKey, []*types.Transaction] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []TransactionColumnsWithAccountKey) ([]*types.TransactionWithAccountID, error) {
			accountIDs := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				accountIDs[i] = key.AccountID
			}
			return models.Transactions.BatchGetByAccountAddresses(ctx, accountIDs, columns)
		},
		func(item *types.TransactionWithAccountID) string {
			return item.AccountID
		},
		func(key TransactionColumnsWithAccountKey) string {
			return key.AccountID
		},
		func(item *types.TransactionWithAccountID) types.Transaction {
			return item.Transaction
		},
	)
}

// // opByAccountLoader creates a dataloader for fetching operations by account address
// // This prevents N+1 queries when multiple accounts request their operations
// // The loader batches multiple account addresses into a single database query
// func operationsByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []string) ([]*types.OperationWithAccountID, error) {
// 			return models.Operations.BatchGetByAccountAddresses(ctx, keys)
// 		},
// 		func(item *types.OperationWithAccountID) string {
// 			return item.AccountID
// 		},
// 		func(item *types.OperationWithAccountID) types.Operation {
// 			return item.Operation
// 		},
// 	)
// }

// // stateChangesByAccountLoader creates a dataloader for fetching state changes by account address
// func stateChangesByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.StateChange] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []string) ([]*types.StateChange, error) {
// 			return models.StateChanges.BatchGetByAccountAddresses(ctx, keys)
// 		},
// 		func(item *types.StateChange) string {
// 			return item.AccountID
// 		},
// 		func(item *types.StateChange) types.StateChange {
// 			return *item
// 		},
// 	)
// }

// // accountsByTxHashLoader creates a dataloader for fetching accounts by transaction hash
// // This prevents N+1 queries when multiple transactions request their accounts
// // The loader batches multiple transaction hashes into a single database query
// func accountsByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Account] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []string) ([]*types.AccountWithTxHash, error) {
// 			return models.Account.BatchGetByTxHashes(ctx, keys)
// 		},
// 		func(item *types.AccountWithTxHash) string {
// 			return item.TxHash
// 		},
// 		func(item *types.AccountWithTxHash) types.Account {
// 			return item.Account
// 		},
// 	)
// }

// // stateChangesByTxHashLoader creates a dataloader for fetching state changes by transaction hash
// // This prevents N+1 queries when multiple transactions request their state changes
// // The loader batches multiple transaction hashes into a single database query
// func stateChangesByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.StateChange] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []string) ([]*types.StateChange, error) {
// 			return models.StateChanges.BatchGetByTxHashes(ctx, keys)
// 		},
// 		func(item *types.StateChange) string {
// 			return item.TxHash
// 		},
// 		func(item *types.StateChange) types.StateChange {
// 			return *item
// 		},
// 	)
// }

// // txByOperationIDLoader creates a dataloader for fetching transactions by operation ID
// // This prevents N+1 queries when multiple operations request their transaction
// // The loader batches multiple operation IDs into a single database query
// func txByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, *types.Transaction] {
// 	return newOneToOneLoader(
// 		func(ctx context.Context, keys []int64) ([]*types.TransactionWithOperationID, error) {
// 			return models.Transactions.BatchGetByOperationIDs(ctx, keys)
// 		},
// 		func(item *types.TransactionWithOperationID) int64 {
// 			return item.OperationID
// 		},
// 		func(item *types.TransactionWithOperationID) types.Transaction {
// 			return item.Transaction
// 		},
// 	)
// }

// // accountsByOperationIDLoader creates a dataloader for fetching accounts by operation ID
// // This prevents N+1 queries when multiple operations request their accounts
// // The loader batches multiple operation IDs into a single database query
// func accountsByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, []*types.Account] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []int64) ([]*types.AccountWithOperationID, error) {
// 			return models.Account.BatchGetByOperationIDs(ctx, keys)
// 		},
// 		func(item *types.AccountWithOperationID) int64 {
// 			return item.OperationID
// 		},
// 		func(item *types.AccountWithOperationID) types.Account {
// 			return item.Account
// 		},
// 	)
// }

// // stateChangesByOperationIDLoader creates a dataloader for fetching state changes by operation ID
// // This prevents N+1 queries when multiple operations request their state changes
// // The loader batches multiple operation IDs into a single database query
// func stateChangesByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, []*types.StateChange] {
// 	return newOneToManyLoader(
// 		func(ctx context.Context, keys []int64) ([]*types.StateChange, error) {
// 			return models.StateChanges.BatchGetByOperationIDs(ctx, keys)
// 		},
// 		func(item *types.StateChange) int64 {
// 			return item.OperationID
// 		},
// 		func(item *types.StateChange) types.StateChange {
// 			return *item
// 		},
// 	)
// }

// // operationByStateChangeIDLoader creates a dataloader for fetching operations by state change ID
// // This prevents N+1 queries when multiple state changes request their operations
// // The loader batches multiple state change IDs into a single database query
// func operationByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[string, *types.Operation] {
// 	return newOneToOneLoader(
// 		func(ctx context.Context, keys []string) ([]*types.OperationWithStateChangeID, error) {
// 			return models.Operations.BatchGetByStateChangeIDs(ctx, keys)
// 		},
// 		func(item *types.OperationWithStateChangeID) string {
// 			return item.StateChangeID
// 		},
// 		func(item *types.OperationWithStateChangeID) types.Operation {
// 			return item.Operation
// 		},
// 	)
// }

// // transactionByStateChangeIDLoader creates a dataloader for fetching transactions by state change ID
// // This prevents N+1 queries when multiple state changes request their transactions
// // The loader batches multiple state change IDs into a single database query
// func transactionByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[string, *types.Transaction] {
// 	return newOneToOneLoader(
// 		func(ctx context.Context, keys []string) ([]*types.TransactionWithStateChangeID, error) {
// 			return models.Transactions.BatchGetByStateChangeIDs(ctx, keys)
// 		},
// 		func(item *types.TransactionWithStateChangeID) string {
// 			return item.StateChangeID
// 		},
// 		func(item *types.TransactionWithStateChangeID) types.Transaction {
// 			return item.Transaction
// 		},
// 	)
// }
