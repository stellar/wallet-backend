// GraphQL DataLoaders package - implements efficient batching for GraphQL resolvers
// DataLoaders solve the N+1 query problem by batching multiple requests into single database queries
// This is essential for GraphQL performance when resolving relationship fields
package dataloaders

import (
	"context"
	"time"

	// dataloadgen provides type-safe dataloader generation for Go
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
	OperationsByTxHashLoader *dataloadgen.Loader[string, []*types.Operation]

	// TransactionsByAccountLoader batches requests for transactions by account address
	// Used by Account.transactions field resolver to prevent N+1 queries
	TransactionsByAccountLoader *dataloadgen.Loader[string, []*types.Transaction]

	// OperationsByAccountLoader batches requests for operations by account address
	// Used by Account.operations field resolver to prevent N+1 queries
	OperationsByAccountLoader *dataloadgen.Loader[string, []*types.Operation]

	// StateChangesByAccountLoader batches requests for state changes by account address
	// Used by Account.statechanges field resolver to prevent N+1 queries
	StateChangesByAccountLoader *dataloadgen.Loader[string, []*types.StateChange]
}

// opByTxHashLoader creates a dataloader for fetching operations by transaction hash
// This prevents N+1 queries when multiple transactions request their operations
// The loader batches multiple transaction hashes into a single database query
func opByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (tx hashes) and returns data for each
		func(ctx context.Context, keys []string) ([][]*types.Operation, []error) {
			// Single database query for all requested transaction hashes
			operations, err := models.Operations.BatchGetByTxHash(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group operations by transaction hash
			// operations is a flat slice, so we need to group them by tx hash.
			// The loader expects a slice of slices, one for each key.
			operationsByTxHash := make(map[string][]*types.Operation)
			for _, operation := range operations {
				operationsByTxHash[operation.TxHash] = append(operationsByTxHash[operation.TxHash], operation)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.Operation, len(keys))
			for i, key := range keys {
				result[i] = operationsByTxHash[key] // Will be nil slice if no operations found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// txByAccountLoader creates a dataloader for fetching transactions by account address
// This prevents N+1 queries when multiple accounts request their transactions
// The loader batches multiple account addresses into a single database query
func txByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Transaction] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (account addresses) and returns data for each
		func(ctx context.Context, keys []string) ([][]*types.Transaction, []error) {
			// Single database query for all requested account addresses
			transactions, err := models.Transactions.BatchGetByAccount(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group transactions by account address
			transactionsByAccount := make(map[string][]*types.Transaction)
			for _, transaction := range transactions {
				// Extract the Transaction from the wrapper struct
				transactionsByAccount[transaction.AccountID] = append(transactionsByAccount[transaction.AccountID], &transaction.Transaction)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.Transaction, len(keys))
			for i, key := range keys {
				result[i] = transactionsByAccount[key] // Will be nil slice if no transactions found
			}
			return result, nil
		},
		// Uses default batch configuration (no custom capacity or wait time)
	)
}

// opByAccountLoader creates a dataloader for fetching operations by account address
// This prevents N+1 queries when multiple accounts request their operations
// The loader batches multiple account addresses into a single database query
func opByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (account addresses) and returns data for each
		func(ctx context.Context, keys []string) ([][]*types.Operation, []error) {
			// Single database query for all requested account addresses
			operationsWithAccounts, err := models.Operations.BatchGetByAccount(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group operations by account address
			// operations is a flat slice, so we need to group them by account address.
			// The loader expects a slice of slices, one for each key.
			operationsByAccount := make(map[string][]*types.Operation)
			for _, opWithAccount := range operationsWithAccounts {
				// Extract just the Operation part from OperationWithAccount wrapper
				operation := &opWithAccount.Operation
				operationsByAccount[opWithAccount.AccountID] = append(operationsByAccount[opWithAccount.AccountID], operation)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.Operation, len(keys))
			for i, key := range keys {
				result[i] = operationsByAccount[key] // Will be nil slice if no operations found
			}
			return result, nil
		},
		// Configure batch size and wait time for optimal performance
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// stateChangesByAccountLoader creates a dataloader for fetching state changes by account address

func stateChangesByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.StateChange] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []string) ([][]*types.StateChange, []error) {
			stateChanges, err := models.StateChanges.BatchGetByAccount(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			// Group state changes by account address
			stateChangesByAccount := make(map[string][]*types.StateChange)
			for _, stateChange := range stateChanges {
				stateChangesByAccount[stateChange.AccountID] = append(stateChangesByAccount[stateChange.AccountID], stateChange)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.StateChange, len(keys))
			for i, key := range keys {
				result[i] = stateChangesByAccount[key] // Will be nil slice if no state changes found
			}
			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// NewDataloaders creates a new instance of all dataloaders
// This is called during GraphQL server initialization
// The dataloaders are then injected into GraphQL context by middleware
// GraphQL resolvers access these loaders to batch database queries efficiently
func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader: opByTxHashLoader(models),
		TransactionsByAccountLoader: txByAccountLoader(models),
		OperationsByAccountLoader: opByAccountLoader(models),
		StateChangesByAccountLoader: stateChangesByAccountLoader(models),
	}
}
