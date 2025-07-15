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

	// OperationsByStateChangeIDLoader batches requests for operations by state change ID
	// Used by StateChange.operation field resolver to prevent N+1 queries
	OperationsByStateChangeIDLoader *dataloadgen.Loader[string, *types.Operation]

	// TransactionsByStateChangeIDLoader batches requests for transactions by state change ID
	// Used by StateChange.transaction field resolver to prevent N+1 queries
	TransactionsByStateChangeIDLoader *dataloadgen.Loader[string, *types.Transaction]
}

// NewDataloaders creates a new instance of all dataloaders
// This is called during GraphQL server initialization
// The dataloaders are then injected into GraphQL context by middleware
// GraphQL resolvers access these loaders to batch database queries efficiently
func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader:          operationsByTxHashLoader(models),
		TransactionsByAccountLoader:       transactionsByAccountLoader(models),
		OperationsByAccountLoader:         operationsByAccountLoader(models),
		StateChangesByAccountLoader:       stateChangesByAccountLoader(models),
		AccountsByTxHashLoader:            accountsByTxHashLoader(models),
		StateChangesByTxHashLoader:        stateChangesByTxHashLoader(models),
		TransactionsByOperationIDLoader:   txByOperationIDLoader(models),
		AccountsByOperationIDLoader:       accountsByOperationIDLoader(models),
		StateChangesByOperationIDLoader:   stateChangesByOperationIDLoader(models),
		OperationsByStateChangeIDLoader:   operationsByStateChangeIDLoader(models),
		TransactionsByStateChangeIDLoader: transactionsByStateChangeIDLoader(models),
	}
}

// opByTxHashLoader creates a dataloader for fetching operations by transaction hash
// This prevents N+1 queries when multiple transactions request their operations
// The loader batches multiple transaction hashes into a single database query
func operationsByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
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
func transactionsByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Transaction] {
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
func operationsByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
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

// accountsByTxHashLoader creates a dataloader for fetching accounts by transaction hash
// This prevents N+1 queries when multiple transactions request their accounts
// The loader batches multiple transaction hashes into a single database query
func accountsByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Account] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (tx hashes) and returns data for each
		func(ctx context.Context, keys []string) ([][]*types.Account, []error) {
			// Single database query for all requested transaction hashes
			accountsWithTxHash, err := models.Account.BatchGetByTxHash(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group accounts by transaction hash
			// accountsWithTxHash is a flat slice, so we need to group them by tx hash.
			// The loader expects a slice of slices, one for each key.
			accountsByTxHash := make(map[string][]*types.Account)
			for _, accountWithTxHash := range accountsWithTxHash {
				// Extract just the Account part from AccountWithTxHash wrapper
				account := &accountWithTxHash.Account
				accountsByTxHash[accountWithTxHash.TxHash] = append(accountsByTxHash[accountWithTxHash.TxHash], account)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.Account, len(keys))
			for i, key := range keys {
				result[i] = accountsByTxHash[key] // Will be nil slice if no accounts found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// stateChangesByTxHashLoader creates a dataloader for fetching state changes by transaction hash
// This prevents N+1 queries when multiple transactions request their state changes
// The loader batches multiple transaction hashes into a single database query
func stateChangesByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.StateChange] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (tx hashes) and returns data for each
		func(ctx context.Context, keys []string) ([][]*types.StateChange, []error) {
			// Single database query for all requested transaction hashes
			stateChanges, err := models.StateChanges.BatchGetByTxHash(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group state changes by transaction hash
			// stateChanges is a flat slice, so we need to group them by tx hash.
			// The loader expects a slice of slices, one for each key.
			stateChangesByTxHash := make(map[string][]*types.StateChange)
			for _, stateChange := range stateChanges {
				stateChangesByTxHash[stateChange.TxHash] = append(stateChangesByTxHash[stateChange.TxHash], stateChange)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.StateChange, len(keys))
			for i, key := range keys {
				result[i] = stateChangesByTxHash[key] // Will be nil slice if no state changes found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// txByOperationIDLoader creates a dataloader for fetching transactions by operation ID
// This prevents N+1 queries when multiple operations request their transaction
// The loader batches multiple operation IDs into a single database query
func txByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, *types.Transaction] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (operation IDs) and returns data for each
		func(ctx context.Context, keys []int64) ([]*types.Transaction, []error) {
			// Single database query for all requested operation IDs
			transactions, err := models.Transactions.BatchGetByOperationID(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Create a map to find transactions for each operation ID
			// Since we query by operation ID and each operation belongs to exactly one transaction,
			// we need to map back to the requested operation IDs
			transactionMap := make(map[int64]*types.Transaction)

			// For this simple case, we'll assume the database query returns transactions
			// in the same order as the operation IDs were requested
			// A more robust implementation would query operations first to get the mapping
			for i, operationID := range keys {
				if i < len(transactions) {
					transactionMap[operationID] = transactions[i]
				}
			}

			// Create result slice matching the order of input keys
			result := make([]*types.Transaction, len(keys))
			for i, key := range keys {
				result[i] = transactionMap[key] // Will be nil if no transaction found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// accountsByOperationIDLoader creates a dataloader for fetching accounts by operation ID
// This prevents N+1 queries when multiple operations request their accounts
// The loader batches multiple operation IDs into a single database query
func accountsByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, []*types.Account] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (operation IDs) and returns data for each
		func(ctx context.Context, keys []int64) ([][]*types.Account, []error) {
			// Single database query for all requested operation IDs
			accountsWithOpID, err := models.Account.BatchGetByOperationID(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group accounts by operation ID
			accountsByOpID := make(map[int64][]*types.Account)
			for _, accountWithOpID := range accountsWithOpID {
				// Extract just the Account part from AccountWithOperationID wrapper
				account := &accountWithOpID.Account
				accountsByOpID[accountWithOpID.OperationID] = append(accountsByOpID[accountWithOpID.OperationID], account)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.Account, len(keys))
			for i, key := range keys {
				result[i] = accountsByOpID[key] // Will be nil slice if no accounts found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

// stateChangesByOperationIDLoader creates a dataloader for fetching state changes by operation ID
// This prevents N+1 queries when multiple operations request their state changes
// The loader batches multiple operation IDs into a single database query
func stateChangesByOperationIDLoader(models *data.Models) *dataloadgen.Loader[int64, []*types.StateChange] {
	return dataloadgen.NewLoader(
		// Batch function - receives multiple keys (operation IDs) and returns data for each
		func(ctx context.Context, keys []int64) ([][]*types.StateChange, []error) {
			// Single database query for all requested operation IDs
			stateChanges, err := models.StateChanges.BatchGetByOperationID(ctx, keys)
			if err != nil {
				// Return error for all keys if batch query fails
				return nil, []error{err}
			}

			// Group state changes by operation ID
			stateChangesByOpID := make(map[int64][]*types.StateChange)
			for _, stateChange := range stateChanges {
				stateChangesByOpID[stateChange.OperationID] = append(stateChangesByOpID[stateChange.OperationID], stateChange)
			}

			// Create result slice matching the order of input keys
			result := make([][]*types.StateChange, len(keys))
			for i, key := range keys {
				result[i] = stateChangesByOpID[key] // Will be nil slice if no state changes found
			}
			return result, nil
		},
		// Configure batch size - maximum number of keys to batch together
		dataloadgen.WithBatchCapacity(100),
		// Configure wait time - how long to wait for more requests before executing batch
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

func operationsByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[string, *types.Operation] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []string) ([]*types.Operation, []error) {
			operations, err := models.Operations.BatchGetByStateChangeID(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			// Group operations by state change ID
			operationsByStateChangeID := make(map[string]*types.Operation)
			for _, operationWithStateChangeID := range operations {
				operationsByStateChangeID[operationWithStateChangeID.StateChangeID] = &operationWithStateChangeID.Operation
			}

			// Create result slice matching the order of input keys
			result := make([]*types.Operation, len(keys))
			for i, key := range keys {
				result[i] = operationsByStateChangeID[key] // Will be nil if no operation found
			}
			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}

func transactionsByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[string, *types.Transaction] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []string) ([]*types.Transaction, []error) {
			transactions, err := models.Transactions.BatchGetByStateChangeID(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			// Group transactions by state change ID
			transactionsByStateChangeID := make(map[string]*types.Transaction)
			for _, transactionWithStateChangeID := range transactions {
				transactionsByStateChangeID[transactionWithStateChangeID.StateChangeID] = &transactionWithStateChangeID.Transaction
			}

			// Create result slice matching the order of input keys
			result := make([]*types.Transaction, len(keys))
			for i, key := range keys {
				result[i] = transactionsByStateChangeID[key] // Will be nil if no transaction found
			}
			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5*time.Millisecond),
	)
}
