package dataloaders

import (
	"context"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type AccountColumnsKey struct {
	ToID        int64
	OperationID int64
	Columns     string
}

// accountsByToIDLoader creates a dataloader for fetching accounts by transaction ToID
// This prevents N+1 queries when multiple transactions request their accounts
// The loader batches multiple transaction ToIDs into a single database query
func accountsByToIDLoader(models *data.Models) *dataloadgen.Loader[AccountColumnsKey, []*types.Account] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []AccountColumnsKey) ([]*types.AccountWithToID, error) {
			toIDs := make([]int64, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				toIDs[i] = key.ToID
			}
			return models.Account.BatchGetByToIDs(ctx, toIDs, columns)
		},
		func(item *types.AccountWithToID) int64 {
			return item.ToID
		},
		func(key AccountColumnsKey) int64 {
			return key.ToID
		},
		func(item *types.AccountWithToID) types.Account {
			return item.Account
		},
	)
}

// accountsByOperationIDLoader creates a dataloader for fetching accounts by operation ID
// This prevents N+1 queries when multiple operations request their accounts
// The loader batches multiple operation IDs into a single database query
func accountsByOperationIDLoader(models *data.Models) *dataloadgen.Loader[AccountColumnsKey, []*types.Account] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []AccountColumnsKey) ([]*types.AccountWithOperationID, error) {
			operationIDs := make([]int64, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				operationIDs[i] = key.OperationID
			}
			return models.Account.BatchGetByOperationIDs(ctx, operationIDs, columns)
		},
		func(item *types.AccountWithOperationID) int64 {
			return item.OperationID
		},
		func(key AccountColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.AccountWithOperationID) types.Account {
			return item.Account
		},
	)
}
