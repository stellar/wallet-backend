package dataloaders

import (
	"context"
	"fmt"

	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type AccountColumnsKey struct {
	TxHash        string
	OperationID   int64
	StateChangeID string
	Columns       string
}

// accountsByTxHashLoader creates a dataloader for fetching accounts by transaction hash
// This prevents N+1 queries when multiple transactions request their accounts
// The loader batches multiple transaction hashes into a single database query
func accountsByTxHashLoader(models *data.Models) *dataloadgen.Loader[AccountColumnsKey, []*types.Account] {
	return newOneToManyLoader(
		func(ctx context.Context, keys []AccountColumnsKey) ([]*types.AccountWithTxHash, error) {
			txHashes := make([]string, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				txHashes[i] = key.TxHash
			}
			return models.Account.BatchGetByTxHashes(ctx, txHashes, columns)
		},
		func(item *types.AccountWithTxHash) string {
			return item.TxHash
		},
		func(key AccountColumnsKey) string {
			return key.TxHash
		},
		func(item *types.AccountWithTxHash) types.Account {
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

// accountByStateChangeIDLoader creates a dataloader for fetching accounts by state change ID
// This prevents N+1 queries when multiple state changes request their accounts
// The loader batches multiple state change IDs into a single database query
func accountByStateChangeIDLoader(models *data.Models) *dataloadgen.Loader[AccountColumnsKey, *types.Account] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []AccountColumnsKey) ([]*types.AccountWithStateChangeID, error) {
			columns := keys[0].Columns
			scIDs := make([]string, len(keys))
			for i, key := range keys {
				scIDs[i] = key.StateChangeID
			}
			scToIDs, scOrders, err := parseStateChangeIDs(scIDs)
			if err != nil {
				return nil, fmt.Errorf("parsing state change IDs: %w", err)
			}
			return models.Account.BatchGetByStateChangeIDs(ctx, scToIDs, scOrders, columns)
		},
		func(item *types.AccountWithStateChangeID) string {
			return item.StateChangeID
		},
		func(key AccountColumnsKey) string {
			return key.StateChangeID
		},
		func(item *types.AccountWithStateChangeID) types.Account {
			return item.Account
		},
	)
}
