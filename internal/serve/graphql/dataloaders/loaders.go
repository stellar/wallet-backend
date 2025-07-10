package dataloaders

import (
	"context"
	"time"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/vikstrous/dataloadgen"
)

type Dataloaders struct {
	OperationsByTxHashLoader *dataloadgen.Loader[string, []*types.Operation]
	TransactionsByAccountLoader *dataloadgen.Loader[string, []*types.Transaction]
}

func opByTxHashLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Operation] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []string) ([][]*types.Operation, []error) {
			operations, err := models.Operations.BatchGetByTxHash(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			// operations is a flat slice, so we need to group them by tx hash.
			// The loader expects a slice of slices, one for each key.
			operationsByTxHash := make(map[string][]*types.Operation)
			for _, operation := range operations {
				operationsByTxHash[operation.TxHash] = append(operationsByTxHash[operation.TxHash], operation)
			}
			result := make([][]*types.Operation, len(keys))
			for i, key := range keys {
				result[i] = operationsByTxHash[key]
			}
			return result, nil
		},
		dataloadgen.WithBatchCapacity(100),
		dataloadgen.WithWait(5 * time.Millisecond),
	)
}

func txByAccountLoader(models *data.Models) *dataloadgen.Loader[string, []*types.Transaction] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []string) ([][]*types.Transaction, []error) {
			transactions, err := models.Transactions.BatchGetByAccount(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			transactionsByAccount := make(map[string][]*types.Transaction)
			for _, transaction := range transactions {
				transactionsByAccount[transaction.AccountID] = append(transactionsByAccount[transaction.AccountID], &transaction.Transaction)
			}
			result := make([][]*types.Transaction, len(keys))
			for i, key := range keys {
				result[i] = transactionsByAccount[key]
			}
			return result, nil
		},
	)
}

func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader: opByTxHashLoader(models),
		TransactionsByAccountLoader: txByAccountLoader(models),
	}
}
