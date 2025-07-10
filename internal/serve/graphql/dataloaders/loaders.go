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
}

func NewDataloaders(models *data.Models) *Dataloaders {
	return &Dataloaders{
		OperationsByTxHashLoader: dataloadgen.NewLoader(
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
		),
	}
}
