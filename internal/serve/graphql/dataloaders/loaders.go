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
				return [][]*types.Operation{operations}, nil
			},
			dataloadgen.WithBatchCapacity(100),
			dataloadgen.WithWait(5 * time.Millisecond),
		),
	}
}
