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

type TransactionColumnsKey struct {
	AccountID       string
	OperationID     int64
	StateChangeID   string
	Columns         string
	LedgerCreatedAt time.Time // parent state change's ledger time; pins the partition column for BatchGetByStateChangeIDs
}

// txByOperationIDLoader creates a dataloader for fetching transactions by operation ID
// This prevents N+1 queries when multiple operations request their transaction
// The loader batches multiple operation IDs into a single database query
func transactionByOperationIDLoader(models *data.Models, m *metrics.DataloaderMetrics) *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []TransactionColumnsKey) ([]*types.TransactionWithOperationID, error) {
			operationIDs := make([]int64, len(keys))
			columns := keys[0].Columns
			for i, key := range keys {
				operationIDs[i] = key.OperationID
			}
			return models.Transactions.BatchGetByOperationIDs(ctx, operationIDs, columns)
		},
		func(item *types.TransactionWithOperationID) int64 {
			return item.OperationID
		},
		func(key TransactionColumnsKey) int64 {
			return key.OperationID
		},
		func(item *types.TransactionWithOperationID) types.Transaction {
			return item.Transaction
		},
		transactionColumnsKeyShape,
		"TransactionsByOperationIDLoader",
		m,
	)
}

// transactionByStateChangeIDLoader creates a dataloader for fetching transactions by state change ID
// This prevents N+1 queries when multiple state changes request their transactions
// The loader batches multiple state change IDs into a single database query
func transactionByStateChangeIDLoader(models *data.Models, m *metrics.DataloaderMetrics) *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction] {
	return newOneToOneLoader(
		func(ctx context.Context, keys []TransactionColumnsKey) ([]*types.TransactionWithStateChangeID, error) {
			columns := keys[0].Columns
			scIDs := make([]string, len(keys))
			ledgerCreatedAts := make([]time.Time, len(keys))
			for i, key := range keys {
				scIDs[i] = key.StateChangeID
				ledgerCreatedAts[i] = key.LedgerCreatedAt
			}
			scToIDs, scOpIDs, scOrders, err := parseStateChangeIDs(scIDs)
			if err != nil {
				return nil, fmt.Errorf("parsing state change IDs: %w", err)
			}
			return models.Transactions.BatchGetByStateChangeIDs(ctx, scToIDs, scOpIDs, scOrders, ledgerCreatedAts, columns)
		},
		func(item *types.TransactionWithStateChangeID) string {
			return item.StateChangeID
		},
		func(key TransactionColumnsKey) string {
			return key.StateChangeID
		},
		func(item *types.TransactionWithStateChangeID) types.Transaction {
			return item.Transaction
		},
		transactionColumnsKeyShape,
		"TransactionByStateChangeIDLoader",
		m,
	)
}

// transactionColumnsKeyShape is the query shape for TransactionColumnsKey: Columns is the only
// field that determines the SQL statement the fetcher builds (these loaders take no limit or
// cursor), so any two keys requesting different columns must land in different batch groups.
func transactionColumnsKeyShape(key TransactionColumnsKey) QueryShape {
	return QueryShape{Columns: key.Columns}
}
