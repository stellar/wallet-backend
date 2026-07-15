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
	"github.com/stellar/wallet-backend/internal/metrics"
)

// loaderBatchCapacity fires a batch as soon as this many distinct keys are enqueued.
// It matches the maximum page size, so a full page's fan-out never waits on the timer.
const loaderBatchCapacity = 100

// loaderWait is the collection window before an under-capacity batch fires. Keys for one
// GraphQL request arrive in a near-instant burst (sibling resolvers run concurrently), so
// the window only exists to catch that burst; each nested loader level serializes one
// window, so this is a per-level latency floor, not a throughput knob.
const loaderWait = 1 * time.Millisecond

// Dataloaders struct holds all dataloader instances for GraphQL resolvers
// Each dataloader batches requests for a specific type of data relationship
// GraphQL resolvers use these to efficiently load related data
type Dataloaders struct {
	// OperationsByToIDLoader batches requests for operations by transaction ToID
	// Used by Transaction.operations field resolver to prevent N+1 queries
	OperationsByToIDLoader *dataloadgen.Loader[OperationColumnsKey, []*types.OperationWithCursor]

	// AccountsByToIDLoader batches requests for accounts by transaction ToID
	// Used by Transaction.accounts field resolver to prevent N+1 queries
	AccountsByToIDLoader *dataloadgen.Loader[AccountColumnsKey, []*types.Account]

	// StateChangesByToIDLoader batches requests for state changes by to_id
	// Used by Transaction.stateChanges field resolver to prevent N+1 queries
	StateChangesByToIDLoader *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor]

	// TransactionsByOperationIDLoader batches requests for transactions by operation ID
	// Used by Operation.transaction field resolver to prevent N+1 queries
	TransactionsByOperationIDLoader *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction]

	// AccountsByOperationIDLoader batches requests for accounts by operation ID
	// Used by Operation.accounts field resolver to prevent N+1 queries
	AccountsByOperationIDLoader *dataloadgen.Loader[AccountColumnsKey, []*types.Account]

	// StateChangesByOperationIDLoader batches requests for state changes by operation ID
	// Used by Operation.stateChanges field resolver to prevent N+1 queries
	StateChangesByOperationIDLoader *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChangeWithCursor]

	// OperationByStateChangeIDLoader batches requests for operations by state change ID
	// Used by StateChange.operation field resolver to prevent N+1 queries
	OperationByStateChangeIDLoader *dataloadgen.Loader[OperationColumnsKey, *types.Operation]

	// TransactionByStateChangeIDLoader batches requests for transactions by state change ID
	// Used by StateChange.transaction field resolver to prevent N+1 queries
	TransactionByStateChangeIDLoader *dataloadgen.Loader[TransactionColumnsKey, *types.Transaction]

	// AccountOperationsByToIDLoader batches account-scoped operations by transaction ToID
	// Used by AccountTransactionEdge.operations
	AccountOperationsByToIDLoader *dataloadgen.Loader[OperationColumnsKey, []*types.Operation]

	// AccountStateChangesByToIDLoader batches account-scoped state changes by transaction ToID
	// Used by AccountTransactionEdge.stateChanges
	AccountStateChangesByToIDLoader *dataloadgen.Loader[StateChangeColumnsKey, []*types.StateChange]
}

// NewDataloaders creates a new instance of all dataloaders
// This is called during GraphQL server initialization
// The dataloaders are then injected into GraphQL context by middleware
// GraphQL resolvers access these loaders to batch database queries efficiently
//
// m may be nil (e.g. in tests constructing loaders directly without a metrics handle); every
// instrumentation call site guards against it.
func NewDataloaders(models *data.Models, m *metrics.DataloaderMetrics) *Dataloaders {
	return &Dataloaders{
		OperationsByToIDLoader:           operationsByToIDLoader(models, m),
		OperationByStateChangeIDLoader:   operationByStateChangeIDLoader(models, m),
		TransactionByStateChangeIDLoader: transactionByStateChangeIDLoader(models, m),
		TransactionsByOperationIDLoader:  transactionByOperationIDLoader(models, m),
		StateChangesByToIDLoader:         stateChangesByToIDLoader(models, m),
		StateChangesByOperationIDLoader:  stateChangesByOperationIDLoader(models, m),
		AccountsByToIDLoader:             accountsByToIDLoader(models, m),
		AccountsByOperationIDLoader:      accountsByOperationIDLoader(models, m),
		AccountOperationsByToIDLoader:    accountOperationsByToIDLoader(models, m),
		AccountStateChangesByToIDLoader:  accountStateChangesByToIDLoader(models, m),
	}
}

// observeBatch records batch-size and fetch-duration metrics for one dataloader batch. It
// returns a func to call when the batch's fetch work is complete (deferred by the caller), so
// duration spans every shape-group fetch the batch was split into. m may be nil.
func observeBatch(m *metrics.DataloaderMetrics, name string, keyCount int) func() {
	if m == nil {
		return func() {}
	}
	m.BatchSize.WithLabelValues(name).Observe(float64(keyCount))
	start := time.Now()
	return func() {
		m.FetchDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
	}
}

// QueryShape captures the query-shaping parameters of a dataloader key: the parameters a fetcher
// closure reads off of keys[0] and applies to the whole slice it's given (Columns, Limit, Cursor,
// SortOrder). dataloadgen hands a batch to the fetch function as one flat, ungrouped slice of
// keys, so two keys differing in any of these fields (e.g. two GraphQL aliases of the same field
// with different sub-selections) must never be fetched together — the fetcher would apply one
// key's shape to both. newOneToManyLoader and newOneToOneLoader group each batch by QueryShape
// before calling the fetcher, so every slice the fetcher sees is shape-homogeneous and keys[0] is
// representative of the whole slice. One-to-one loaders only vary in Columns, so they leave
// Limit/Cursor/SortOrder at their zero values.
type QueryShape struct {
	Columns   string
	Limit     int32
	HasLimit  bool
	Cursor    any
	HasCursor bool
	SortOrder data.SortOrder
}

// newOneToManyLoader is a generic helper function that creates a dataloader for one-to-many relationships.
// It abstracts the common pattern of fetching items, grouping them by a key, and returning them in the
// order of the original keys. This reduces boilerplate code in dataloader definitions.
//
// Parameters:
//   - fetcher: A function that fetches all items for a given set of keys. Every call is guaranteed
//     to receive a shape-homogeneous slice (see QueryShape), so it may safely derive query
//     parameters from keys[0] and apply them to the whole slice.
//   - getKey: A function that extracts the grouping key from an item.
//   - shapeOf: Extracts the query shape from a key, used to group the batch before fetching.
//   - name: A static loader name used only as the "loader" metrics label (fixed set, bounded cardinality).
//   - m: Metrics handle for batch-size/fetch-duration instrumentation; may be nil.
//
// Returns:
//   - A configured dataloadgen.Loader for one-to-many relationships.
func newOneToManyLoader[K comparable, PK comparable, V any, T any](
	fetcher func(ctx context.Context, keys []K) ([]T, error),
	getPKFromItem func(item T) PK,
	getPKFromKey func(key K) PK,
	transform func(item T) V,
	shapeOf func(key K) QueryShape,
	name string,
	m *metrics.DataloaderMetrics,
) *dataloadgen.Loader[K, []*V] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []K) ([][]*V, []error) {
			done := observeBatch(m, name, len(keys))
			defer done()

			result := make([][]*V, len(keys))
			errs := make([]error, len(keys))

			for _, indices := range groupIndicesByShape(keys, shapeOf) {
				items, err := fetcher(ctx, subset(keys, indices))
				if err != nil {
					for _, i := range indices {
						errs[i] = err
					}
					continue
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
					pk := getPKFromItem(item)
					transformedItem := transform(item)
					itemsByPK[pk] = append(itemsByPK[pk], &transformedItem)
				}

				for _, i := range indices {
					result[i] = itemsByPK[getPKFromKey(keys[i])]
				}
			}

			return result, errs
		},
		dataloadgen.WithBatchCapacity(loaderBatchCapacity),
		dataloadgen.WithWait(loaderWait),
	)
}

// groupIndicesByShape partitions a batch's key indices by QueryShape, so the fetcher only ever
// sees shape-homogeneous slices. A group whose shape carries a cursor and holds more than one key
// is split into singleton groups instead: the multi-key data-layer methods take no cursor
// parameter, so a >1-key cursored group must be fetched one key at a time to honor each key's
// cursor rather than silently dropping it.
func groupIndicesByShape[K comparable](keys []K, shapeOf func(K) QueryShape) [][]int {
	indicesByShape := make(map[QueryShape][]int)
	var order []QueryShape
	for i, key := range keys {
		shape := shapeOf(key)
		if _, seen := indicesByShape[shape]; !seen {
			order = append(order, shape)
		}
		indicesByShape[shape] = append(indicesByShape[shape], i)
	}

	groups := make([][]int, 0, len(order))
	for _, shape := range order {
		indices := indicesByShape[shape]
		if shape.HasCursor && len(indices) > 1 {
			for _, i := range indices {
				groups = append(groups, []int{i})
			}
			continue
		}
		groups = append(groups, indices)
	}
	return groups
}

// subset returns the keys at the given indices, preserving order.
func subset[K any](keys []K, indices []int) []K {
	out := make([]K, len(indices))
	for j, i := range indices {
		out[j] = keys[i]
	}
	return out
}

// newAccountScopedLoader creates a dataloader for account-scoped one-to-many lookups keyed by
// transaction ToID. A single GraphQL request can alias accountByAddress for multiple accounts, and
// can alias one account with differing field sub-selections, so a single batch may contain keys for
// different accounts and/or different column sets. This groups keys by (account, columns), fetches
// each group once (still batching that group's ToIDs into one query), and maps results back per
// (account, ToID) — so two accounts that share a transaction never surface each other's rows on the
// wrong edge, and a key requesting extra columns never receives another key's narrower selection.
func newAccountScopedLoader[K comparable, V any](
	fetch func(ctx context.Context, accountID string, toIDs []int64, ledgerCreatedAts []time.Time, columns string) ([]*V, error),
	accountID func(key K) string,
	columns func(key K) string,
	toID func(key K) int64,
	ledgerCreatedAt func(key K) time.Time,
	itemToID func(item *V) int64,
	name string,
	m *metrics.DataloaderMetrics,
) *dataloadgen.Loader[K, []*V] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []K) ([][]*V, []error) {
			done := observeBatch(m, name, len(keys))
			defer done()

			result := make([][]*V, len(keys))
			errs := make([]error, len(keys))

			// Group by (account, columns): one request can alias the same account with different field
			// sub-selections, so a batch may hold same-account keys whose column sets differ. Each group
			// fetches its own columns; grouping by account alone would serve them all the first key's columns.
			type scopeGroup struct{ account, columns string }
			indicesByScope := make(map[scopeGroup][]int)
			for i, key := range keys {
				scope := scopeGroup{account: accountID(key), columns: columns(key)}
				indicesByScope[scope] = append(indicesByScope[scope], i)
			}

			for scope, indices := range indicesByScope {
				toIDs := make([]int64, len(indices))
				ledgerCreatedAts := make([]time.Time, len(indices))
				for j, i := range indices {
					toIDs[j] = toID(keys[i])
					ledgerCreatedAts[j] = ledgerCreatedAt(keys[i])
				}

				items, err := fetch(ctx, scope.account, toIDs, ledgerCreatedAts, scope.columns)
				if err != nil {
					for _, i := range indices {
						errs[i] = err
					}
					continue
				}

				itemsByToID := make(map[int64][]*V)
				for _, item := range items {
					itemsByToID[itemToID(item)] = append(itemsByToID[itemToID(item)], item)
				}
				for _, i := range indices {
					result[i] = itemsByToID[toID(keys[i])]
				}
			}

			return result, errs
		},
		dataloadgen.WithBatchCapacity(loaderBatchCapacity),
		dataloadgen.WithWait(loaderWait),
	)
}

// newOneToOneLoader is a generic helper function that creates a dataloader for one-to-one relationships.
// It abstracts the common pattern of fetching a single item for each key and returning them in the
// order of the original keys. This is useful for relationships where each key maps to exactly one item.
//
// Parameters:
//   - fetcher: A function that fetches all items for a given set of keys. Every call is guaranteed
//     to receive a shape-homogeneous slice (see QueryShape), so it may safely derive query
//     parameters from keys[0] and apply them to the whole slice.
//   - getKey: A function that extracts the grouping key from an item.
//   - setKey: A function that associates a fetched item with its corresponding key. This is necessary
//     because the fetcher may not return items in the same order as the input keys.
//   - shapeOf: Extracts the query shape from a key, used to group the batch before fetching.
//   - name: A static loader name used only as the "loader" metrics label (fixed set, bounded cardinality).
//   - m: Metrics handle for batch-size/fetch-duration instrumentation; may be nil.
//
// Returns:
//   - A configured dataloadgen.Loader for one-to-one relationships.
func newOneToOneLoader[K comparable, PK comparable, V any, T any](
	fetcher func(ctx context.Context, keys []K) ([]T, error),
	getPKFromItem func(item T) PK,
	getPKFromKey func(key K) PK,
	transform func(item T) V,
	shapeOf func(key K) QueryShape,
	name string,
	m *metrics.DataloaderMetrics,
) *dataloadgen.Loader[K, *V] {
	return dataloadgen.NewLoader(
		func(ctx context.Context, keys []K) ([]*V, []error) {
			done := observeBatch(m, name, len(keys))
			defer done()

			result := make([]*V, len(keys))
			errs := make([]error, len(keys))

			for _, indices := range groupIndicesByShape(keys, shapeOf) {
				items, err := fetcher(ctx, subset(keys, indices))
				if err != nil {
					for _, i := range indices {
						errs[i] = err
					}
					continue
				}

				itemsByPK := make(map[PK]*V)
				for _, item := range items {
					pk := getPKFromItem(item)
					transformedItem := transform(item)
					itemsByPK[pk] = &transformedItem
				}

				for _, i := range indices {
					result[i] = itemsByPK[getPKFromKey(keys[i])]
				}
			}

			return result, errs
		},
		dataloadgen.WithBatchCapacity(loaderBatchCapacity),
		dataloadgen.WithWait(loaderWait),
	)
}
