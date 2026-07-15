package dataloaders

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func int32Ptr(v int32) *int32 { return &v }

// TestOperationsByToIDLoader_ColumnsPerKeyInSharedBatch covers a single request that aliases the
// `operations` field on the SAME transaction with different sub-selections (e.g. a lean alias
// selecting only `id` and a full alias also selecting `operationType`). Both aliases produce
// OperationColumnsKey values with the same ToID but different Columns, and dataloadgen hands the
// whole batch to the fetch function as one flat slice with no grouping. The fetcher reads
// keys[0].Columns and applies it to the whole batch, so unless the loader groups the batch by
// shape before calling the fetcher, whichever key happens to be first "wins" the columns for
// every other key. The narrow key is listed first so a regression reproduces deterministically.
func TestOperationsByToIDLoader_ColumnsPerKeyInSharedBatch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	const toID int64 = 1 << 48

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = $1`, toID+1) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toID+1, types.XDRBytea([]byte("xdr")))
	require.NoError(t, err)

	limit := int32Ptr(10)
	loaders := NewDataloaders(models)
	results, err := loaders.OperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
		{ToID: toID, Columns: "id", Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		{ToID: toID, Columns: "id, operation_type", Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NotEmpty(t, results[0])
	require.NotEmpty(t, results[1])
	assert.Empty(t, results[0][0].OperationType, "key selecting only id receives no operation_type")
	assert.Equal(t, types.OperationTypePayment, results[1][0].OperationType, "key selecting operation_type must receive it, not the narrow key's columns")
}

// TestOperationsByToIDLoader_MultiKeyBatchHonorsFullLimit covers a batch with two DIFFERENT
// transaction ToIDs (the multi-key branch). One parent has 12 operations; the loader must never
// clamp the multi-key branch's limit to a fixed constant independent of what the caller asked
// for, and it must honor a smaller limit exactly (same as the single-key branch already does).
func TestOperationsByToIDLoader_MultiKeyBatchHonorsFullLimit(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	const (
		toIDA int64 = 1 << 49
		toIDB int64 = 1 << 50
	)

	t.Cleanup(func() {
		ids := make([]int64, 0, 13)
		for i := int64(1); i <= 12; i++ {
			ids = append(ids, toIDA+i)
		}
		ids = append(ids, toIDB+1)
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = ANY($1)`, ids) //nolint:errcheck
	})

	// Parent A has 12 operations; parent B has 1. Both share the same shape (no columns, same
	// limit, same sort order) so they land in the same batch group, forcing the multi-key branch.
	for i := int64(1); i <= 12; i++ {
		_, err := testDBConnectionPool.Exec(ctx, `
			INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
			VALUES ($1, 'PAYMENT', $2, 'op_success', true, 1, $3)
		`, toIDA+i, types.XDRBytea([]byte("xdr")), now)
		require.NoError(t, err)
	}
	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($1, 'PAYMENT', $2, 'op_success', true, 1, $3)
	`, toIDB+1, types.XDRBytea([]byte("xdr")), now)
	require.NoError(t, err)

	t.Run("limit above available rows returns everything, not a fixed cap", func(t *testing.T) {
		limit := int32Ptr(51)
		loaders := NewDataloaders(models)
		results, err := loaders.OperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
			{ToID: toIDA, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
			{ToID: toIDB, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Len(t, results[0], 12, "parent A's 12 operations must all come back uncapped")
		assert.Len(t, results[1], 1)
	})

	t.Run("limit below available rows is honored exactly", func(t *testing.T) {
		limit := int32Ptr(6)
		loaders := NewDataloaders(models)
		results, err := loaders.OperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
			{ToID: toIDA, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
			{ToID: toIDB, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Len(t, results[0], 6)
	})
}

// TestOperationsByToIDLoader_DuplicateCursorInSharedBatchIsHonoredPerKey covers two keys for
// DIFFERENT transactions that happen to carry the exact same Cursor value (e.g. two aliased
// `operations(after: $cursor)` fields sharing a client-supplied cursor). The multi-key data-layer
// method (BatchGetByToIDs) has no cursor parameter at all, so merging these into one multi-key
// fetch would silently drop the cursor for both. The loader must recognize that the group has
// more than one key sharing a cursor and fall back to fetching each key individually so cursor
// filtering still applies.
func TestOperationsByToIDLoader_DuplicateCursorInSharedBatchIsHonoredPerKey(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	const (
		toIDA int64 = 1 << 51
		toIDB int64 = 1 << 52
	)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1, $2, $3)`, toIDA+1, toIDA+2, toIDB+1) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1),
		       ($4, 'PAYMENT', $3, 'op_success', true, 1, $1),
		       ($5, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toIDA+1, types.XDRBytea([]byte("xdr")), toIDA+2, toIDB+1)
	require.NoError(t, err)

	limit := int32Ptr(10)
	cursor := toIDA + 1
	results, err := NewDataloaders(models).OperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
		{ToID: toIDA, Limit: limit, Cursor: &cursor, SortOrder: data.ASC, LedgerCreatedAt: now},
		{ToID: toIDB, Limit: limit, Cursor: &cursor, SortOrder: data.ASC, LedgerCreatedAt: now},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Parent A: cursor excludes id <= toIDA+1, so only toIDA+2 remains.
	require.Len(t, results[0], 1)
	assert.Equal(t, toIDA+2, results[0][0].Operation.ID)

	// Parent B: unaffected by A's cursor value, but if the cursor were dropped for the whole
	// group (bug), an unfiltered multi-key query could still change what comes back here too.
	require.Len(t, results[1], 1)
	assert.Equal(t, toIDB+1, results[1][0].Operation.ID)
}

// TestOperationsByToIDLoader_NilLimitErrors covers the defensive guard: a key with no limit must
// fail closed with an error, never dereference a nil pointer (panic) or send an unbounded query.
func TestOperationsByToIDLoader_NilLimitErrors(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	loaders := NewDataloaders(models)
	_, err := loaders.OperationsByToIDLoader.Load(ctx, OperationColumnsKey{ToID: 1 << 53, SortOrder: data.ASC})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a positive limit")
}

// TestTransactionsByOperationIDLoader_ColumnsPerKeyInSharedBatch covers the one-to-one loader
// equivalent of the columns bug: a single request aliasing Operation.transaction twice with
// different sub-selections. Both keys share the same OperationID but request different Columns;
// the fetcher must not apply the first key's columns to the second.
func TestTransactionsByOperationIDLoader_ColumnsPerKeyInSharedBatch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Transactions: &data.TransactionModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	const txToID int64 = 1 << 54
	const opID int64 = txToID + 1

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = $1`, opID)        //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, txToID) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 4200, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000ee"), txToID)
	require.NoError(t, err)
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, opID, types.XDRBytea([]byte("xdr")))
	require.NoError(t, err)

	loaders := NewDataloaders(models)
	results, err := loaders.TransactionsByOperationIDLoader.LoadAll(ctx, []TransactionColumnsKey{
		{OperationID: opID, Columns: "hash"},
		{OperationID: opID, Columns: "hash, fee_charged"},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NotNil(t, results[0])
	require.NotNil(t, results[1])
	assert.Zero(t, results[0].FeeCharged, "key selecting only hash receives no fee_charged")
	assert.EqualValues(t, 4200, results[1].FeeCharged, "key selecting fee_charged must receive it, not the first key's narrower columns")
}
