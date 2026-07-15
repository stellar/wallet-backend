package dataloaders

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// TestStateChangesByToIDLoader_ColumnsPerKeyInSharedBatch is the state-changes equivalent of
// TestOperationsByToIDLoader_ColumnsPerKeyInSharedBatch: two aliases of the same transaction's
// `stateChanges` field with different sub-selections must each receive their own columns, not
// whichever key happens to be first in the batch.
func TestStateChangesByToIDLoader_ColumnsPerKeyInSharedBatch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	const toID int64 = 1 << 55

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id = $1`, toID) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($1, 1, 'BALANCE', 'CREDIT', $2, 1, $3, 0)
	`, toID, now, types.AddressBytea(acct))
	require.NoError(t, err)

	limit := int32Ptr(10)
	loaders := NewDataloaders(models, m.Dataloader)
	results, err := loaders.StateChangesByToIDLoader.LoadAll(ctx, []StateChangeColumnsKey{
		{ToID: toID, Columns: "state_change_category", Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		{ToID: toID, Columns: "state_change_category, state_change_reason", Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NotEmpty(t, results[0])
	require.NotEmpty(t, results[1])
	assert.Empty(t, results[0][0].StateChangeReason, "key selecting only the category receives no reason")
	assert.Equal(t, types.StateChangeReasonCredit, results[1][0].StateChangeReason, "key selecting the reason must receive it, not the first key's narrower columns")
}

// TestStateChangesByToIDLoader_MultiKeyBatchHonorsFullLimit is the state-changes equivalent of
// TestOperationsByToIDLoader_MultiKeyBatchHonorsFullLimit: the multi-key branch must not clamp
// the limit to a fixed constant, and must honor a smaller limit exactly.
func TestStateChangesByToIDLoader_MultiKeyBatchHonorsFullLimit(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	const (
		toIDA int64 = 1 << 56
		toIDB int64 = 1 << 57
	)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN ($1, $2)`, toIDA, toIDB) //nolint:errcheck
	})

	for i := int64(1); i <= 12; i++ {
		_, err := testDBConnectionPool.Exec(ctx, `
			INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
			VALUES ($1, $2, 'BALANCE', 'CREDIT', $3, 1, $4, 0)
		`, toIDA, i, now, types.AddressBytea(acct))
		require.NoError(t, err)
	}
	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($1, 1, 'BALANCE', 'CREDIT', $2, 1, $3, 0)
	`, toIDB, now, types.AddressBytea(acct))
	require.NoError(t, err)

	t.Run("limit above available rows returns everything, not a fixed cap", func(t *testing.T) {
		limit := int32Ptr(51)
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.StateChangesByToIDLoader.LoadAll(ctx, []StateChangeColumnsKey{
			{ToID: toIDA, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
			{ToID: toIDB, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Len(t, results[0], 12, "parent A's 12 state changes must all come back uncapped")
		assert.Len(t, results[1], 1)
	})

	t.Run("limit below available rows is honored exactly", func(t *testing.T) {
		limit := int32Ptr(6)
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.StateChangesByToIDLoader.LoadAll(ctx, []StateChangeColumnsKey{
			{ToID: toIDA, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
			{ToID: toIDB, Limit: limit, SortOrder: data.ASC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Len(t, results[0], 6)
	})
}

// TestStateChangesByToIDLoader_NilLimitErrors mirrors TestOperationsByToIDLoader_NilLimitErrors
// for the state-changes loader: a key with no limit must fail closed with an error.
func TestStateChangesByToIDLoader_NilLimitErrors(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	loaders := NewDataloaders(models, m.Dataloader)
	_, err := loaders.StateChangesByToIDLoader.Load(ctx, StateChangeColumnsKey{ToID: 1 << 58, SortOrder: data.ASC})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a positive limit")
}
