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

// TestAccountScopedLoaders_NoCrossAccountLeakInSharedBatch covers the case where a single request
// aliases accountByAddress for two different accounts that both took part in the SAME transaction.
// Dataloaders are per-request, so both accounts' edge loads land in one batch. The loader must scope
// each key to its own account; if it scoped the whole batch to one account (or grouped only by
// to_id), the two accounts would collide on the shared transaction and leak each other's rows.
// LoadAll forces both keys into a single batch.
func TestAccountScopedLoaders_NoCrossAccountLeakInSharedBatch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acctA := keypair.MustRandom().Address()
	acctB := keypair.MustRandom().Address()
	const toID int64 = 1 << 42 // distinct from the per-to_id routing test's ranges

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id = $1`, toID)                               //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id IN ($1, $2)`, toID+1, toID+2) //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1, $2)`, toID+1, toID+2)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, toID)                                //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000cc"), toID)
	require.NoError(t, err)

	// One operation per account, both inside transaction toID: toID+1 -> acctA, toID+2 -> acctB.
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1), ($4, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toID+1, types.XDRBytea([]byte("xdr")), toID+2)
	require.NoError(t, err)
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		VALUES ($1, $2, $3), ($1, $4, $5)
	`, now, toID+1, types.AddressBytea(acctA), toID+2, types.AddressBytea(acctB))
	require.NoError(t, err)

	// One state change per account in the same transaction: sc 1 -> acctA, sc 2 -> acctB.
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($2, 1, 'BALANCE', 'CREDIT', $1, 1, $3, $4), ($2, 2, 'BALANCE', 'CREDIT', $1, 1, $5, $6)
	`, now, toID, types.AddressBytea(acctA), toID+1, types.AddressBytea(acctB), toID+2)
	require.NoError(t, err)

	t.Run("operations loader does not leak across accounts in one batch", func(t *testing.T) {
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.AccountOperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
			{AccountID: acctA, ToID: toID, LedgerCreatedAt: now},
			{AccountID: acctB, ToID: toID, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Len(t, results[0], 1)
		assert.Equal(t, toID+1, results[0][0].ID, "acctA edge sees only acctA's operation")
		require.Len(t, results[1], 1)
		assert.Equal(t, toID+2, results[1][0].ID, "acctB edge sees only acctB's operation, not acctA's")
	})

	t.Run("state-changes loader does not leak across accounts in one batch", func(t *testing.T) {
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.AccountStateChangesByToIDLoader.LoadAll(ctx, []StateChangeColumnsKey{
			{AccountID: acctA, ToID: toID, LedgerCreatedAt: now},
			{AccountID: acctB, ToID: toID, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Len(t, results[0], 1)
		assert.Equal(t, acctA, results[0][0].AccountID.String(), "acctA edge sees only acctA's state change")
		require.Len(t, results[1], 1)
		assert.Equal(t, acctB, results[1][0].AccountID.String(), "acctB edge sees only acctB's state change, not acctA's")
	})
}

// TestAccountScopedLoaders_PerColumnsFetchInSharedBatch covers a single request that aliases the
// SAME account with different field sub-selections. Each alias produces a key with the same
// (account, to_id) but a different Columns set, and both land in one batch. The loader must fetch
// each column set on its own; if it grouped by account alone and reused the first key's columns, the
// alias asking for more columns would silently receive zero-valued fields. The lean-columns key is
// listed first so a regressed "reuse first key's columns" fetch would serve "id" to both.
func TestAccountScopedLoaders_PerColumnsFetchInSharedBatch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	const toID int64 = 1 << 45 // distinct from the other tests' ranges

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id = $1`, toID+1) //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = $1`, toID+1)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, toID)                 //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000dd"), toID)
	require.NoError(t, err)

	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toID+1, types.XDRBytea([]byte("xdr")))
	require.NoError(t, err)
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		VALUES ($1, $2, $3)
	`, now, toID+1, types.AddressBytea(acct))
	require.NoError(t, err)

	loaders := NewDataloaders(models, m.Dataloader)
	results, err := loaders.AccountOperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
		{AccountID: acct, ToID: toID, LedgerCreatedAt: now, Columns: "id"},
		{AccountID: acct, ToID: toID, LedgerCreatedAt: now, Columns: "id, operation_type"},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Len(t, results[0], 1)
	require.Len(t, results[1], 1)
	assert.Empty(t, results[0][0].OperationType, "key selecting only id receives no operation_type")
	assert.NotEmpty(t, results[1][0].OperationType, "key selecting operation_type must receive it, not the first key's narrower columns")
}

// TestAccountScopedLoaders_BatchRoutesPerToID covers a single account with multiple transactions in
// one batch — the loader's reason to exist. It groups the batch by (account, columns), fetches that account's
// rows once, then maps each row back to its originating to_id (itemsByToID[toID(key)]). With three
// transactions in one batch — toIDA and toIDB each holding distinct rows, toIDC holding none — this
// proves: (1) per-to_id routing with no A/B swap, (2) the operations ID &^ 0xFFF derivation across
// distinct to_ids, and (3) the empty/nil branch for a key whose to_id returns nothing.
func TestAccountScopedLoaders_BatchRoutesPerToID(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	const (
		toIDA int64 = 1 << 43
		toIDB int64 = 1 << 44
		toIDC int64 = 1 << 45 // account is queried for this tx but has no ops/state changes in it
	)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN ($1, $2, $3)`, toIDA, toIDB, toIDC)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id IN ($1, $2, $3)`, toIDA+1, toIDA+2, toIDB+1) //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1, $2, $3)`, toIDA+1, toIDA+2, toIDB+1)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id IN ($1, $2, $3)`, toIDA, toIDB, toIDC)                     //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
		       ($4, $5, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
		       ($6, $7, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now,
		types.HashBytea("00000000000000000000000000000000000000000000000000000000000000d1"), toIDA,
		types.HashBytea("00000000000000000000000000000000000000000000000000000000000000d2"), toIDB,
		types.HashBytea("00000000000000000000000000000000000000000000000000000000000000d3"), toIDC)
	require.NoError(t, err)

	// Operations: two under toIDA, one under toIDB, none under toIDC.
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1),
		       ($4, 'PAYMENT', $3, 'op_success', true, 1, $1),
		       ($5, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toIDA+1, types.XDRBytea([]byte("xdr")), toIDA+2, toIDB+1)
	require.NoError(t, err)
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		VALUES ($1, $2, $3), ($1, $4, $3), ($1, $5, $3)
	`, now, toIDA+1, types.AddressBytea(acct), toIDA+2, toIDB+1)
	require.NoError(t, err)

	// State changes: two under toIDA, one under toIDB, none under toIDC.
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($2, 1, 'BALANCE', 'CREDIT', $1, 1, $3, $4),
		       ($2, 2, 'BALANCE', 'CREDIT', $1, 1, $3, $4),
		       ($5, 1, 'BALANCE', 'CREDIT', $1, 1, $3, $6)
	`, now, toIDA, types.AddressBytea(acct), toIDA+1, toIDB, toIDB+1)
	require.NoError(t, err)

	t.Run("operations loader routes per to_id", func(t *testing.T) {
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.AccountOperationsByToIDLoader.LoadAll(ctx, []OperationColumnsKey{
			{AccountID: acct, ToID: toIDA, LedgerCreatedAt: now},
			{AccountID: acct, ToID: toIDB, LedgerCreatedAt: now},
			{AccountID: acct, ToID: toIDC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 3)

		// toIDA: two ops, ORDER BY id DESC -> toIDA+2 then toIDA+1.
		require.Len(t, results[0], 2)
		assert.Equal(t, toIDA+2, results[0][0].ID)
		assert.Equal(t, toIDA+1, results[0][1].ID)

		// toIDB: one op, and it must NOT be one of toIDA's ops (proves no cross-to_id swap).
		require.Len(t, results[1], 1)
		assert.Equal(t, toIDB+1, results[1][0].ID)

		// toIDC: account queried but has no ops here -> empty.
		assert.Empty(t, results[2])
	})

	t.Run("state-changes loader routes per to_id", func(t *testing.T) {
		loaders := NewDataloaders(models, m.Dataloader)
		results, err := loaders.AccountStateChangesByToIDLoader.LoadAll(ctx, []StateChangeColumnsKey{
			{AccountID: acct, ToID: toIDA, LedgerCreatedAt: now},
			{AccountID: acct, ToID: toIDB, LedgerCreatedAt: now},
			{AccountID: acct, ToID: toIDC, LedgerCreatedAt: now},
		})
		require.NoError(t, err)
		require.Len(t, results, 3)

		require.Len(t, results[0], 2)
		for _, sc := range results[0] {
			assert.Equal(t, toIDA, sc.ToID)
		}
		require.Len(t, results[1], 1)
		assert.Equal(t, toIDB, results[1][0].ToID)
		assert.Empty(t, results[2])
	})
}
