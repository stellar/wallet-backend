package resolvers

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
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestAccountTransactionEdgeResolver(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}
	resolver := &accountTransactionEdgeResolver{&Resolver{models: models}}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	other := keypair.MustRandom().Address()
	const toID int64 = 1 << 40 // high, unique TOID to avoid colliding with package seed data

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id = $1`, toID)                                           //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id IN ($1, $2, $3)`, toID+1, toID+2, toID+3) //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1, $2, $3)`, toID+1, toID+2, toID+3)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, toID)                                            //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000aa"), toID)
	require.NoError(t, err)

	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($2, 'PAYMENT', $3, 'op_success', true, 1, $1), ($4, 'PAYMENT', $3, 'op_success', true, 1, $1), ($5, 'PAYMENT', $3, 'op_success', true, 1, $1)
	`, now, toID+1, types.XDRBytea([]byte("xdr")), toID+2, toID+3)
	require.NoError(t, err)

	// toID+1 and toID+2 belong to acct; toID+3 belongs to other and must be excluded
	// from acct-scoped results to prove account scoping through operations_accounts.
	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		VALUES ($2, $3, $1), ($2, $4, $1), ($2, $5, $6)
	`, types.AddressBytea(acct), now, toID+1, toID+2, toID+3, types.AddressBytea(other))
	require.NoError(t, err)

	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($2, 1, 'BALANCE', 'CREDIT', $1, 1, $3, $4), ($2, 2, 'BALANCE', 'CREDIT', $1, 1, $5, $4)
	`, now, toID, types.AddressBytea(acct), toID+1, types.AddressBytea(other))
	require.NoError(t, err)

	edge := &types.AccountTransactionEdge{
		Node:           &types.Transaction{ToID: toID, LedgerCreatedAt: now},
		AccountAddress: types.AddressBytea(acct),
	}

	t.Run("operations are scoped to the account", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(models)
		c := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		ops, err := resolver.Operations(c, edge)
		require.NoError(t, err)
		require.Len(t, ops, 2) // only acct's ops; toID+3 (owned by other) is excluded
		// ORDER BY o.ledger_created_at DESC, o.id DESC — both ops share the same ledger_created_at,
		// so DESC by id yields toID+2 first, toID+1 second.
		assert.Equal(t, toID+2, ops[0].ID)
		assert.Equal(t, toID+1, ops[1].ID)
	})

	t.Run("state changes are scoped to the account", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(models)
		c := context.WithValue(getTestCtx("stateChanges", []string{"toId"}), middleware.LoadersKey, loaders)
		scs, err := resolver.StateChanges(c, edge)
		require.NoError(t, err)
		require.Len(t, scs, 1) // only acct's state change, not other's
	})
}

// TestAccountTransactionEdge_NestedOperations_NoLedgerCreatedAtSelected drives the full path:
// Account.transactions builds the edge from the DB, then the edge's Operations resolver loads
// account-scoped operations. The node selection deliberately omits ledgerCreatedAt. The edge
// resolver pins the partition column from the node's LedgerCreatedAt, so that value must be the
// real inserted time even when unrequested; otherwise chunk exclusion prunes the nested operations
// and they return silently empty.
func TestAccountTransactionEdge_NestedOperations_NoLedgerCreatedAtSelected(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Transactions: &data.TransactionModel{DB: testDBConnectionPool, Metrics: m.DB},
		Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
	}
	acctResolver := &accountResolver{&Resolver{models: models}}
	edgeResolver := &accountTransactionEdgeResolver{&Resolver{models: models}}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	const toID int64 = 1 << 41 // distinct from the other edge test's 1<<40 range

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id = $1`, toID+1) //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = $1`, toID+1)                    //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions_accounts WHERE tx_to_id = $1`, toID)     //nolint:errcheck
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, toID)                 //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, $3, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000bb"), toID)
	require.NoError(t, err)

	_, err = testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id)
		VALUES ($1, $2, $3)
	`, now, toID, types.AddressBytea(acct))
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

	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	// Node selection omits ledgerCreatedAt; the partition pin must still come from the DB row.
	txCtx := getTestCtx("transactions", []string{"hash"})
	conn, err := acctResolver.Transactions(txCtx, parentAccount, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 1)
	edge := conn.Edges[0]
	require.False(t, edge.Node.LedgerCreatedAt.IsZero(), "node must carry the real ledger_created_at for partition pinning")

	loaders := dataloaders.NewDataloaders(models)
	opCtx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
	ops, err := edgeResolver.Operations(opCtx, edge)
	require.NoError(t, err)
	require.Len(t, ops, 1) // nested operations resolve only when the node's partition pin is correct
	assert.Equal(t, toID+1, ops[0].ID)
}

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
	const toID int64 = 1 << 42 // distinct from the other edge tests' ranges

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
		loaders := dataloaders.NewDataloaders(models)
		results, err := loaders.AccountOperationsByToIDLoader.LoadAll(ctx, []dataloaders.OperationColumnsKey{
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
		loaders := dataloaders.NewDataloaders(models)
		results, err := loaders.AccountStateChangesByToIDLoader.LoadAll(ctx, []dataloaders.StateChangeColumnsKey{
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
