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

func TestDetailedTransactionEdgeResolver(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	models := &data.Models{
		Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
	}
	resolver := &detailedTransactionEdgeResolver{&Resolver{models: models}}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	other := keypair.MustRandom().Address()
	const toID int64 = 1 << 40 // high, unique TOID to avoid colliding with package seed data

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id = $1`, toID)
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations_accounts WHERE operation_id IN ($1, $2, $3)`, toID+1, toID+2, toID+3)
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1, $2, $3)`, toID+1, toID+2, toID+3)
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, toID)
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

	edge := &types.DetailedTransactionEdge{
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
