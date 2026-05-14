// Unit tests for the SEP-41 BalanceModel.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package sep41_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func newBalancesFixture(t *testing.T) (context.Context, *pgxpool.Pool, *sep41.BalanceModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &sep41.BalanceModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

// insertContractToken seeds contract_tokens so FK-deferred sep41_balances inserts pass commit-time validation.
func insertContractToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, contractAddr string) uuid.UUID {
	t.Helper()
	id := data.DeterministicContractID(contractAddr)
	_, err := pool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, id, contractAddr, "sep41", 7)
	require.NoError(t, err)
	return id
}

func runInTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	require.NoError(t, tx.Commit(ctx))
}

func TestBalanceModel_BatchApplyDeltas_InsertsFreshRow(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		err := m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
			AccountAddress: acct,
			ContractID:     cid,
			Balance:        "1000",
			LedgerNumber:   42,
		}})
		require.NoError(t, err)
	})

	balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	require.Len(t, balances, 1)
	assert.Equal(t, "1000", balances[0].Balance)
	assert.Equal(t, uint32(42), balances[0].LedgerNumber)
}

func TestBalanceModel_BatchApplyDeltas_SumsWithExistingRow(t *testing.T) {
	// This is the regression test for the restart-overwrite bug: a second application of a
	// delta on a (account, contract) that already has a balance must sum, not overwrite.
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	// Pre-populate balance = 1000.
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "1000", LedgerNumber: 42,
		}}))
	})

	// Apply -250 and +50 in a second ledger. Should sum to 800.
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{
			{AccountAddress: acct, ContractID: cid, Balance: "-250", LedgerNumber: 43},
			{AccountAddress: acct, ContractID: cid, Balance: "50", LedgerNumber: 43},
		}))
	})

	balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	require.Len(t, balances, 1)
	assert.Equal(t, "800", balances[0].Balance)
	assert.Equal(t, uint32(43), balances[0].LedgerNumber)
}

func TestBalanceModel_BatchApplyDeltas_DeletesWhenBalanceSettlesToZero(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "500", LedgerNumber: 10,
		}}))
	})
	// Burn the entire balance.
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "-500", LedgerNumber: 11,
		}}))
	})

	balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	assert.Empty(t, balances, "zero-balance row should be swept")
}

func TestBalanceModel_BatchApplyDeltas_EmptyInputNoOp(t *testing.T) {
	ctx, _, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	// Must not fail when no deltas are staged.
	require.NoError(t, m.BatchApplyDeltas(ctx, nil, nil))
}
