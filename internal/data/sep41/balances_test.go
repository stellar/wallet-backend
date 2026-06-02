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
	"github.com/stellar/wallet-backend/internal/indexer/types"
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

func TestBalanceModel_BatchApplyDeltas(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	t.Run("inserts a fresh row when no balance exists for (account, contract)", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			err := m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID:    types.AddressBytea(acct),
				ContractID:   cid,
				Balance:      "1000",
				LedgerNumber: 42,
			}})
			require.NoError(t, err)
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "1000", balances[0].Balance)
		assert.Equal(t, uint32(42), balances[0].LedgerNumber)
	})

	t.Run("sums deltas with the existing balance rather than overwriting", func(t *testing.T) {
		// Regression test for the restart-overwrite bug: a subsequent application of a
		// delta on a (account, contract) that already has a balance must sum, not overwrite.
		// Each call supplies a single delta — the upstream processor dedupes per ledger,
		// so the data layer never sees the same (account, contract) twice per call.
		acct := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		// Ledger 42: balance = 1000.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "1000", LedgerNumber: 42,
			}}))
		})

		// Ledger 43: -250 → 750.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "-250", LedgerNumber: 43,
			}}))
		})

		// Ledger 44: +50 → 800.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "50", LedgerNumber: 44,
			}}))
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "800", balances[0].Balance)
		assert.Equal(t, uint32(44), balances[0].LedgerNumber)
	})

	t.Run("applies multiple distinct (account, contract) rows in a single UNNEST upsert", func(t *testing.T) {
		// Exercises the multi-row UNNEST path: distinct keys must all land in one call.
		acctA := keypair.MustRandom().Address()
		acctB := keypair.MustRandom().Address()
		contract1 := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		contract2 := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
		cid1 := insertContractToken(t, ctx, pool, contract1)
		cid2 := insertContractToken(t, ctx, pool, contract2)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{
				{AccountID: types.AddressBytea(acctA), ContractID: cid1, Balance: "100", LedgerNumber: 50},
				{AccountID: types.AddressBytea(acctA), ContractID: cid2, Balance: "200", LedgerNumber: 50},
				{AccountID: types.AddressBytea(acctB), ContractID: cid1, Balance: "300", LedgerNumber: 50},
			}))
		})

		balancesA, err := m.GetByAccount(ctx, acctA, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balancesA, 2)
		gotA := map[uuid.UUID]string{}
		for _, b := range balancesA {
			gotA[b.ContractID] = b.Balance
			assert.Equal(t, uint32(50), b.LedgerNumber)
		}
		assert.Equal(t, "100", gotA[cid1])
		assert.Equal(t, "200", gotA[cid2])

		balancesB, err := m.GetByAccount(ctx, acctB, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balancesB, 1)
		assert.Equal(t, "300", balancesB[0].Balance)
		assert.Equal(t, uint32(50), balancesB[0].LedgerNumber)
	})

	t.Run("deletes the row when a delta settles the balance to zero", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "500", LedgerNumber: 10,
			}}))
		})
		// Burn the entire balance.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "-500", LedgerNumber: 11,
			}}))
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		assert.Empty(t, balances, "zero-balance row should be swept")
	})

	t.Run("is a no-op when no deltas are staged", func(t *testing.T) {
		// Must not fail when no deltas are staged.
		require.NoError(t, m.BatchApplyDeltas(ctx, nil, nil))
	})
}
