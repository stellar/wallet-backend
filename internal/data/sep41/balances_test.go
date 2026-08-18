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

	t.Run("skips deltas at or below the row's last_modified_ledger", func(t *testing.T) {
		// The strict-monotone guard: a delta whose ledger is <= the row's stamp is
		// already included in the row's value (repair writes stamp the ledger their
		// simulated value is true at) and must not be re-applied.
		acct := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "1000", LedgerNumber: 100,
			}}))
		})

		// Same ledger (replay) and older ledger: both skipped.
		for _, stale := range []uint32{100, 99} {
			runInTx(t, ctx, pool, func(tx pgx.Tx) {
				require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
					AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "-400", LedgerNumber: stale,
				}}))
			})
		}

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "1000", balances[0].Balance)
		assert.Equal(t, uint32(100), balances[0].LedgerNumber)

		// A genuinely newer delta still applies on top.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "-400", LedgerNumber: 101,
			}}))
		})
		balances, err = m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "600", balances[0].Balance)
		assert.Equal(t, uint32(101), balances[0].LedgerNumber)
	})

	t.Run("does not sweep a zero row whose stamp is newer than the skipped delta", func(t *testing.T) {
		// A zero row at a newer ledger is a repair barrier: its value ("holds zero as
		// of ledger R") must survive stale deltas, or a later stale delta would INSERT
		// a wrong row after the sweep removed it. Only the repair engine deletes such
		// rows, once live ingestion has passed R.
		acct := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		rawAcct, err := types.AddressBytea(acct).Value()
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `
			INSERT INTO sep41_balances (account_id, contract_id, balance, last_modified_ledger)
			VALUES ($1, $2, 0, 200)
		`, rawAcct, cid)
		require.NoError(t, err)

		// Stale delta at ledger 150: guard skips it, and the sweep must not delete
		// the untouched zero row (its stamp 200 differs from the batch's ledger 150).
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "-400", LedgerNumber: 150,
			}}))
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1, "zero barrier row must survive stale deltas")
		assert.Equal(t, "0", balances[0].Balance)
		assert.Equal(t, uint32(200), balances[0].LedgerNumber)

		// A newer delta folds on the barrier and moves the stamp.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "70", LedgerNumber: 201,
			}}))
		})
		balances, err = m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "70", balances[0].Balance)
		assert.Equal(t, uint32(201), balances[0].LedgerNumber)
	})

	t.Run("is a no-op when no deltas are staged", func(t *testing.T) {
		// Must not fail when no deltas are staged.
		require.NoError(t, m.BatchApplyDeltas(ctx, nil, nil))
	})
}

func TestBalanceModel_ApplyAbsolute(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	t.Run("inserts a fresh row and reports applied", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			applied, err := m.ApplyAbsolute(ctx, tx, sep41.Balance{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "12345", LedgerNumber: 500,
			})
			require.NoError(t, err)
			assert.True(t, applied)
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "12345", balances[0].Balance)
		assert.Equal(t, uint32(500), balances[0].LedgerNumber)
	})

	t.Run("replaces a stale fold value and is idempotent at the same ledger", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "999", LedgerNumber: 400,
			}}))
		})

		for range 2 { // second run exercises the equal-ledger idempotent path
			runInTx(t, ctx, pool, func(tx pgx.Tx) {
				applied, err := m.ApplyAbsolute(ctx, tx, sep41.Balance{
					AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "1500", LedgerNumber: 450,
				})
				require.NoError(t, err)
				assert.True(t, applied)
			})
		}

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "1500", balances[0].Balance)
		assert.Equal(t, uint32(450), balances[0].LedgerNumber)
	})

	t.Run("no-ops when the row has moved past the observation ledger", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "999", LedgerNumber: 600,
			}}))
		})

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			applied, err := m.ApplyAbsolute(ctx, tx, sep41.Balance{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "1", LedgerNumber: 599,
			})
			require.NoError(t, err)
			assert.False(t, applied, "row written at 600 must reject truth observed at 599")
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, "999", balances[0].Balance)
		assert.Equal(t, uint32(600), balances[0].LedgerNumber)
	})

	t.Run("stores zero as a barrier row instead of deleting", func(t *testing.T) {
		acct := keypair.MustRandom().Address()
		cid := insertContractToken(t, ctx, pool, contract)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "42", LedgerNumber: 700,
			}}))
		})

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			applied, err := m.ApplyAbsolute(ctx, tx, sep41.Balance{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: "0", LedgerNumber: 750,
			})
			require.NoError(t, err)
			assert.True(t, applied)
		})

		balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, balances, 1, "zero must remain as a barrier row until DeleteZeroRows")
		assert.Equal(t, "0", balances[0].Balance)
		assert.Equal(t, uint32(750), balances[0].LedgerNumber)
	})
}

func TestBalanceModel_ListPairs(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	contract1 := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	contract2 := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
	cid1 := insertContractToken(t, ctx, pool, contract1)
	cid2 := insertContractToken(t, ctx, pool, contract2)

	acctA := keypair.MustRandom().Address()
	acctB := keypair.MustRandom().Address()
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchApplyDeltas(ctx, tx, []sep41.Balance{
			{AccountID: types.AddressBytea(acctA), ContractID: cid1, Balance: "1", LedgerNumber: 10},
			{AccountID: types.AddressBytea(acctA), ContractID: cid2, Balance: "2", LedgerNumber: 10},
			{AccountID: types.AddressBytea(acctB), ContractID: cid1, Balance: "3", LedgerNumber: 10},
		}))
	})

	t.Run("pages the full table in keyset order with the token address joined", func(t *testing.T) {
		var all []sep41.Balance
		var after *sep41.Balance
		for {
			page, err := m.ListPairs(ctx, nil, "", after, 2)
			require.NoError(t, err)
			if len(page) == 0 {
				break
			}
			all = append(all, page...)
			after = &page[len(page)-1]
		}
		require.Len(t, all, 3)
		for _, p := range all {
			assert.NotEmpty(t, p.TokenID)
			assert.Contains(t, []uuid.UUID{cid1, cid2}, p.ContractID)
		}
	})

	t.Run("filters by contract and by account", func(t *testing.T) {
		byContract, err := m.ListPairs(ctx, &cid2, "", nil, 10)
		require.NoError(t, err)
		require.Len(t, byContract, 1)
		assert.Equal(t, contract2, byContract[0].TokenID)
		assert.Equal(t, acctA, string(byContract[0].AccountID))

		byAccount, err := m.ListPairs(ctx, nil, acctB, nil, 10)
		require.NoError(t, err)
		require.Len(t, byAccount, 1)
		assert.Equal(t, contract1, byAccount[0].TokenID)
	})
}

func TestBalanceModel_DeleteZeroRows(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	acctSwept := keypair.MustRandom().Address()
	acctAhead := keypair.MustRandom().Address()
	acctNonZero := keypair.MustRandom().Address()

	seed := func(acct, balance string, ledger uint32) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			applied, err := m.ApplyAbsolute(ctx, tx, sep41.Balance{
				AccountID: types.AddressBytea(acct), ContractID: cid, Balance: balance, LedgerNumber: ledger,
			})
			require.NoError(t, err)
			require.True(t, applied)
		})
	}
	seed(acctSwept, "0", 100)   // barrier at/below the cursor: swept
	seed(acctAhead, "0", 300)   // barrier above the cursor: kept
	seed(acctNonZero, "7", 100) // non-zero: kept

	pairs := []sep41.Balance{
		{AccountID: types.AddressBytea(acctSwept), ContractID: cid},
		{AccountID: types.AddressBytea(acctAhead), ContractID: cid},
		{AccountID: types.AddressBytea(acctNonZero), ContractID: cid},
	}
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		deleted, err := m.DeleteZeroRows(ctx, tx, pairs, 200)
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
	})

	swept, err := m.GetByAccount(ctx, acctSwept, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	assert.Empty(t, swept)
	ahead, err := m.GetByAccount(ctx, acctAhead, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	assert.Len(t, ahead, 1)
	nonZero, err := m.GetByAccount(ctx, acctNonZero, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	assert.Len(t, nonZero, 1)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		deleted, err := m.DeleteZeroRows(ctx, tx, nil, 200)
		require.NoError(t, err)
		assert.Zero(t, deleted)
	})
}
