// Unit tests for the SEP-41 AllowanceModel.
// These tests exercise real SQL and require a PostgreSQL test database.
package sep41_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func newAllowancesFixture(t *testing.T) (context.Context, *pgxpool.Pool, *sep41.AllowanceModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &sep41.AllowanceModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

// seedAllowance inserts a single allowance row and its backing contract_tokens row.
func seedAllowance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, owner, spender, contractAddr string, expirationLedger uint32) {
	t.Helper()
	cid := insertContractToken(t, ctx, pool, contractAddr)
	_, err := pool.Exec(ctx, `
		INSERT INTO sep41_allowances (owner_address, spender_address, contract_id, amount, expiration_ledger, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, owner, spender, cid, "100", expirationLedger, uint32(1))
	require.NoError(t, err)
}

// readAllowance fetches the current row for (owner, spender, contract) directly via SQL.
// Used to exercise BatchUpsert end-to-end without depending on the GetByOwner expiration filter.
func readAllowance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, owner, spender string, contractID uuid.UUID) (amount string, expirationLedger uint32, lastModifiedLedger uint32, found bool) {
	t.Helper()
	err := pool.QueryRow(ctx, `
		SELECT amount, expiration_ledger, last_modified_ledger
		FROM sep41_allowances
		WHERE owner_address = $1 AND spender_address = $2 AND contract_id = $3
	`, owner, spender, contractID).Scan(&amount, &expirationLedger, &lastModifiedLedger)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, 0, false
	}
	require.NoError(t, err)
	return amount, expirationLedger, lastModifiedLedger, true
}

func TestAllowanceModel_GetByOwner(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

	t.Run("caps result count at the requested limit and orders ASC by (spender, contract_id)", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		const currentLedger uint32 = 5000

		// Seed 3 distinct contracts × a fresh spender each so (spender, contract_id) is the page key.
		contracts := []string{
			"CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
			"CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z",
			"CBKGXSTBGF7EEX6SYCJTFQ4RZJL3O4WKXBFHIWSL4SDP3UEESH4XJY3A",
		}
		spenders := make([]string, 0, len(contracts))
		for _, c := range contracts {
			sp := keypair.MustRandom().Address()
			spenders = append(spenders, sp)
			seedAllowance(t, ctx, pool, owner, sp, c, currentLedger+100)
		}

		// Unbounded limit would exceed 3 — LIMIT 2 must cap the result at 2.
		page1, err := m.GetByOwner(ctx, owner, currentLedger, 2, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, page1, 2, "limit must cap the result set")

		// ASC ordering by (spender_address, contract_id). Mirror the expected order.
		sortedSpenders := append([]string(nil), spenders...)
		sort.Strings(sortedSpenders)
		assert.Equal(t, sortedSpenders[0], page1[0].SpenderAddress)
		assert.Equal(t, sortedSpenders[1], page1[1].SpenderAddress)
	})

	t.Run("continues a keyset walk from the cursor of the previous page", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		const currentLedger uint32 = 5000

		spenders := make([]string, 0, 4)
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		for i := 0; i < 4; i++ {
			sp := keypair.MustRandom().Address()
			spenders = append(spenders, sp)
			seedAllowance(t, ctx, pool, owner, sp, contract, currentLedger+100)
		}
		sort.Strings(spenders)

		page1, err := m.GetByOwner(ctx, owner, currentLedger, 2, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, page1, 2)
		assert.Equal(t, spenders[0], page1[0].SpenderAddress)
		assert.Equal(t, spenders[1], page1[1].SpenderAddress)

		// Feed the last row back as a cursor; should return rows [2:4].
		cursor := &sep41.AllowanceCursor{
			SpenderAddress: page1[1].SpenderAddress,
			ContractID:     page1[1].ContractID,
		}
		page2, err := m.GetByOwner(ctx, owner, currentLedger, 2, cursor, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		assert.Equal(t, spenders[2], page2[0].SpenderAddress)
		assert.Equal(t, spenders[3], page2[1].SpenderAddress)
	})

	t.Run("returns rows in descending order when SortDESC is given", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		const currentLedger uint32 = 5000

		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		spenders := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			sp := keypair.MustRandom().Address()
			spenders = append(spenders, sp)
			seedAllowance(t, ctx, pool, owner, sp, contract, currentLedger+100)
		}
		sort.Strings(spenders)

		page, err := m.GetByOwner(ctx, owner, currentLedger, 10, nil, sep41.SortDESC)
		require.NoError(t, err)
		require.Len(t, page, 3)
		assert.Equal(t, spenders[2], page[0].SpenderAddress)
		assert.Equal(t, spenders[1], page[1].SpenderAddress)
		assert.Equal(t, spenders[0], page[2].SpenderAddress)
	})

	t.Run("filters out allowances whose expiration_ledger is below currentLedger", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		spender := keypair.MustRandom().Address()
		const currentLedger uint32 = 5000

		activeContract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		expiredContract := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
		seedAllowance(t, ctx, pool, owner, spender, activeContract, currentLedger+1)
		seedAllowance(t, ctx, pool, owner, spender, expiredContract, currentLedger-1)

		page, err := m.GetByOwner(ctx, owner, currentLedger, 10, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, page, 1, "expired allowance should be filtered server-side")
		assert.Equal(t, activeContract, page[0].TokenID)
	})

	t.Run("rejects a non-positive limit with an error", func(t *testing.T) {
		_, err := m.GetByOwner(ctx, keypair.MustRandom().Address(), 0, 0, nil, sep41.SortASC)
		require.Error(t, err)
	})
}

func TestAllowanceModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

	t.Run("is a no-op when both upsert and delete inputs are empty", func(t *testing.T) {
		// Must not Begin a tx or touch the DB when both sides are empty.
		require.NoError(t, m.BatchUpsert(ctx, nil, nil, nil))
	})

	t.Run("inserts new grants, then updates and revokes them on a later ledger", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		spender := keypair.MustRandom().Address()
		contractA := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		contractB := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
		cidA := insertContractToken(t, ctx, pool, contractA)
		cidB := insertContractToken(t, ctx, pool, contractB)

		// First ledger: insert two grants via UNNEST upsert path.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []sep41.Allowance{
				{OwnerAddress: owner, SpenderAddress: spender, ContractID: cidA, Amount: "100", ExpirationLedger: 5100, LedgerNumber: 5000},
				{OwnerAddress: owner, SpenderAddress: spender, ContractID: cidB, Amount: "200", ExpirationLedger: 5200, LedgerNumber: 5000},
			}, nil))
		})

		amount, exp, ledger, found := readAllowance(t, ctx, pool, owner, spender, cidA)
		require.True(t, found)
		assert.Equal(t, "100", amount)
		assert.Equal(t, uint32(5100), exp)
		assert.Equal(t, uint32(5000), ledger)

		// Second ledger: update grant A and revoke (delete) grant B.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx,
				[]sep41.Allowance{{OwnerAddress: owner, SpenderAddress: spender, ContractID: cidA, Amount: "300", ExpirationLedger: 6100, LedgerNumber: 5001}},
				[]sep41.Allowance{{OwnerAddress: owner, SpenderAddress: spender, ContractID: cidB}},
			))
		})

		amount, exp, ledger, found = readAllowance(t, ctx, pool, owner, spender, cidA)
		require.True(t, found, "updated grant should still be present")
		assert.Equal(t, "300", amount)
		assert.Equal(t, uint32(6100), exp)
		assert.Equal(t, uint32(5001), ledger)

		_, _, _, found = readAllowance(t, ctx, pool, owner, spender, cidB)
		assert.False(t, found, "revoked grant should be removed by the UNNEST delete")
	})

	t.Run("handles upsert-only and delete-only paths without touching the other branch", func(t *testing.T) {
		owner := keypair.MustRandom().Address()
		spender := keypair.MustRandom().Address()
		contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cid := insertContractToken(t, ctx, pool, contract)

		// Upsert-only path (deletes nil) must not error or touch the delete branch.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []sep41.Allowance{
				{OwnerAddress: owner, SpenderAddress: spender, ContractID: cid, Amount: "42", ExpirationLedger: 5100, LedgerNumber: 5000},
			}, nil))
		})
		_, _, _, found := readAllowance(t, ctx, pool, owner, spender, cid)
		require.True(t, found)

		// Delete-only path (upserts nil) must not error or touch the insert branch.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, nil, []sep41.Allowance{
				{OwnerAddress: owner, SpenderAddress: spender, ContractID: cid},
			}))
		})
		_, _, _, found = readAllowance(t, ctx, pool, owner, spender, cid)
		assert.False(t, found)
	})
}

func TestAllowanceModel_DeleteExpiredBefore(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

	t.Run("removes strictly-expired rows but preserves boundary rows whose expiration equals currentLedger", func(t *testing.T) {
		ownerExpired := keypair.MustRandom().Address()
		ownerActive := keypair.MustRandom().Address()
		spender := keypair.MustRandom().Address()
		const currentLedger uint32 = 5000

		expiredContract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		activeContract := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
		boundaryContract := "CBKGXSTBGF7EEX6SYCJTFQ4RZJL3O4WKXBFHIWSL4SDP3UEESH4XJY3A"
		seedAllowance(t, ctx, pool, ownerExpired, spender, expiredContract, currentLedger-1)
		seedAllowance(t, ctx, pool, ownerActive, spender, activeContract, currentLedger+1)
		seedAllowance(t, ctx, pool, ownerActive, keypair.MustRandom().Address(), boundaryContract, currentLedger)

		err := db.RunInTransaction(ctx, pool, func(dbTx pgx.Tx) error {
			return m.DeleteExpiredBefore(ctx, dbTx, currentLedger)
		})
		require.NoError(t, err)

		var remaining int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM sep41_allowances`).Scan(&remaining)
		require.NoError(t, err)
		assert.Equal(t, 2, remaining)

		expiredPage, err := m.GetByOwner(ctx, ownerExpired, currentLedger, 10, nil, sep41.SortASC)
		require.NoError(t, err)
		assert.Empty(t, expiredPage)

		activePage, err := m.GetByOwner(ctx, ownerActive, currentLedger, 10, nil, sep41.SortASC)
		require.NoError(t, err)
		require.Len(t, activePage, 2)
	})
}
