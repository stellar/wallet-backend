// Unit tests for the SEP-41 AllowanceModel.
// These tests exercise real SQL and require a PostgreSQL test database.
package sep41_test

import (
	"context"
	"sort"
	"testing"

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

func TestAllowanceModel_GetByOwner_LimitsAndOrders(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

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
}

func TestAllowanceModel_GetByOwner_KeysetCursorContinuesWalk(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

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
}

func TestAllowanceModel_GetByOwner_DescOrdersInReverse(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

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
}

func TestAllowanceModel_GetByOwner_ExpiredRowsFiltered(t *testing.T) {
	ctx, pool, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

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
}

func TestAllowanceModel_GetByOwner_RejectsNonPositiveLimit(t *testing.T) {
	ctx, _, m, cleanup := newAllowancesFixture(t)
	defer cleanup()

	_, err := m.GetByOwner(ctx, keypair.MustRandom().Address(), 0, 0, nil, sep41.SortASC)
	require.Error(t, err)
}
