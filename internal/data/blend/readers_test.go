// Unit tests for the Blend v2 read-side accessors (GetByAccount/GetByPools/
// GetByIDs/GetAll) used by the GraphQL resolvers.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// addrBytes decodes a types.AddressBytea to its 33-byte BYTEA representation —
// the same encoding Postgres orders on — so ordering assertions reflect actual
// DB collation instead of guessing base32 strkey string order.
func addrBytes(t *testing.T, a types.AddressBytea) []byte {
	t.Helper()
	v, err := a.Value()
	require.NoError(t, err)
	b, ok := v.([]byte)
	require.True(t, ok)
	return b
}

// assertOrderedByAddrThen asserts items are non-decreasing by the address bytes
// addrOf extracts and, for equal addresses, strictly ascending by secondaryOf.
func assertOrderedByAddrThen[T any](t *testing.T, items []T, addrOf func(T) types.AddressBytea, secondaryOf func(T) int32) {
	t.Helper()
	for i := 1; i < len(items); i++ {
		prev := addrBytes(t, addrOf(items[i-1]))
		cur := addrBytes(t, addrOf(items[i]))
		switch bytes.Compare(prev, cur) {
		case 0:
			assert.Less(t, secondaryOf(items[i-1]), secondaryOf(items[i]), "same address: secondary key must ascend")
		case -1:
			// address strictly ascended, ok
		default:
			t.Fatalf("address bytes must ascend, got %x then %x", prev, cur)
		}
	}
}

// assertOrderedByAddr asserts items are non-decreasing by the address bytes addrOf extracts.
func assertOrderedByAddr[T any](t *testing.T, items []T, addrOf func(T) types.AddressBytea) {
	t.Helper()
	for i := 1; i < len(items); i++ {
		prev := addrBytes(t, addrOf(items[i-1]))
		cur := addrBytes(t, addrOf(items[i]))
		assert.LessOrEqual(t, bytes.Compare(prev, cur), 0, "address bytes must not decrease")
	}
}

func TestPositionModel_GetByAccount(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	insertPosition(t, ctx, pool, poolA, userA, 1, "10", "20", "30", "1", "2")
	insertPosition(t, ctx, pool, poolA, userA, 0, "11", "21", "31", "3", "4")
	insertPosition(t, ctx, pool, poolB, userA, 0, "12", "22", "32", "5", "6")
	insertPosition(t, ctx, pool, poolA, userB, 0, "99", "99", "99", "9", "9")

	got, err := m.GetByAccount(ctx, userA)
	require.NoError(t, err)
	require.Len(t, got, 3, "only userA's rows across both pools")
	for _, p := range got {
		assert.Equal(t, types.AddressBytea(userA), p.UserAccountID)
	}
	assertOrderedByAddrThen(t, got,
		func(p blend.Position) types.AddressBytea { return p.PoolContractID },
		func(p blend.Position) int32 { return p.ReserveIndex },
	)

	t.Run("unknown account returns an empty, non-nil slice", func(t *testing.T) {
		got, err := m.GetByAccount(ctx, keypair.MustRandom().Address())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestBackstopPositionModel_GetByAccount(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPositionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{
			{PoolContractID: types.AddressBytea(poolA), UserAccountID: types.AddressBytea(userA), Shares: "100", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), UserAccountID: types.AddressBytea(userA), Shares: "200", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolA), UserAccountID: types.AddressBytea(userB), Shares: "300", LastModifiedLedger: 1},
		}))
	})

	got, err := m.GetByAccount(ctx, userA)
	require.NoError(t, err)
	require.Len(t, got, 2, "only userA's rows across both pools")
	for _, bp := range got {
		assert.Equal(t, types.AddressBytea(userA), bp.UserAccountID)
	}
	assertOrderedByAddr(t, got, func(bp blend.BackstopPosition) types.AddressBytea { return bp.PoolContractID })

	t.Run("unknown account returns an empty, non-nil slice", func(t *testing.T) {
		got, err := m.GetByAccount(ctx, keypair.MustRandom().Address())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestPoolModel_GetByIDs(t *testing.T) {
	ctx, pool, m, cleanup := newPoolsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address()
	adminA := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{
			{PoolContractID: types.AddressBytea(poolA), Name: strPtr("A"), Admin: types.AddressBytea(adminA), LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), Name: strPtr("B"), LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolC), Name: strPtr("C"), LastModifiedLedger: 1},
		}))
	})
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.SetRewardZone(ctx, tx, []types.AddressBytea{types.AddressBytea(poolA)}, 5))
	})

	got, err := m.GetByIDs(ctx, []string{poolA, poolC, keypair.MustRandom().Address()})
	require.NoError(t, err)
	require.Len(t, got, 2, "subset match; an unknown pool ID is silently excluded")
	assertOrderedByAddr(t, got, func(p blend.Pool) types.AddressBytea { return p.PoolContractID })

	byID := map[types.AddressBytea]blend.Pool{}
	for _, p := range got {
		byID[p.PoolContractID] = p
	}
	pA, ok := byID[types.AddressBytea(poolA)]
	require.True(t, ok)
	assert.Equal(t, types.AddressBytea(adminA), pA.Admin, "admin round-trips through the reader")
	assert.True(t, pA.InRewardZone, "in_reward_zone round-trips through the reader")

	pC, ok := byID[types.AddressBytea(poolC)]
	require.True(t, ok)
	assert.Equal(t, types.AddressBytea(""), pC.Admin, "no admin observed: empty, not garbage")
	assert.False(t, pC.InRewardZone)

	_, ok = byID[types.AddressBytea(poolB)]
	assert.False(t, ok)

	t.Run("empty poolIDs returns an empty slice without querying", func(t *testing.T) {
		m := &blend.PoolModel{}
		got, err := m.GetByIDs(context.Background(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestPoolModel_GetAll(t *testing.T) {
	ctx, pool, m, cleanup := newPoolsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	adminA := keypair.MustRandom().Address()
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{
			{PoolContractID: types.AddressBytea(poolA), Admin: types.AddressBytea(adminA), LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), LastModifiedLedger: 1},
		}))
	})
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.SetRewardZone(ctx, tx, []types.AddressBytea{types.AddressBytea(poolB)}, 5))
	})

	got, err := m.GetAll(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assertOrderedByAddr(t, got, func(p blend.Pool) types.AddressBytea { return p.PoolContractID })

	for _, p := range got {
		switch p.PoolContractID {
		case types.AddressBytea(poolA):
			assert.Equal(t, types.AddressBytea(adminA), p.Admin)
			assert.False(t, p.InRewardZone)
		case types.AddressBytea(poolB):
			assert.Equal(t, types.AddressBytea(""), p.Admin)
			assert.True(t, p.InRewardZone)
		}
	}
}

func TestReserveModel_GetByPools(t *testing.T) {
	ctx, pool, m, cleanup := newReservesFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address() // untouched by the query, must be excluded
	assetA := keypair.MustRandom().Address()
	assetB := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Reserve{
			fullReserve(poolA, assetA, 1, 1),
			fullReserve(poolA, assetB, 0, 1),
			fullReserve(poolB, assetA, 0, 1),
			fullReserve(poolC, assetA, 0, 1),
		}))
	})

	got, err := m.GetByPools(ctx, []string{poolA, poolB, keypair.MustRandom().Address()})
	require.NoError(t, err)
	require.Len(t, got, 3)
	assertOrderedByAddrThen(t, got,
		func(r blend.Reserve) types.AddressBytea { return r.PoolContractID },
		func(r blend.Reserve) int32 { return r.ReserveIndex },
	)
	for _, r := range got {
		assert.NotEqual(t, types.AddressBytea(poolC), r.PoolContractID)
	}

	t.Run("empty poolIDs returns an empty slice without querying", func(t *testing.T) {
		m := &blend.ReserveModel{}
		got, err := m.GetByPools(context.Background(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestReserveEmissionModel_GetByPools(t *testing.T) {
	ctx, pool, m, cleanup := newReserveEmissionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.ReserveEmission{
			{PoolContractID: types.AddressBytea(poolA), ReserveTokenID: 1, Eps: 1, EmissionIndex: "1", Expiration: 1, LastTime: 1, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolA), ReserveTokenID: 0, Eps: 1, EmissionIndex: "1", Expiration: 1, LastTime: 1, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), ReserveTokenID: 0, Eps: 1, EmissionIndex: "1", Expiration: 1, LastTime: 1, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolC), ReserveTokenID: 0, Eps: 1, EmissionIndex: "1", Expiration: 1, LastTime: 1, LastModifiedLedger: 1},
		}))
	})

	got, err := m.GetByPools(ctx, []string{poolA, poolB})
	require.NoError(t, err)
	require.Len(t, got, 3)
	assertOrderedByAddrThen(t, got,
		func(r blend.ReserveEmission) types.AddressBytea { return r.PoolContractID },
		func(r blend.ReserveEmission) int32 { return r.ReserveTokenID },
	)
	for _, r := range got {
		assert.NotEqual(t, types.AddressBytea(poolC), r.PoolContractID)
	}

	t.Run("empty poolIDs returns an empty slice without querying", func(t *testing.T) {
		m := &blend.ReserveEmissionModel{}
		got, err := m.GetByPools(context.Background(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestBackstopPoolModel_GetByIDs(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPoolsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsertBalances(ctx, tx, []blend.BackstopPool{
			{PoolContractID: types.AddressBytea(poolA), Shares: "1", Tokens: "1", Q4W: "0", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), Shares: "2", Tokens: "2", Q4W: "0", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolC), Shares: "3", Tokens: "3", Q4W: "0", LastModifiedLedger: 1},
		}))
	})

	got, err := m.GetByIDs(ctx, []string{poolA, poolC, keypair.MustRandom().Address()})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assertOrderedByAddr(t, got, func(bp blend.BackstopPool) types.AddressBytea { return bp.PoolContractID })
	for _, bp := range got {
		assert.NotEqual(t, types.AddressBytea(poolB), bp.PoolContractID)
	}

	t.Run("empty poolIDs returns an empty slice without querying", func(t *testing.T) {
		m := &blend.BackstopPoolModel{}
		got, err := m.GetByIDs(context.Background(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestEmissionModel_GetByAccount(t *testing.T) {
	ctx, pool, m, cleanup := newEmissionsFixture(t)
	defer cleanup()

	sourceA := keypair.MustRandom().Address()
	sourceB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Emission{
			{SourceContractID: types.AddressBytea(sourceA), UserAccountID: types.AddressBytea(userA), TokenID: 1, EmissionIndex: "1", Accrued: "1", LastModifiedLedger: 1},
			{SourceContractID: types.AddressBytea(sourceA), UserAccountID: types.AddressBytea(userA), TokenID: 0, EmissionIndex: "1", Accrued: "1", LastModifiedLedger: 1},
			{SourceContractID: types.AddressBytea(sourceB), UserAccountID: types.AddressBytea(userA), TokenID: blend.BackstopEmissionTokenID, EmissionIndex: "1", Accrued: "1", LastModifiedLedger: 1},
			{SourceContractID: types.AddressBytea(sourceA), UserAccountID: types.AddressBytea(userB), TokenID: 0, EmissionIndex: "1", Accrued: "1", LastModifiedLedger: 1},
		}))
	})

	got, err := m.GetByAccount(ctx, userA)
	require.NoError(t, err)
	require.Len(t, got, 3, "only userA's rows across both sources")
	for _, e := range got {
		assert.Equal(t, types.AddressBytea(userA), e.UserAccountID)
	}
	assertOrderedByAddrThen(t, got,
		func(e blend.Emission) types.AddressBytea { return e.SourceContractID },
		func(e blend.Emission) int32 { return e.TokenID },
	)

	t.Run("unknown account returns an empty, non-nil slice", func(t *testing.T) {
		got, err := m.GetByAccount(ctx, keypair.MustRandom().Address())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}
