package resolvers

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

func TestBuildLiquidityPoolBalanceFromDB(t *testing.T) {
	lp := data.LiquidityPoolBalance{
		PoolID:       "abc123",
		Shares:       15000000, // 1.5 shares in stroops
		LedgerNumber: 999,
		AssetA:       "native",
		AmountA:      100000000, // 10.0
		AssetB:       "USDC:GISSUER",
		AmountB:      250000000, // 25.0
	}

	got := buildLiquidityPoolBalanceFromDB(lp)

	assert.Equal(t, "abc123", got.TokenID)
	assert.Equal(t, "abc123", got.LiquidityPoolID)
	assert.Equal(t, graphql1.TokenTypeLiquidityPool, got.TokenType)
	assert.Equal(t, "1.5000000", got.Balance)
	assert.Equal(t, uint32(999), got.LastModifiedLedger)
	require.Len(t, got.Reserves, 2)
	assert.Equal(t, "native", got.Reserves[0].Asset)
	assert.Equal(t, "10.0000000", got.Reserves[0].Amount)
	assert.Equal(t, "USDC:GISSUER", got.Reserves[1].Asset)
	assert.Equal(t, "25.0000000", got.Reserves[1].Amount)
}

// TestAccountResolver_LiquidityPoolBalancesCrossSourcePagination verifies the Account.balances
// connection pages across the SEP-41 → liquidity-pool source boundary and that the opaque cursor
// round-trips both across that boundary and within the LP source's own keyset.
func TestAccountResolver_LiquidityPoolBalancesCrossSourcePagination(t *testing.T) {
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	// Two SEP-41 tokens for the account.
	sep41Contract1 := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	sep41Contract2 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	cid1 := data.DeterministicContractID(sep41Contract1)
	cid2 := data.DeterministicContractID(sep41Contract2)

	// Two liquidity pools the account holds shares in. Hex pool ids order aaaa... < bbbb...
	pool1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pool2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES ($1, $2, 'sep41', 'AAA', 'AAA', 7), ($3, $4, 'sep41', 'BBB', 'BBB', 7)
		ON CONFLICT (id) DO NOTHING
	`, cid1, sep41Contract1, cid2, sep41Contract2)
	execTestDB(t, `
		INSERT INTO sep41_balances (account_id, contract_id, balance, last_modified_ledger) VALUES ($1, $2, '111', 10), ($1, $3, '222', 11)
	`, types.AddressBytea(acct), cid1, cid2)
	execTestDB(t, `
		INSERT INTO liquidity_pools (pool_id, asset_a, amount_a, asset_b, amount_b, last_modified_ledger) VALUES
		($1, 'native', 100, 'USDC:GISSUER', 200, 20),
		($2, 'BTC:GISSUER', 300, 'ETH:GISSUER', 400, 21)
	`, pool1, pool2)
	execTestDB(t, `
		INSERT INTO liquidity_pool_balances (account_id, pool_id, shares, last_modified_ledger) VALUES ($1, $2, 5000, 20), ($1, $3, 9000, 21)
	`, types.AddressBytea(acct), pool1, pool2)

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM liquidity_pool_balances WHERE account_id = $1`, types.AddressBytea(acct))
		execTestDB(t, `DELETE FROM sep41_balances WHERE account_id = $1`, types.AddressBytea(acct))
		execTestDB(t, `DELETE FROM liquidity_pools WHERE pool_id IN ($1, $2)`, pool1, pool2)
	})

	m := metrics.NewMetrics(prometheus.NewRegistry())
	rpcMock := services.NewRPCServiceMock(t)
	rpcMock.On("NetworkPassphrase").Return("Test SDF Network ; September 2015").Maybe()

	reader := NewBalanceReader(
		&data.TrustlineBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.NativeBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.SACBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.LiquidityPoolBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.BalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.AllowanceModel{DB: testDBConnectionPool, Metrics: m.DB},
	)

	resolver := &accountResolver{&Resolver{
		rpcService:    rpcMock,
		balanceReader: reader,
		metrics:       m,
	}}

	ctx := getTestCtx("balances", []string{"balance", "tokenId", "liquidityPoolId"})

	// Full connection: canonical order is SEP-41 first, then liquidity pools.
	first := int32(10)
	conn, err := resolver.Balances(ctx, parentAccount, &first, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 4)

	_, ok0 := conn.Edges[0].Node.(*graphql1.SEP41Balance)
	require.True(t, ok0, "edge[0] should be SEP41Balance, got %T", conn.Edges[0].Node)
	_, ok1 := conn.Edges[1].Node.(*graphql1.SEP41Balance)
	require.True(t, ok1, "edge[1] should be SEP41Balance, got %T", conn.Edges[1].Node)

	lp1, ok2 := conn.Edges[2].Node.(*graphql1.LiquidityPoolBalance)
	require.True(t, ok2, "edge[2] should be LiquidityPoolBalance, got %T", conn.Edges[2].Node)
	lp2, ok3 := conn.Edges[3].Node.(*graphql1.LiquidityPoolBalance)
	require.True(t, ok3, "edge[3] should be LiquidityPoolBalance, got %T", conn.Edges[3].Node)

	assert.Equal(t, pool1, lp1.LiquidityPoolID)
	assert.Equal(t, "0.0005000", lp1.Balance)
	require.Len(t, lp1.Reserves, 2)
	assert.Equal(t, "native", lp1.Reserves[0].Asset)
	assert.Equal(t, pool2, lp2.LiquidityPoolID)

	// Cursor round-trip ACROSS the SEP-41 → LP boundary: resume after the last SEP-41 edge and
	// expect exactly the two LP balances (SEP-41 fully consumed, LP scanned from its start).
	afterLastSEP41 := conn.Edges[1].Cursor
	lpPage, err := resolver.Balances(ctx, parentAccount, &first, &afterLastSEP41, nil, nil)
	require.NoError(t, err)
	require.Len(t, lpPage.Edges, 2)
	got1, okA := lpPage.Edges[0].Node.(*graphql1.LiquidityPoolBalance)
	require.True(t, okA, "expected LiquidityPoolBalance after SEP-41 boundary, got %T", lpPage.Edges[0].Node)
	assert.Equal(t, pool1, got1.LiquidityPoolID)
	got2, okB := lpPage.Edges[1].Node.(*graphql1.LiquidityPoolBalance)
	require.True(t, okB)
	assert.Equal(t, pool2, got2.LiquidityPoolID)

	// Cursor round-trip WITHIN the LP source's own keyset: resume after the first LP edge.
	afterFirstLP := lpPage.Edges[0].Cursor
	tailPage, err := resolver.Balances(ctx, parentAccount, &first, &afterFirstLP, nil, nil)
	require.NoError(t, err)
	require.Len(t, tailPage.Edges, 1)
	tail, okC := tailPage.Edges[0].Node.(*graphql1.LiquidityPoolBalance)
	require.True(t, okC)
	assert.Equal(t, pool2, tail.LiquidityPoolID)
}
