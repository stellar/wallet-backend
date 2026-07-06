package resolvers

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/google/uuid"
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

func TestBalanceSourcesForAddress(t *testing.T) {
	t.Run("includes SEP-41 for G-addresses alongside native and classic", func(t *testing.T) {
		g := keypair.MustRandom().Address()
		gSources := balanceSourcesForAddress(g)
		assert.Contains(t, gSources, balanceSourceSEP41, "G-addresses must advertise SEP-41 as a balance source")
		assert.Contains(t, gSources, balanceSourceLiquidityPool, "G-addresses must advertise liquidity-pool shares as a balance source")
		assert.Equal(t, []balanceSource{balanceSourceNative, balanceSourceClassic, balanceSourceSEP41, balanceSourceLiquidityPool}, gSources)
	})

	t.Run("includes SEP-41 for C-addresses alongside SAC", func(t *testing.T) {
		// A syntactically valid contract address for the sake of the test.
		c := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		cSources := balanceSourcesForAddress(c)
		assert.Contains(t, cSources, balanceSourceSEP41, "C-addresses must also advertise SEP-41")
		assert.Equal(t, []balanceSource{balanceSourceSAC, balanceSourceSEP41}, cSources)
	})
}

func TestParseBalanceCursor(t *testing.T) {
	t.Run("accepts a SEP-41 cursor with a valid UUID payload", func(t *testing.T) {
		cid := uuid.New()
		// parseBalanceCursor takes the inner payload, not the base64-wrapped cursor.
		inner := fmt.Sprintf("%s:%s:%s", balanceCursorPrefix, balanceSourceSEP41, cid.String())
		got, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceNative, balanceSourceClassic, balanceSourceSEP41})
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, balanceSourceSEP41, got.Source)
		assert.Equal(t, cid.String(), got.ID)

		// And its .uuid() helper should round-trip.
		parsed, uerr := got.uuid()
		require.NoError(t, uerr)
		require.NotNil(t, parsed)
		assert.Equal(t, cid, *parsed)
	})

	t.Run("rejects a SEP-41 cursor whose ID is not a valid UUID", func(t *testing.T) {
		inner := fmt.Sprintf("%s:%s:not-a-uuid", balanceCursorPrefix, balanceSourceSEP41)
		_, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceSEP41})
		require.Error(t, err)
	})

	t.Run("rejects a tampered cursor whose source is disallowed for the address", func(t *testing.T) {
		// A client that tampers with the cursor source should still get rejected; this
		// guards against replaying e.g. a classic cursor against an address that can't hold classic.
		inner := base64.StdEncoding.EncodeToString([]byte("v1:classic:" + uuid.New().String()))
		_, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceSEP41})
		require.Error(t, err)
	})
}

func TestAccountResolver_SEP41BalancesReturnedByBalancesConnection(t *testing.T) {
	// Use an address that is not touched by setupDB so the native/classic sources return
	// nothing and we can see the SEP-41 edges cleanly.
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := data.DeterministicContractID(contractAddr)
	name, symbol := "USDC", "USDC"

	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
	`, cid, contractAddr, "sep41", name, symbol, 7)
	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM sep41_balances WHERE account_id = $1`, types.AddressBytea(acct))
	})

	execTestDB(t, `
		INSERT INTO sep41_balances (account_id, contract_id, balance, last_modified_ledger)
		VALUES ($1, $2, $3, $4)
	`, types.AddressBytea(acct), cid, "123456789", uint32(9999))

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

	ctx := getTestCtx("balances", []string{"balance", "tokenId", "decimals"})
	conn, err := resolver.Balances(ctx, parentAccount, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Len(t, conn.Edges, 1)

	sep41Bal, ok := conn.Edges[0].Node.(*graphql1.SEP41Balance)
	require.True(t, ok, "edge[0] should be SEP41Balance, got %T", conn.Edges[0].Node)
	assert.Equal(t, contractAddr, sep41Bal.TokenID)
	assert.Equal(t, "123456789", sep41Bal.Balance)
	assert.Equal(t, int32(7), sep41Bal.Decimals)
	require.NotNil(t, sep41Bal.Name)
	assert.Equal(t, "USDC", *sep41Bal.Name)
	assert.Equal(t, graphql1.TokenTypeSep41, sep41Bal.TokenType)
}
