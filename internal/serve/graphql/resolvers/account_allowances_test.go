package resolvers

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountResolver_SEP41AllowancesFiltersExpired(t *testing.T) {
	acct := keypair.MustRandom().Address()
	spender := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	activeContract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	expiredContract := "CBN5OPS5WUNUCBI4GO7AZG5KV4JUKIX5RXZ2HKFLPDOLC5W3L3HKL34Z"
	activeCID := data.DeterministicContractID(activeContract)
	expiredCID := data.DeterministicContractID(expiredContract)

	execTestDB(t,
		`INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
		activeCID, activeContract, "sep41", 7)
	execTestDB(t,
		`INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
		expiredCID, expiredContract, "sep41", 7)

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM sep41_allowances WHERE owner_id = $1`, types.AddressBytea(acct))
	})

	const currentLedger uint32 = 5000
	execTestDB(t, `
		INSERT INTO sep41_allowances (owner_id, spender_id, contract_id, amount, expiration_ledger, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, types.AddressBytea(acct), types.AddressBytea(spender), activeCID, "100", currentLedger+100, currentLedger-10)
	execTestDB(t, `
		INSERT INTO sep41_allowances (owner_id, spender_id, contract_id, amount, expiration_ledger, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, types.AddressBytea(acct), types.AddressBytea(spender), expiredCID, "50", currentLedger-5, currentLedger-50)

	// Seed the ingest_store latest_ingest_ledger value the resolver reads.
	execTestDB(t,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		data.LatestLedgerCursorName, "5000")

	m := metrics.NewMetrics(prometheus.NewRegistry())
	reader := NewBalanceReader(
		&data.TrustlineBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.NativeBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.SACBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.LiquidityPoolBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.BalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.AllowanceModel{DB: testDBConnectionPool, Metrics: m.DB},
	)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			IngestStore: &data.IngestStoreModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
		balanceReader: reader,
		metrics:       m,
	}}

	ctx := getTestCtx("sep41Allowances", []string{"owner", "spender", "tokenId", "amount", "expirationLedger"})
	conn, err := resolver.Sep41Allowances(ctx, parentAccount, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Only the active allowance should come back. Expired rows are filtered by GetByOwner.
	require.Len(t, conn.Edges, 1)
	a := conn.Edges[0].Node
	assert.Equal(t, acct, a.Owner)
	assert.Equal(t, spender, a.Spender)
	assert.Equal(t, activeContract, a.TokenID)
	assert.Equal(t, "100", a.Amount)
	assert.Equal(t, currentLedger+100, a.ExpirationLedger)
	assert.False(t, conn.PageInfo.HasNextPage)
	assert.False(t, conn.PageInfo.HasPreviousPage)
}

func TestAccountResolver_SEP41AllowancesPaginates(t *testing.T) {
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	// 4 spenders, each with one allowance against a shared contract. Using distinct
	// contract IDs isn't required because (spender, contract_id) is the page key.
	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := data.DeterministicContractID(contractAddr)
	execTestDB(t,
		`INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
		cid, contractAddr, "sep41", 7)
	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM sep41_allowances WHERE owner_id = $1`, types.AddressBytea(acct))
	})

	const currentLedger uint32 = 5000
	spenders := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		spender := keypair.MustRandom().Address()
		spenders = append(spenders, spender)
		execTestDB(t, `
			INSERT INTO sep41_allowances (owner_id, spender_id, contract_id, amount, expiration_ledger, last_modified_ledger)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, types.AddressBytea(acct), types.AddressBytea(spender), cid, "100", currentLedger+100, currentLedger-10)
	}

	execTestDB(t,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		data.LatestLedgerCursorName, "5000")

	m := metrics.NewMetrics(prometheus.NewRegistry())
	reader := NewBalanceReader(
		&data.TrustlineBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.NativeBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.SACBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.LiquidityPoolBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.BalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&sep41data.AllowanceModel{DB: testDBConnectionPool, Metrics: m.DB},
	)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			IngestStore: &data.IngestStoreModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
		balanceReader: reader,
		metrics:       m,
	}}

	ctx := getTestCtx("sep41Allowances", []string{"spender", "tokenId", "amount"})

	// Forward walk: first page of 2, then after(endCursor).
	first := int32(2)
	page1, err := resolver.Sep41Allowances(ctx, parentAccount, &first, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, page1)
	require.Len(t, page1.Edges, 2)
	require.True(t, page1.PageInfo.HasNextPage, "expected HasNextPage on first forward page")
	require.False(t, page1.PageInfo.HasPreviousPage)
	require.NotNil(t, page1.PageInfo.EndCursor)

	page2, err := resolver.Sep41Allowances(ctx, parentAccount, &first, page1.PageInfo.EndCursor, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, page2)
	require.Len(t, page2.Edges, 2)
	require.False(t, page2.PageInfo.HasNextPage, "expected no next page after all 4 consumed")
	require.True(t, page2.PageInfo.HasPreviousPage)

	seen := []string{
		page1.Edges[0].Node.Spender, page1.Edges[1].Node.Spender,
		page2.Edges[0].Node.Spender, page2.Edges[1].Node.Spender,
	}
	sortedSpenders := append([]string(nil), spenders...)
	sortStrkeysByBytea(t, sortedSpenders)
	assert.Equal(t, sortedSpenders, seen, "forward walk should return spenders in ASC order")

	// Backward walk from the end: last:2 with no cursor gives the final two,
	// then last:2 before the startCursor gives the first two.
	last := int32(2)
	rpage1, err := resolver.Sep41Allowances(ctx, parentAccount, nil, nil, &last, nil)
	require.NoError(t, err)
	require.NotNil(t, rpage1)
	require.Len(t, rpage1.Edges, 2)

	rpage2, err := resolver.Sep41Allowances(ctx, parentAccount, nil, nil, &last, rpage1.PageInfo.StartCursor)
	require.NoError(t, err)
	require.NotNil(t, rpage2)
	require.Len(t, rpage2.Edges, 2)

	// Oversize page is rejected with BAD_USER_INPUT.
	huge := maxAllowancePageLimit + 1
	_, err = resolver.Sep41Allowances(ctx, parentAccount, &huge, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "less than or equal to 100")
}

func TestParseAllowanceCursor(t *testing.T) {
	t.Run("nil cursor returns nil", func(t *testing.T) {
		got, err := parseAllowanceCursor(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("valid cursor round trips", func(t *testing.T) {
		id := data.DeterministicContractID("CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA")
		spender := keypair.MustRandom().Address()
		raw := "v1:" + spender + ":" + id.String()
		got, err := parseAllowanceCursor(&raw)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, spender, got.SpenderAddress)
		assert.Equal(t, id, got.ContractID)
	})

	t.Run("wrong version rejected", func(t *testing.T) {
		raw := "v2:G123:11111111-1111-1111-1111-111111111111"
		_, err := parseAllowanceCursor(&raw)
		assert.Error(t, err)
	})

	t.Run("non-uuid contract id rejected", func(t *testing.T) {
		raw := "v1:G123:not-a-uuid"
		_, err := parseAllowanceCursor(&raw)
		assert.Error(t, err)
	})

	t.Run("wrong segment count rejected", func(t *testing.T) {
		raw := "v1:only-two"
		_, err := parseAllowanceCursor(&raw)
		assert.Error(t, err)
	})
}

// sortStrkeysByBytea sorts strkey addresses by their 33-byte BYTEA encoding,
// matching the ordering Postgres uses for the bytea columns. Used to compute
// the expected page order for sep41_allowances tests where the DB orders by
// (spender_id::bytea, contract_id).
func sortStrkeysByBytea(t *testing.T, addrs []string) {
	t.Helper()
	encoded := make(map[string][]byte, len(addrs))
	for _, a := range addrs {
		raw, err := types.AddressBytea(a).Value()
		require.NoError(t, err)
		encoded[a] = raw.([]byte)
	}
	for i := 1; i < len(addrs); i++ {
		for j := i; j > 0 && bytes.Compare(encoded[addrs[j-1]], encoded[addrs[j]]) > 0; j-- {
			addrs[j-1], addrs[j] = addrs[j], addrs[j-1]
		}
	}
}
