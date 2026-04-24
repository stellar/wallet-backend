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

// execTestDB runs a raw SQL exec against the shared test pool. Used by SEP-41
// resolver tests to seed fixtures without involving the data-layer interfaces.
func execTestDB(t *testing.T, sql string, args ...any) {
	t.Helper()
	_, err := testDBConnectionPool.Exec(testCtx, sql, args...)
	require.NoError(t, err)
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
		execTestDB(t, `DELETE FROM sep41_balances WHERE account_address = $1`, acct)
	})

	execTestDB(t, `
		INSERT INTO sep41_balances (account_address, contract_id, balance, last_modified_ledger)
		VALUES ($1, $2, $3, $4)
	`, acct, cid, "123456789", uint32(9999))

	m := metrics.NewMetrics(prometheus.NewRegistry())
	rpcMock := services.NewRPCServiceMock(t)
	rpcMock.On("NetworkPassphrase").Return("Test SDF Network ; September 2015").Maybe()

	reader := NewBalanceReader(
		&data.TrustlineBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.NativeBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
		&data.SACBalanceModel{DB: testDBConnectionPool, Metrics: m.DB},
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

func TestAccountResolver_SEP41TransferSurfacesAsStandardBalanceChange(t *testing.T) {
	// Sep-41 transfer → state_changes row with category=BALANCE, reason=CREDIT, tokenId set,
	// amount set, and (new) to_muxed_id set. Through Account.stateChanges this should come
	// back as a StandardBalanceChange with the expected fields.
	acct := keypair.MustRandom().Address()
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(acct)}

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	// Minimal state_changes row. No FK to operations/transactions because the hypertable
	// already removed those FKs (see migrations).
	// Token column is BYTEA-encoded strkey via NullAddressBytea. Use pgx binding via driver.Valuer
	// by going through the raw SQL: `encode(decode(...), 'hex')` is messy; just encode the 33-byte
	// BYTEA directly here.
	execTestDB(t, `DELETE FROM state_changes WHERE account_id = $1::bytea`, mustAddressBytes(t, acct))
	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM state_changes WHERE account_id = $1::bytea`, mustAddressBytes(t, acct))
	})

	execTestDB(t, `
		INSERT INTO state_changes (
			to_id, state_change_id, state_change_category, state_change_reason,
			ledger_created_at, ledger_number, account_id, operation_id,
			token_id, amount, to_muxed_id
		) VALUES ($1, $2, $3, $4, NOW(), $5, $6::bytea, $7, $8::bytea, $9, $10)
	`,
		int64(42<<32), int64(1),
		string(types.StateChangeCategoryBalance), string(types.StateChangeReasonCredit),
		uint32(100), mustAddressBytes(t, acct), int64((42<<32)|1),
		mustAddressBytes(t, contractAddr), "500",
		"18446744073709551615", // u64 max, proves TEXT column handles >2^63
	)

	m := metrics.NewMetrics(prometheus.NewRegistry())

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
		},
		metrics: m,
	}}

	ctx := getTestCtx("stateChanges", []string{
		"type", "reason", "tokenId", "amount", "toMuxedId", "ledgerNumber",
	})

	first := int32(10)
	conn, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &first, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Len(t, conn.Edges, 1)

	bc, ok := conn.Edges[0].Node.(*types.StandardBalanceStateChangeModel)
	require.True(t, ok, "edge[0] should be StandardBalanceStateChangeModel, got %T", conn.Edges[0].Node)
	assert.Equal(t, types.StateChangeCategoryBalance, bc.StateChangeCategory)
	assert.Equal(t, types.StateChangeReasonCredit, bc.StateChangeReason)
	assert.Equal(t, contractAddr, bc.TokenID.String())
	assert.True(t, bc.Amount.Valid)
	assert.Equal(t, "500", bc.Amount.String)
	assert.True(t, bc.ToMuxedID.Valid)
	assert.Equal(t, "18446744073709551615", bc.ToMuxedID.String)
}

// mustAddressBytes returns the 33-byte BYTEA encoding of a Stellar strkey address,
// matching the types.AddressBytea.Value serialization. Cheap way to build the bytes
// for ad-hoc SQL inserts in tests.
func mustAddressBytes(t *testing.T, addr string) []byte {
	t.Helper()
	a := types.AddressBytea(addr)
	v, err := a.Value()
	require.NoError(t, err)
	b, ok := v.([]byte)
	require.True(t, ok)
	return b
}

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
		execTestDB(t, `DELETE FROM sep41_allowances WHERE owner_address = $1`, acct)
	})

	const currentLedger uint32 = 5000
	execTestDB(t, `
		INSERT INTO sep41_allowances (owner_address, spender_address, contract_id, amount, expiration_ledger, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, acct, spender, activeCID, "100", currentLedger+100, currentLedger-10)
	execTestDB(t, `
		INSERT INTO sep41_allowances (owner_address, spender_address, contract_id, amount, expiration_ledger, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, acct, spender, expiredCID, "50", currentLedger-5, currentLedger-50)

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
		execTestDB(t, `DELETE FROM sep41_allowances WHERE owner_address = $1`, acct)
	})

	const currentLedger uint32 = 5000
	spenders := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		spender := keypair.MustRandom().Address()
		spenders = append(spenders, spender)
		execTestDB(t, `
			INSERT INTO sep41_allowances (owner_address, spender_address, contract_id, amount, expiration_ledger, last_modified_ledger)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, acct, spender, cid, "100", currentLedger+100, currentLedger-10)
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
	sortStrings(sortedSpenders)
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
	huge := int32(maxAllowancePageLimit + 1)
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

// sortStrings is a tiny in-package helper so the test doesn't need to import sort.
func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j-1] > ss[j]; j-- {
			ss[j-1], ss[j] = ss[j], ss[j-1]
		}
	}
}
