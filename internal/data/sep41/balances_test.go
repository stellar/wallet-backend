// Unit tests for the SEP-41 BalanceModel.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package sep41_test

import (
	"context"
	"testing"
	"time"

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
	return insertContractTokenOfType(t, ctx, pool, contractAddr, "sep41")
}

func insertContractTokenOfType(t *testing.T, ctx context.Context, pool *pgxpool.Pool, contractAddr, typ string) uuid.UUID {
	t.Helper()
	id := data.DeterministicContractID(contractAddr)
	_, err := pool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, id, contractAddr, typ, 7)
	require.NoError(t, err)
	return id
}

// insertBalanceStateChange writes one state_changes row with the BYTEA encoding
// the production code emits, so GetAllSEP41Pairs sees real data shapes. We
// don't go through StateChangeModel.BatchCopy here to keep the helper light;
// the column subset we set is the minimum needed by the bootstrap query.
func insertBalanceStateChange(t *testing.T, ctx context.Context, pool *pgxpool.Pool, account, contract string, toID, opID, scID int64, ledger uint32, ts time.Time) {
	t.Helper()
	accountBytes, err := types.AddressBytea(account).Value()
	require.NoError(t, err)
	tokenBytes, err := types.AddressBytea(contract).Value()
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO state_changes (
			to_id, operation_id, state_change_id,
			state_change_category, state_change_reason,
			ledger_number, account_id, token_id,
			ledger_created_at
		) VALUES ($1, $2, $3, 'BALANCE', 'CREDIT', $4, $5, $6, $7)
	`, toID, opID, scID, ledger, accountBytes, tokenBytes, ts)
	require.NoError(t, err)
}

func runInTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	require.NoError(t, tx.Commit(ctx))
}

func TestBalanceModel_BatchUpsertAbsolute_InsertsFreshRow(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		err := m.BatchUpsertAbsolute(ctx, tx, []sep41.Balance{{
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

func TestBalanceModel_BatchUpsertAbsolute_OverwritesExistingRow(t *testing.T) {
	// Authoritative absolute writes must replace any pre-existing value — the old
	// delta-based code accumulated on conflict; the new code must not.
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsertAbsolute(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "1000", LedgerNumber: 42,
		}}))
	})
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsertAbsolute(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "750", LedgerNumber: 43,
		}}))
	})

	balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	require.Len(t, balances, 1)
	assert.Equal(t, "750", balances[0].Balance)
	assert.Equal(t, uint32(43), balances[0].LedgerNumber)
}

func TestBalanceModel_BatchUpsertAbsolute_DeletesZeroBalances(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	acct := keypair.MustRandom().Address()
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsertAbsolute(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "500", LedgerNumber: 10,
		}}))
	})
	// Subsequent RPC fetch returns 0 (e.g., account fully transferred away) — sweep.
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsertAbsolute(ctx, tx, []sep41.Balance{{
			AccountAddress: acct, ContractID: cid, Balance: "0", LedgerNumber: 11,
		}}))
	})

	balances, err := m.GetByAccount(ctx, acct, nil, nil, sep41.SortASC)
	require.NoError(t, err)
	assert.Empty(t, balances, "zero-balance row should be swept")
}

func TestBalanceModel_BatchUpsertAbsolute_EmptyInputNoOp(t *testing.T) {
	ctx, _, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	require.NoError(t, m.BatchUpsertAbsolute(ctx, nil, nil))
}

func TestBalanceModel_GetAllSEP41Pairs_FromStateChanges(t *testing.T) {
	// The bootstrap source is `state_changes`, not `sep41_balances`. After a
	// fresh `protocol-migrate history` run, sep41_balances is empty but
	// state_changes contains every event for every account that ever held a
	// SEP-41 token — that's the set we have to refresh.
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cid := insertContractToken(t, ctx, pool, contract)

	acctA := keypair.MustRandom().Address()
	acctB := keypair.MustRandom().Address()
	now := time.Now()
	insertBalanceStateChange(t, ctx, pool, acctA, contract, 1, 11, 1, 1, now)
	insertBalanceStateChange(t, ctx, pool, acctB, contract, 2, 22, 1, 1, now)
	// A second event for acctA on the same token must not produce a duplicate pair.
	insertBalanceStateChange(t, ctx, pool, acctA, contract, 3, 33, 1, 2, now.Add(time.Second))

	var pairs []sep41.BalancePair
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		pairs, err = m.GetAllSEP41Pairs(ctx, tx)
		require.NoError(t, err)
	})

	require.Len(t, pairs, 2)
	got := map[string]sep41.BalancePair{}
	for _, p := range pairs {
		got[p.AccountAddress] = p
	}
	require.Contains(t, got, acctA)
	require.Contains(t, got, acctB)
	for _, p := range pairs {
		assert.Equal(t, contract, p.ContractAddress)
		assert.Equal(t, cid, p.ContractID)
	}
}

func TestBalanceModel_GetAllSEP41Pairs_ExcludesNonSEP41Tokens(t *testing.T) {
	// state_changes carries balance events for every token type, including
	// SAC. The bootstrap must filter to contract_tokens.type='sep41' so SAC
	// (or unknown) tokens don't get refreshed via SEP-41's RPC path.
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()

	sep41Contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	sacContract := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	insertContractTokenOfType(t, ctx, pool, sep41Contract, "sep41")
	insertContractTokenOfType(t, ctx, pool, sacContract, "sac")

	holder := keypair.MustRandom().Address()
	now := time.Now()
	insertBalanceStateChange(t, ctx, pool, holder, sep41Contract, 1, 11, 1, 1, now)
	insertBalanceStateChange(t, ctx, pool, holder, sacContract, 2, 22, 1, 1, now)

	var pairs []sep41.BalancePair
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		pairs, err = m.GetAllSEP41Pairs(ctx, tx)
		require.NoError(t, err)
	})

	require.Len(t, pairs, 1, "SAC token must be excluded from SEP-41 bootstrap")
	assert.Equal(t, sep41Contract, pairs[0].ContractAddress)
	assert.Equal(t, holder, pairs[0].AccountAddress)
}

func TestBalanceModel_GetAllSEP41Pairs_EmptyStateChanges(t *testing.T) {
	ctx, pool, m, cleanup := newBalancesFixture(t)
	defer cleanup()
	_ = pool

	var pairs []sep41.BalancePair
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		pairs, err = m.GetAllSEP41Pairs(ctx, tx)
		require.NoError(t, err)
	})

	assert.Empty(t, pairs)
}
