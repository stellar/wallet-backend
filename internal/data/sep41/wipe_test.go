// Tests for the SEP-41 current-state wipe.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package sep41_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n))
	return n
}

// TestWipeCurrentState pins what the SEP-41 rebuild's wipe clears: both
// current-state tables, and not contract_tokens, which classification owns and
// nothing rebuilds.
func TestWipeCurrentState(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.Open(t)
	defer dbt.Close()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	contractAddr := keypair.MustRandom().Address()
	tokenID := insertContractToken(t, ctx, pool, contractAddr)

	account := []byte("account-bytes-32-padded---------")
	spender := []byte("spender-bytes-32-padded---------")
	_, err = pool.Exec(ctx, `INSERT INTO sep41_balances (account_id, contract_id) VALUES ($1, $2)`, account, tokenID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO sep41_allowances (owner_id, spender_id, contract_id) VALUES ($1, $2, $3)`, account, spender, tokenID)
	require.NoError(t, err)

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, sep41.WipeCurrentState(ctx, tx))
	})

	assert.Zero(t, countRows(t, ctx, pool, "sep41_balances"))
	assert.Zero(t, countRows(t, ctx, pool, "sep41_allowances"))
	assert.Equal(t, 1, countRows(t, ctx, pool, "contract_tokens"), "classification owns contract_tokens; the wipe must leave it intact")
}
