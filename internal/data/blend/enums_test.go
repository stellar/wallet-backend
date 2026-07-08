// Unit tests for the state_changes LENDING category/reason CHECK constraints.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func newStateChangesFixture(t *testing.T) (context.Context, *pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, cleanup
}

func insertStateChange(ctx context.Context, pool *pgxpool.Pool, category, reason string) error {
	acct := keypair.MustRandom().Address()
	_, err := pool.Exec(ctx, `
		INSERT INTO state_changes (
			to_id, operation_id, state_change_id, state_change_category, state_change_reason,
			ledger_number, account_id, ledger_created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, int64(1), int64(1), int64(1), category, reason, int32(1), types.AddressBytea(acct), time.Now())
	return err
}

func TestStateChangeEnums(t *testing.T) {
	ctx, pool, cleanup := newStateChangesFixture(t)
	defer cleanup()

	t.Run("accepts LENDING category with SUPPLY reason", func(t *testing.T) {
		err := insertStateChange(ctx, pool, "LENDING", "SUPPLY")
		require.NoError(t, err)
	})

	t.Run("rejects LENDING category with an unknown reason", func(t *testing.T) {
		err := insertStateChange(ctx, pool, "LENDING", "BOGUS")
		require.Error(t, err)
		require.ErrorContains(t, err, "state_changes_state_change_reason_check")
	})
}

func TestBlendTablesExist(t *testing.T) {
	ctx, pool, cleanup := newStateChangesFixture(t)
	defer cleanup()

	tables := []string{
		"blend_pools",
		"blend_positions",
		"blend_reserves",
		"blend_backstop_positions",
		"blend_backstop_pools",
		"blend_reserve_emissions",
		"blend_emissions",
		"blend_oracle_prices",
	}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			var count int
			err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}
