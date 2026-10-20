package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunInReadSnapshot pins the property the balances resolver relies on: a
// commit landing between two reads inside fn is invisible to the second.
func TestRunInReadSnapshot(t *testing.T) {
	pool, ctx := openTestPool(t)
	_, err := pool.Exec(ctx, `CREATE TABLE snapshot_test (value int NOT NULL); INSERT INTO snapshot_test VALUES (1)`)
	require.NoError(t, err)

	err = RunInReadSnapshot(ctx, pool, func(ctx context.Context) error {
		q := QuerierFrom(ctx, pool)
		first, err := QueryOne[int](ctx, q, `SELECT value FROM snapshot_test`)
		require.NoError(t, err)

		_, err = pool.Exec(ctx, `UPDATE snapshot_test SET value = 2`)
		require.NoError(t, err)

		second, err := QueryOne[int](ctx, q, `SELECT value FROM snapshot_test`)
		require.NoError(t, err)
		assert.Equal(t, 1, first)
		assert.Equal(t, first, second, "a read in the snapshot must not see a commit made after its first read")
		return nil
	})
	require.NoError(t, err)

	assert.Same(t, pool, QuerierFrom(ctx, pool), "outside RunInReadSnapshot reads use the fallback")
}
