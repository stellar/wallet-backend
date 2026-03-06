package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func TestOpenDBConnectionPool(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	assert.NotNil(t, pool)

	err = pool.Ping(ctx)
	require.NoError(t, err)
}
