package db

import (
	"context"
	"testing"

	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenDBConnectionPool(t *testing.T) {
	dbt := dbtest.Postgres(t)
	defer dbt.Close()

	dbConnectionPool, err := OpenDBConnectionPool(dbt.DSN, nil)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	assert.Equal(t, "postgres", dbConnectionPool.DriverName())

	ctx := context.Background()
	err = dbConnectionPool.Ping(ctx)
	require.NoError(t, err)
}
