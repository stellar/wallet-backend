package db

import (
	"testing"

	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenDBConnectionPool(t *testing.T) {
	dbt := dbtest.Postgres(t)
	defer dbt.Close()

	dbConnectionPool, err := OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	assert.Equal(t, "postgres", dbConnectionPool.DriverName())

	err = dbConnectionPool.Ping()
	require.NoError(t, err)
}
