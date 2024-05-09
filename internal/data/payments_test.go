package data

import (
	"context"
	"database/sql"
	"testing"
	"wallet-backend/internal/db"
	"wallet-backend/internal/db/dbtest"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &PaymentModel{
		db: dbConnectionPool,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = m.SubscribeAddress(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.db.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}
