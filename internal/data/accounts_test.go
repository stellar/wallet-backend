package data

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountModelInsert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &AccountModel{
		DB: dbConnectionPool,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = m.Insert(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}

func TestAccountModelDelete(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &AccountModel{
		DB: dbConnectionPool,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	err = m.Delete(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestAccountModelExists(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &AccountModel{
		DB: dbConnectionPool,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()

	exists, err := m.Exists(ctx, address)
	require.NoError(t, err)
	assert.False(t, exists)

	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	exists, err = m.Exists(ctx, address)
	require.NoError(t, err)
	assert.True(t, exists)
}
