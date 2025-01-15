package services

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountRegister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN, nil)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)
	accountService, err := NewAccountService(models)
	require.NoError(t, err)

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = accountService.RegisterAccount(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}

func TestAccountDeregister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN, nil)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)
	accountService, err := NewAccountService(models)
	require.NoError(t, err)

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	result, err := dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	err = accountService.DeregisterAccount(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	assert.ErrorIs(t, err, sql.ErrNoRows)
}
